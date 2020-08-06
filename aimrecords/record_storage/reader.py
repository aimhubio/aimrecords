import os
import io
import gzip
from typing import Union, Tuple, List, Optional, Dict
from collections import namedtuple, Iterator

from aimrecords.indexing.reader import Reader as IndexReader
from aimrecords.record_storage.consts import (
    RECORD_LEN_SIZE,
    RECORD_OFFSET_SIZE,
    BUCKET_OFFSET_SIZE,
    RECORDS_NUM_SIZE,
    ENDIANNESS,
    RECORDS_NUM,
    BUCKETS_NUM,
    COMPRESSION,
    COMPRESSION_GZIP,
    COMPRESSION_ALGORITHMS,
    DATA_VERSION,
)
from aimrecords.record_storage.utils import (
    read_metadata,
    get_data_fname,
    get_bucket_offsets_fname,
    get_record_offsets_fname,
    current_bucket_fname,
    data_version_compatibility,
)
from aimrecords.indexing.index_key import IndexKey


RecordLocation = namedtuple('RecordLocation', ['bucket_offset',
                                               'bucket_size',
                                               'current_bucket',
                                               'record_local_offset',
                                               ])


class Reader(object):
    def __init__(self, path: str, uncommitted_bucket_visible: bool = False):
        self.path = path
        self.uncommitted_bucket_visible = uncommitted_bucket_visible
        self.metadata = read_metadata(path)
        self.data_version = self.metadata.get('data_version') or DATA_VERSION
        self.validate()

        data_version_compatibility(self.data_version, DATA_VERSION)

        self.compression = self.metadata.get(COMPRESSION)
        assert self.compression in COMPRESSION_ALGORITHMS
        self.buckets_num = self.metadata.get(BUCKETS_NUM) or 0
        self.data_file_bsize = self.metadata.get('data_file_bsize') or [0]
        self.data_records_num = self.metadata.get(RECORDS_NUM) or 0

        self.data_file = open(get_data_fname(self.path), 'rb')
        self._decompressed_bucket_cache = None

        current_bucket_name = current_bucket_fname(self.path)
        if uncommitted_bucket_visible and os.path.exists(current_bucket_name):
            self.current_bucket_file = open(current_bucket_name, 'rb')
            self.curr_bucket_size = os.path.getsize(current_bucket_name)
        else:
            self.current_bucket_file = None
            self.curr_bucket_size = 0

        self.all_records_num = self.data_records_num
        self.record_index_to_offset_list = \
            self._get_record_index_to_offset_list()

        self.indexes: Dict[IndexKey, IndexReader] = {}
        self._iter = None

    def get_records_num(self, indexing: Optional[dict] = None) -> int:
        if indexing is None:
            if self.uncommitted_bucket_visible:
                return self.all_records_num
            else:
                return self.data_records_num
        else:
            if self.uncommitted_bucket_visible:
                return self._get_index(indexing).indexed_records_num()
            else:
                indices_meta = self.metadata.get('indices') or {}
                index_key = IndexKey(indexing)
                index_meta = indices_meta.get(str(index_key)) or {}
                return index_meta.get('indexed_records_num') or 0

    def get(self, index: int,
            indexing: Optional[dict] = None) -> bytes:
        if indexing is not None:
            index = self._get_index(indexing).get(index)

        if index >= self.get_records_num():
            raise IndexError('index out of range')

        record_loc = self.record_index_to_offset_list[index]

        if index >= self.data_records_num:
            record_offset = record_loc.record_local_offset
            self.current_bucket_file.seek(record_offset)
            record_source_fobj = self.current_bucket_file
        elif self.compression is None:
            record_offset = record_loc.record_local_offset
            self.data_file.seek(record_loc.bucket_offset + record_offset)
            record_source_fobj = self.data_file
        else:
            bucket = self._decompressed_bucket(record_loc.bucket_offset,
                                               record_loc.bucket_size)
            bucket.seek(record_loc.record_local_offset)
            record_source_fobj = bucket

        assert record_source_fobj is not None
        size = int.from_bytes(record_source_fobj.read(RECORD_LEN_SIZE),
                              ENDIANNESS)
        record = record_source_fobj.read(size)
        return record

    def close(self):
        self.data_file.close()
        if self.current_bucket_file:
            self.current_bucket_file.close()

        for index in self.indexes.values():
            index.close()

    def get_modification_time(self) -> float:
        if self.uncommitted_bucket_visible:
            current_bucket_name = current_bucket_fname(self.path)
            if os.path.isfile(current_bucket_name):
                return os.path.getmtime(current_bucket_name)

        data_fname = get_data_fname(self.path)
        if os.path.exists(data_fname):
            return os.path.getmtime(data_fname)

        return 0

    def _get_index(self, indexing: dict) -> IndexReader:
        index_key = IndexKey(indexing)
        if index_key not in self.indexes:
            self.indexes[index_key] = IndexReader(self.path, index_key, 'rb')
        return self.indexes[index_key]

    def _decompressed_bucket(self, bucket_offset: int, bucket_size: int):
        if (self._decompressed_bucket_cache is not None
                and self._decompressed_bucket_cache['bucket_offset'] == bucket_offset
                and self._decompressed_bucket_cache['bucket_size'] == bucket_size):
            bucket = self._decompressed_bucket_cache['bucket']
        else:
            self.data_file.seek(bucket_offset)
            compressed_bucket = self.data_file.read(bucket_size)

            if self.compression == COMPRESSION_GZIP:
                bucket = gzip.GzipFile(fileobj=io.BytesIO(compressed_bucket),
                                       mode='rb')
            else:
                raise NotImplementedError('{} decompression is not ' +
                                          'implemented'
                                          .format(self.compression))

            self._decompressed_bucket_cache = {
                'bucket_offset': bucket_offset,
                'bucket_size': bucket_size,
                'bucket': bucket,
            }

        return bucket

    def _get_record_index_to_offset_list(self) -> List[RecordLocation]:
        bucket_offset_records_num_list = []
        with open(get_bucket_offsets_fname(self.path), 'rb') as f_in:
            for _ in range(self.buckets_num):
                bucket_offset = int.from_bytes(f_in.read(BUCKET_OFFSET_SIZE),
                                               ENDIANNESS)
                records_num = int.from_bytes(f_in.read(RECORDS_NUM_SIZE),
                                             ENDIANNESS)

                if len(bucket_offset_records_num_list):
                    prev_bucket_offset = bucket_offset_records_num_list[-1]
                    prev_bucket_offset[1] = (bucket_offset
                                             - prev_bucket_offset[0])
                bucket_offset_records_num_list.append([bucket_offset, 0,
                                                       records_num])

            if len(bucket_offset_records_num_list):
                last_bucket_offset = bucket_offset_records_num_list[-1]
                last_bucket_offset[1] = (self.data_file_bsize[0]
                                         - last_bucket_offset[0])

        record_index_to_offset_list = []
        with open(get_record_offsets_fname(self.path), 'rb') as f_in:
            for record_index in range(self.data_records_num):
                if record_index >= bucket_offset_records_num_list[0][2]:
                    bucket_offset_records_num_list.pop(0)

                bucket_offset = bucket_offset_records_num_list[0][0]
                bucket_size = bucket_offset_records_num_list[0][1]
                record_local_offset = int.from_bytes(
                    f_in.read(RECORD_OFFSET_SIZE),
                    ENDIANNESS)

                record_loc = RecordLocation(bucket_offset, bucket_size,
                                            False, record_local_offset)
                record_index_to_offset_list.append(record_loc)

            if self.uncommitted_bucket_visible:
                self.all_records_num = self.data_records_num
                while True:
                    record_offset_bytes = f_in.read(RECORD_OFFSET_SIZE)
                    if not record_offset_bytes:
                        break

                    record_local_offset = int.from_bytes(record_offset_bytes,
                                                         ENDIANNESS)
                    record_loc = RecordLocation(0, self.curr_bucket_size,
                                                True, record_local_offset)
                    record_index_to_offset_list.append(record_loc)
                    self.all_records_num += 1

        return record_index_to_offset_list

    def validate(self):
        # TODO: implement
        pass


class ReaderIterator(Reader, Iterator):
    def __init__(self, *args, **kwargs):
        super(ReaderIterator, self).__init__(*args, **kwargs)
        self.applied_index = None

    def __getitem__(self, item: Optional[Union[int, Tuple[int, ...], slice]]
                    ) -> iter:
        if item is None:
            item = slice(0, None, 1)

        if isinstance(item, int):
            self._iter = iter((item,))
        elif isinstance(item, tuple):
            if any(map(lambda i: not isinstance(i, int), item)):
                raise TypeError('tuple indices must be integers')
            self._iter = iter(item)
        elif isinstance(item, slice):
            start, stop, step = item.start, item.stop, item.step
            if self.applied_index is None:
                indices_range = range(self.get_records_num())
            else:
                indices_range = range(
                    self._get_index(self.applied_index).indexed_records_num())
            self._iter = iter(indices_range[start:stop:step])
        else:
            raise TypeError('expected slice or tuple of indices')

        return iter(self)

    def __next__(self) -> bytes:
        try:
            idx = next(self._iter)
            return self.get(idx, self.applied_index)
        except (IndexError, StopIteration):
            self._iter = None
            raise StopIteration

    def apply_index(self, index: dict):
        self.applied_index = index
