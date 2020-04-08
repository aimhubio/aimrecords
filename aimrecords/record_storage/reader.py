import io
import gzip
from typing import Union, Tuple, List
from collections import namedtuple, Iterator

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
)

from aimrecords.record_storage.utils import (
    read_metadata,
    get_data_fname,
    get_bucket_offsets_fname,
    get_record_offsets_fname,
)


RecordLocation = namedtuple('RecordLocation', ['bucket_offset',
                                               'bucket_size',
                                               'record_local_offset',
                                               ])


class Reader(object):
    def __init__(self, path: str):
        self.path = path
        self.metadata = read_metadata(path)
        # TODO: check data version compatibility
        self._validate()

        self.compression = self.metadata[COMPRESSION]
        assert self.compression in COMPRESSION_ALGORITHMS
        self._decompressed_bucket_cache = None

        self.buckets_num = self.metadata[BUCKETS_NUM]
        self.records_num = self.metadata[RECORDS_NUM]
        self.data_file_bsize = self.metadata['data_file_bsize']
        self.data_file = open(get_data_fname(self.path), 'rb')
        self.record_index_to_offset_list = self._get_record_index_to_offset_list()

        self._iter = None

    def get(self, index: int) -> bytes:
        if index >= self.records_num:
            raise IndexError('index out of range')

        record_loc = self.record_index_to_offset_list[index]

        if self.compression is None:
            record_offset = record_loc.record_local_offset
            self.data_file.seek(record_loc.bucket_offset + record_offset)
            record_source_fobj = self.data_file
        else:
            bucket = self._decompressed_bucket(record_loc.bucket_offset,
                                               record_loc.bucket_size)
            bucket.seek(record_loc.record_local_offset)
            record_source_fobj = bucket

        assert record_source_fobj is not None
        size = int.from_bytes(record_source_fobj.read(RECORD_LEN_SIZE), ENDIANNESS)
        record = record_source_fobj.read(size)
        return record

    def close(self):
        self.data_file.close()

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
                                          'implemented'.format(self.compression))

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
                bucket_offset = int.from_bytes(f_in.read(BUCKET_OFFSET_SIZE), ENDIANNESS)
                records_num = int.from_bytes(f_in.read(RECORDS_NUM_SIZE), ENDIANNESS)

                if len(bucket_offset_records_num_list):
                    prev_bucket_offset = bucket_offset_records_num_list[-1]
                    prev_bucket_offset[1] = bucket_offset - prev_bucket_offset[0]
                bucket_offset_records_num_list.append([bucket_offset, 0, records_num])

            last_bucket_offset = bucket_offset_records_num_list[-1]
            last_bucket_offset[1] = self.data_file_bsize[0] - last_bucket_offset[0]

        record_index_to_offset_list = []
        with open(get_record_offsets_fname(self.path), 'rb') as f_in:
            for record_index in range(self.records_num):
                if record_index >= bucket_offset_records_num_list[0][2]:
                    bucket_offset_records_num_list.pop(0)

                bucket_offset = bucket_offset_records_num_list[0][0]
                bucket_size = bucket_offset_records_num_list[0][1]
                record_local_offset = int.from_bytes(f_in.read(RECORD_OFFSET_SIZE), ENDIANNESS)

                record_loc = RecordLocation(bucket_offset, bucket_size, record_local_offset)
                record_index_to_offset_list.append(record_loc)

        return record_index_to_offset_list

    def _validate(self):
        # TODO: implement
        pass


class ReaderIterator(Reader, Iterator):
    def __getitem__(self, item: Union[None, int, Tuple[int, ...], slice]
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
            indices_range = range(self.records_num)
            self._iter = iter(indices_range[start:stop:step])
        else:
            raise TypeError('expected slice or tuple of indices')

        return iter(self)

    def __next__(self) -> bytes:
        try:
            idx = next(self._iter)
            return self.get(idx)
        except (IndexError, StopIteration):
            self._iter = None
            raise StopIteration

    def __len__(self) -> int:
        return self.records_num
