import os
import io
import gzip
from shutil import rmtree
from typing import Optional, Dict

from aimrecords.record_storage.consts import (
    RECORD_OFFSET_SIZE,
    RECORD_LEN_SIZE,
    BUCKET_OFFSET_SIZE,
    RECORDS_NUM_SIZE,
    BUCKET_SIZE_KB,
    ENDIANNESS,
    RECORDS_NUM,
    BUCKETS_NUM,
    COMPRESSION_GZIP,
    COMPRESSION_ALGORITHMS,
    DATA_VERSION,
)
from aimrecords.record_storage.utils import (
    write_metadata,
    read_metadata,
    metadata_exists,
    get_bucket_offsets_fname,
    get_data_fname,
    get_record_offsets_fname,
    current_bucket_fname,
    data_version_compatibility,
)
from aimrecords.indexing.index_key import IndexKey
from aimrecords.indexing.writer import IndexWriter


class Writer(object):
    @staticmethod
    def validate_artifact_path(path):
        # TODO
        return True

    def __init__(self, path: str,
                 compression: Optional[str] = COMPRESSION_GZIP,
                 rewrite: bool = False):
        assert self.validate_artifact_path(path)
        assert compression in COMPRESSION_ALGORITHMS

        self.path = path
        self.rewrite = rewrite

        if self.rewrite or not metadata_exists(self.path):
            self.data_version = DATA_VERSION
            self.compression = compression
            self.data_chunks_num = 0
            self.buckets_num = 0
            self.records_num = 0
            self.indices_meta = {}
        else:
            meta = read_metadata(self.path)
            self.data_version = meta.get('data_version')
            data_version_compatibility(self.data_version, DATA_VERSION)

            self.data_chunks_num = meta.get('data_chunks_num')
            self.buckets_num = meta.get(BUCKETS_NUM)
            self.records_num = meta.get(RECORDS_NUM)
            self.compression = meta.get('compression')
            self.indices_meta = meta.get('indices') or {}
            if self.compression != compression:
                raise ValueError('already applied {} compression for ' +
                                 '{} artifact'.format(self.compression,
                                                      self.path))

        if self.rewrite and self.exists():
            rmtree(self.path)
            os.makedirs(self.path)
        elif not self.exists():
            os.makedirs(self.path)

        file_open_mode = 'wb' if self.rewrite else 'ab'

        (
            self.record_offsets_file,
            self.bucket_offsets_file,
            self.current_data_file,
            self.current_bucket_file,
        ) = (
            open(get_record_offsets_fname(self.path), file_open_mode),
            open(get_bucket_offsets_fname(self.path), file_open_mode),
            open(get_data_fname(self.path), file_open_mode),
            open(current_bucket_fname(self.path), file_open_mode),
        )

        self.file_open_mode = file_open_mode

        self.indexes: Dict[IndexKey, IndexWriter] = {}

    def append_record(self, data: bytes,
                      index: Optional[dict] = None):
        current_record_offset = self.current_bucket_file.tell()
        offset_b = current_record_offset.to_bytes(RECORD_OFFSET_SIZE,
                                                  ENDIANNESS)
        data_len_b = len(data).to_bytes(RECORD_LEN_SIZE, ENDIANNESS)

        self.record_offsets_file.write(offset_b)
        self.current_bucket_file.write(data_len_b)
        self.current_bucket_file.write(data)

        if index is not None:
            self.register_index(index)

        self.records_num += 1

        self.current_bucket_file.flush()
        if self._current_bucket_overflow():
            self._finalize_current_bucket()

    def register_index(self, index: dict):
        index_key = IndexKey(index)
        if index_key not in self.indexes:
            self.indexes[index_key] = IndexWriter(self.path, index_key,
                                                  self.file_open_mode)

        index_inst = self.indexes[index_key]
        index_inst.register_record(self.records_num)

    def flush(self):
        self.current_bucket_file.flush()
        self.record_offsets_file.flush()

        for index in self.indexes.values():
            index.flush()

    def save_metadata(self):
        metadata = {
            'data_version': self.data_version,
            'compression': self.compression,
            'data_chunks_num': self.data_chunks_num,
            BUCKETS_NUM: self.buckets_num,
            RECORDS_NUM: self.records_num,
            'record_offsets_bsize': self.record_offsets_file.tell(),
            'bucket_offsets_bsize': self.bucket_offsets_file.tell(),
            'data_file_bsize': [self.current_data_file.tell()],
            'indices': self.indices_meta,
        }

        for index_key, index in self.indexes.items():
            metadata['indices'].setdefault(index.name, {})
            idx_meta = metadata['indices'][index.name]
            idx_meta.setdefault('indexed_records_num', 0)
            idx_meta.setdefault('keys', index_key.index)
            if index.num_appended:
                idx_meta['indexed_records_num'] = index.indexed_records_num()

        write_metadata(self.path, metadata)

    def close(self):
        if self.current_bucket_file.tell() > 0:
            self._finalize_current_bucket()

        self.save_metadata()

        self.record_offsets_file.close()
        self.bucket_offsets_file.close()
        self.current_data_file.close()

        for index in self.indexes.values():
            index.close()

        assert self.current_bucket_file.tell() == 0
        self.current_bucket_file.close()
        os.remove(current_bucket_fname(self.path))

    def exists(self):
        return os.path.isdir(self.path)

    def _current_bucket_overflow(self):
        return self.current_bucket_file.tell() / 1024 > BUCKET_SIZE_KB

    def _finalize_current_bucket(self):
        current_bucket_offset = self.current_data_file.tell()
        offset_b = current_bucket_offset.to_bytes(BUCKET_OFFSET_SIZE,
                                                  ENDIANNESS)
        records_num_b = self.records_num.to_bytes(RECORDS_NUM_SIZE, ENDIANNESS)

        self.bucket_offsets_file.write(offset_b)
        self.bucket_offsets_file.write(records_num_b)

        with open(current_bucket_fname(self.path), 'rb') as f_in:
            # depending on size of current_bucket we may want to read it in
            # chunks depending on compression we need to handle this differently
            bucket_data = f_in.read()

            if self.compression == COMPRESSION_GZIP:
                bucket_comp_obj = io.BytesIO(b'')
                with gzip.GzipFile(fileobj=bucket_comp_obj, mode='wb') \
                        as writer:
                    writer.write(bucket_data)
                bucket_data = bucket_comp_obj.getvalue()

            self.current_data_file.write(bucket_data)

        self.buckets_num += 1
        self.current_data_file.flush()
        self.bucket_offsets_file.flush()
        self.current_bucket_file.truncate(0)
        self.current_bucket_file.seek(0)

        self.save_metadata()
