import os

from .consts import (
    ARTIFACT_OFFSET_SIZE,
    ARTIFACT_LEN_SIZE,
    BUCKET_OFFSET_SIZE,
    ARTIFACTS_NUM_SIZE,
    ENDIANNESS,
    ARTIFACTS_NUM,
    BUCKETS_NUM,
)

from .utils import (
    write_metadata,
    get_bucket_offsets_fname,
    get_data_fname,
    get_artifact_offsets_fname,
)


class Writer(object):
    def __init__(self, path: str):
        self.data_version = '0'
        self.compression = None
        self.data_chunks_num = 0

        os.mkdir(path)

        self.path = path
        self.buckets_num = 0
        self.artifacts_num = 0
        self.artifact_offsets_file = open(get_artifact_offsets_fname(self.path), 'wb')
        self.bucket_offsets_file = open(get_bucket_offsets_fname(self.path), 'wb')
        self.current_data_file = open(get_data_fname(self.path), 'wb')
        self.current_bucket_file = open(self._current_bucket_fname(), 'wb')

    def append_artifact(self, data: bytes):
        current_artifact_offset = self.current_bucket_file.tell()
        offset_b = current_artifact_offset.to_bytes(ARTIFACT_OFFSET_SIZE, ENDIANNESS)
        data_len_b = len(data).to_bytes(ARTIFACT_LEN_SIZE, ENDIANNESS)

        self.artifact_offsets_file.write(offset_b)
        self.current_bucket_file.write(data_len_b)
        self.current_bucket_file.write(data)
        self.artifacts_num += 1

        # TODO: prob we want to flush less frequently ?
        self.current_bucket_file.flush()
        self.artifact_offsets_file.flush()

        # TODO: do this a bit smarter :))
        if self.artifacts_num % 1500 == 0:
            self._finalize_current_bucket()

    def _finalize_current_bucket(self):
        current_bucket_offset = self.current_data_file.tell()
        offset_b = current_bucket_offset.to_bytes(BUCKET_OFFSET_SIZE, ENDIANNESS)
        artifacts_num_b = self.artifacts_num.to_bytes(ARTIFACTS_NUM_SIZE, ENDIANNESS)

        self.bucket_offsets_file.write(offset_b)
        self.bucket_offsets_file.write(artifacts_num_b)

        with open(self._current_bucket_fname(), 'rb') as f_in:
            # depending on size of current_bucket we may want to read it in chunks
            # depending on compression we need to handle this differently
            assert self.compression is None
            self.current_data_file.write(f_in.read())

        self.buckets_num += 1
        self.current_data_file.flush()
        self.bucket_offsets_file.flush()
        self.current_bucket_file.truncate(0)
        self.current_bucket_file.seek(0)

    def finalize(self):
        if self.current_bucket_file.tell() > 0:
            self._finalize_current_bucket()

        metadata = {
            'data_version': self.data_version,
            'compression': self.compression,
            'data_chunks_num': self.data_chunks_num,
            BUCKETS_NUM: self.buckets_num,
            ARTIFACTS_NUM: self.artifacts_num,
            'artifact_offsets_bsize': self.artifact_offsets_file.tell(),
            'bucket_offsets_bsize': self.bucket_offsets_file.tell(),
            'data_file_bsize': [self.current_data_file.tell()],
        }

        write_metadata(self.path, metadata)

        self.artifact_offsets_file.close()
        self.bucket_offsets_file.close()
        self.current_data_file.close()

        assert self.current_bucket_file.tell() == 0
        self.current_bucket_file.close()
        os.remove(self._current_bucket_fname())

    def _current_bucket_fname(self) -> str:
        return os.path.join(self.path, 'current_bucket.bin')
