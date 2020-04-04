from .consts import (
    ARTIFACT_OFFSET_SIZE,
    BUCKET_OFFSET_SIZE,
    ARTIFACTS_NUM_SIZE,
    ENDIANNESS,
    ARTIFACTS_NUM,
    BUCKETS_NUM,
    COMPRESSION,
)

from .utils import (
    read_metadata,
    get_data_fname,
    get_bucket_offsets_fname,
    get_artifact_offsets_fname,
)


class Reader(object):
    def __init__(self, path: str):
        self.path = path
        self.metadata = read_metadata(path)
        self._validate()

        assert self.metadata[COMPRESSION] is None
        self.buckets_num = self.metadata[BUCKETS_NUM]
        self.artifacts_num = self.metadata[ARTIFACTS_NUM]
        self.data_file = open(get_data_fname(self.path), 'rb')
        self.artifact_index_to_offset_list = self._get_artifact_index_to_offset_list()

    def _get_artifact_index_to_offset_list(self) -> list:
        bucket_offset_artifacts_num_list = []
        with open(get_bucket_offsets_fname(self.path), 'rb') as f_in:
            for _ in range(self.buckets_num):
                bucket_offset = int.from_bytes(f_in.read(BUCKET_OFFSET_SIZE), ENDIANNESS)
                artifacts_num = int.from_bytes(f_in.read(ARTIFACTS_NUM_SIZE), ENDIANNESS)

                bucket_offset_artifacts_num_list.append((bucket_offset, artifacts_num))

        artifact_index_to_offset_list = []
        with open(get_artifact_offsets_fname(self.path), 'rb') as f_in:
            for artifact_index in range(self.artifacts_num):
                if artifact_index >= bucket_offset_artifacts_num_list[0][1]:
                    bucket_offset_artifacts_num_list.pop(0)

                bucket_offset = bucket_offset_artifacts_num_list[0][0]
                artifact_local_offset = int.from_bytes(f_in.read(ARTIFACT_OFFSET_SIZE), ENDIANNESS)
                artifact_index_to_offset_list.append(bucket_offset + artifact_local_offset)

        return artifact_index_to_offset_list

    def get(self, index: int) -> bytes:
        assert index < len(self.artifact_index_to_offset_list)
        offset = self.artifact_index_to_offset_list[index]
        self.data_file.seek(offset)
        size = int.from_bytes(self.data_file.read(ARTIFACT_OFFSET_SIZE), ENDIANNESS)
        return self.data_file.read(size)

    def _validate(self):
        # TODO: implement
        pass

    def __del__(self):
        self.data_file.close()
