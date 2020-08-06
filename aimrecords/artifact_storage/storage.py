import os
from typing import Union, Tuple, Optional
from collections import Iterator

from aimrecords.record_storage.writer import Writer
from aimrecords.record_storage.reader import ReaderIterator
from aimrecords.artifact_storage.consts import (
    STORAGE_DIR_NAME,
)


class Storage:
    WRITING_MODE = 'w'
    READING_MODE = 'r'
    MODES = (
        WRITING_MODE,
        READING_MODE,
    )

    def __init__(self, root_path: str, mode: str):
        self.root_path = root_path
        self.storage_path = self._get_storage_path()
        self._artifacts = {}
        assert mode in self.MODES
        self._mode = mode

        if not os.path.isdir(self.storage_path):
            os.makedirs(self.storage_path)

    def __iter__(self):
        return iter(self._artifacts)

    def __del__(self):
        self.close()

    def open(self, artifact_name: str, *args, **kwargs):
        assert artifact_name not in self._artifacts
        artifact_path = self._get_artifact_path(artifact_name)

        if self._mode == self.WRITING_MODE:
            artifact_instance = Writer(artifact_path, *args, **kwargs)
        elif self._mode == self.READING_MODE:
            artifact_instance = ReaderIterator(artifact_path, *args, **kwargs)
        else:
            raise TypeError('undefined mode')

        self._artifacts.update({
            artifact_name: artifact_instance,
        })

    def append_record(self, artifact_name: str, data: bytes,
                      indexing: Optional[dict] = None) -> int:
        assert self._mode == self.WRITING_MODE

        artifact = self._get_artifact(artifact_name)
        artifact.append_record(data, indexing)

        return artifact.records_num

    def flush(self, artifact_name: str = None):
        assert self._mode == self.WRITING_MODE

        if artifact_name:
            artifact = self._get_artifact(artifact_name)
            artifact.flush()
        else:
            for a in self._artifacts.values():
                a.flush()

    def read_records(self, artifact_name: str,
                     record_indices: Optional[Union[int, Tuple[int, ...],
                                                    slice]] = None,
                     indexing: Optional[dict] = None) -> Iterator:
        assert self._mode == self.READING_MODE

        artifact = self._get_artifact(artifact_name)

        if indexing is not None:
            artifact.apply_index(indexing)

        return artifact[record_indices]

    def get_records_num(self, artifact_name: str,
                        indexing: Optional[dict] = None) -> int:
        artifact = self._get_artifact(artifact_name)
        return artifact.get_records_num(indexing)

    def get_modification_time(self, artifact_name: str) -> float:
        assert self._mode == self.READING_MODE
        artifact = self._get_artifact(artifact_name)
        return artifact.get_modification_time()

    def close(self, artifact_name: str = None):
        if artifact_name:
            artifact = self._get_artifact(artifact_name)
            artifact.close()
            del self._artifacts[artifact_name]
        else:
            while len(self._artifacts):
                artifact_name = list(self._artifacts.keys())[0]
                artifact_inst = self._artifacts[artifact_name]
                artifact_inst.close()
                del self._artifacts[artifact_name]

    def _get_artifact(self, artifact_name: str
                      ) -> Union[Writer, ReaderIterator]:
        artifact = self._artifacts.get(artifact_name)
        if artifact is None:
            raise ValueError('artifact {} is not in ' +
                             'storage'.format(artifact_name))
        return artifact

    def _get_storage_path(self) -> str:
        return os.path.join(self.root_path, STORAGE_DIR_NAME)

    def _get_artifact_path(self, name: str) -> str:
        return os.path.join(self.storage_path, name)
