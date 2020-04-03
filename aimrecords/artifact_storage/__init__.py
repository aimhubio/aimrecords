import os

from aimrecords.record_storage.writer import Writer
from aimrecords.record_storage.reader import Reader

from aimrecords.artifact_storage.consts import (
    STORAGE_DIR_NAME,
)


class Storage:
    def __init__(self, root_path: str = None):
        self.root_path = root_path or os.getcwd()
        self.storage_path = self._get_storage_path()

        if not os.path.isdir(self.storage_path):
            os.makedirs(self.storage_path)

    def write(self, artifact_name: str, *args, **kwargs) -> Writer:
        artifact_path = self._get_artifact_path(artifact_name)
        return Writer(artifact_path, *args, **kwargs)

    def read(self, artifact_name: str) -> Reader:
        artifact_path = self._get_artifact_path(artifact_name)
        return Reader(artifact_path)

    def _get_storage_path(self) -> str:
        return os.path.join(self.root_path, STORAGE_DIR_NAME)

    def _get_artifact_path(self, name: str) -> str:
        return os.path.join(self.storage_path, name)
