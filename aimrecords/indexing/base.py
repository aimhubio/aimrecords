import os

from aimrecords.indexing.index_key import IndexKey
from aimrecords.indexing.utils import (
    get_index_offsets_fname,
    get_indices_dir_path,
)
from aimrecords.indexing.consts import (
    INDEX_OFFSET_SIZE,
)


class IndexBase(object):
    def __init__(self, root_path: str, index_key: IndexKey, mode: str):
        self.root_path = root_path
        self.index_key = index_key
        self.name = str(index_key)
        self.num_appended = 0
        self.mode = mode
        self._indices_path = get_indices_dir_path(root_path)
        self._path = os.path.join(self._indices_path, self.name)
        self._offset_file_path = get_index_offsets_fname(self._path)
        self._offset_file = None

    def indexed_records_num(self):
        return int(os.path.getsize(self._offset_file_path) / INDEX_OFFSET_SIZE)

    def close(self):
        self._get_offset_file().close()

    def _get_offset_file(self):
        if not os.path.isdir(self._path):
            if self.mode == 'rb':
                raise FileNotFoundError('index file is not found')
            else:
                os.makedirs(self._path, exist_ok=True)

        if self._offset_file is None:
            if not os.path.exists(self._offset_file_path):
                if self.mode == 'rb':
                    raise FileNotFoundError('index file is not found')
                else:
                    self._offset_file = open(self._offset_file_path, 'wb')
            else:
                self._offset_file = open(self._offset_file_path, self.mode)

        return self._offset_file
