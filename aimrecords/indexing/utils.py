import os
from typing import List

from aimrecords.indexing.consts import (
    INDEX_OFFSET_SIZE,
    INDEX_FILE_NAME,
)


def get_index_offsets_fname(path: str) -> str:
    return os.path.join(path, INDEX_FILE_NAME)


def get_indices_dir_path(path: str) -> str:
    return os.path.join(path, 'indices')


def main_index_file_exists(path: str) -> bool:
    indices_dir_path = get_indices_dir_path(path)
    main_index_path = os.path.join(indices_dir_path, INDEX_FILE_NAME)
    return os.path.isfile(main_index_path)


def list_indices(path: str) -> List[str]:
    indices_dir_path = get_indices_dir_path(path)
    if not os.path.isdir(indices_dir_path):
        return []
    return list(filter(lambda i: i != INDEX_FILE_NAME,
                       os.listdir(indices_dir_path)))


def count_records_num_from_index(path: str, index_key: str) -> int:
    indices_dir_path = get_indices_dir_path(path)
    index_dir_path = os.path.join(indices_dir_path, index_key)
    index_file_path = get_index_offsets_fname(index_dir_path)

    if not os.path.isfile(index_file_path):
        return 0

    records_num = int(os.path.getsize(index_file_path) / INDEX_OFFSET_SIZE)

    return int(records_num)


def normalize_type(val):
    if isinstance(val, str):
        if val == 'True':
            return True
        elif val == 'False':
            return False
        elif val == 'None':
            return None

        try:
            val = int(val)
        except:
            try:
                val = float(val)
            except:
                pass
    return val
