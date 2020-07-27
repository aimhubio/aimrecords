import os


def get_index_offsets_fname(path: str) -> str:
    return os.path.join(path, 'index_offsets.bin')


def get_indices_dir_path(path: str) -> str:
    return os.path.join(path, 'indices')
