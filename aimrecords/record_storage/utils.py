import os
import json


def get_data_fname(path: str) -> str:
    return os.path.join(path, 'data_chunk_00000.bin')


def get_record_offsets_fname(path: str) -> str:
    return os.path.join(path, 'record_offsets.bin')


def get_bucket_offsets_fname(path: str) -> str:
    return os.path.join(path, 'bucket_offsets.bin')


def get_metadata_fname(path: str) -> str:
    return os.path.join(path, 'metadata.json')


def write_metadata(path: str, metadata: dict):
    with open(get_metadata_fname(path), 'w') as f_out:
        json.dump(metadata, f_out)


def read_metadata(path: str) -> dict:
    with open(get_metadata_fname(path), 'r') as f_in:
        return json.load(f_in)
