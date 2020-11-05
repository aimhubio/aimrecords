import os
import json
from typing import List

from aimrecords.indexing.index_key import IndexKey
from aimrecords.indexing.utils import list_indices
from aimrecords.record_storage.consts import (
    BUCKET_OFFSET_SIZE,
    RECORDS_NUM_SIZE,
    RECORD_OFFSET_SIZE,
)


def get_data_fname(path: str) -> str:
    return os.path.join(path, 'data_chunk_00000.bin')


def get_record_offsets_fname(path: str) -> str:
    return os.path.join(path, 'record_offsets.bin')


def get_bucket_offsets_fname(path: str) -> str:
    return os.path.join(path, 'bucket_offsets.bin')


def get_metadata_fname(path: str) -> str:
    return os.path.join(path, 'metadata.json')


def current_bucket_fname(path: str) -> str:
    return os.path.join(path, 'current_bucket.bin')


def metadata_exists(path: str) -> bool:
    return os.path.isfile(get_metadata_fname(path))


def write_metadata(path: str, metadata: dict):
    with open(get_metadata_fname(path), 'w') as f_out:
        json.dump(metadata, f_out)


def read_metadata(path: str) -> dict:
    if metadata_exists(path):
        with open(get_metadata_fname(path), 'r') as f_in:
            return json.load(f_in)
    return {}


def data_version_compatibility(prev_version: str, version: str):
    assert 0 <= int(version) - int(prev_version) <= 1


def count_records_num_from_disk(path: str) -> int:
    record_file_path = get_record_offsets_fname(path)
    if not os.path.isfile(record_file_path):
        return 0

    records_num = int(os.path.getsize(record_file_path) / RECORD_OFFSET_SIZE)

    return records_num


def count_buckets_num_from_disk(path: str) -> int:
    bucket_file_path = get_bucket_offsets_fname(path)
    if not os.path.isfile(bucket_file_path):
        return 0

    bucket_info_block_size = BUCKET_OFFSET_SIZE + RECORDS_NUM_SIZE
    buckets_num = os.path.getsize(bucket_file_path) / bucket_info_block_size

    return int(buckets_num)


def get_indices_keys(path: str) -> List[IndexKey]:
    indices_encoded_names_list = list_indices(path)
    indices_keys_list = [IndexKey.from_str(k)
                         for k in indices_encoded_names_list
                         ]
    return indices_keys_list
