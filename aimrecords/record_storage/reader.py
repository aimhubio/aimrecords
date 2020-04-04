from typing import Union, Tuple

from aimrecords.record_storage.consts import (
    RECORD_OFFSET_SIZE,
    BUCKET_OFFSET_SIZE,
    RECORDS_NUM_SIZE,
    ENDIANNESS,
    RECORDS_NUM,
    BUCKETS_NUM,
    COMPRESSION,
)

from aimrecords.record_storage.utils import (
    read_metadata,
    get_data_fname,
    get_bucket_offsets_fname,
    get_record_offsets_fname,
)


class Reader:
    def __init__(self, path: str):
        self.path = path
        self.metadata = read_metadata(path)
        self._validate()

        assert self.metadata[COMPRESSION] is None
        self.buckets_num = self.metadata[BUCKETS_NUM]
        self.records_num = self.metadata[RECORDS_NUM]
        self.data_file = open(get_data_fname(self.path), 'rb')
        self.record_index_to_offset_list = self._get_record_index_to_offset_list()

        self._iter = None

    def __getitem__(self, item: Union[int, Tuple[int, ...], slice]):
        if isinstance(item, int):
            self._iter = iter((item,))
        elif isinstance(item, tuple):
            if any(map(lambda i: not isinstance(i, int), item)):
                raise TypeError('tuple indices must be integers')
            self._iter = iter(item)
        elif isinstance(item, slice):
            start, stop, step = item.start, item.stop, item.step
            indices_range = range(self.records_num)
            self._iter = iter(indices_range[start:stop:step])
        else:
            raise TypeError('expected slice or tuple of indices')

        return iter(self)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            idx = next(self._iter)
            return self.get(idx)
        except (IndexError, StopIteration):
            self._iter = None
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self

    def __del__(self):
        self.data_file.close()

    def get(self, index: int) -> bytes:
        if index >= len(self.record_index_to_offset_list):
            raise IndexError('index out of range')

        offset = self.record_index_to_offset_list[index]
        self.data_file.seek(offset)
        size = int.from_bytes(self.data_file.read(RECORD_OFFSET_SIZE), ENDIANNESS)
        return self.data_file.read(size)

    def _get_record_index_to_offset_list(self) -> list:
        bucket_offset_records_num_list = []
        with open(get_bucket_offsets_fname(self.path), 'rb') as f_in:
            for _ in range(self.buckets_num):
                bucket_offset = int.from_bytes(f_in.read(BUCKET_OFFSET_SIZE), ENDIANNESS)
                records_num = int.from_bytes(f_in.read(RECORDS_NUM_SIZE), ENDIANNESS)

                bucket_offset_records_num_list.append((bucket_offset, records_num))

        record_index_to_offset_list = []
        with open(get_record_offsets_fname(self.path), 'rb') as f_in:
            for record_index in range(self.records_num):
                if record_index >= bucket_offset_records_num_list[0][1]:
                    bucket_offset_records_num_list.pop(0)

                bucket_offset = bucket_offset_records_num_list[0][0]
                record_local_offset = int.from_bytes(f_in.read(RECORD_OFFSET_SIZE), ENDIANNESS)
                record_index_to_offset_list.append(bucket_offset + record_local_offset)

        return record_index_to_offset_list

    def _validate(self):
        # TODO: implement
        pass
