from aimrecords.indexing.base import IndexBase

from aimrecords.indexing.consts import (
    INDEX_OFFSET_SIZE,
    ENDIANNESS,
)


class Reader(IndexBase):
    def __len__(self) -> int:
        return self.indexed_records_num()

    def get(self, record_index: int):
        if record_index >= self.indexed_records_num():
            raise IndexError('index out of range')

        offset_file = self._get_offset_file()
        offset_file.seek(INDEX_OFFSET_SIZE * record_index)

        record_loc = int.from_bytes(offset_file.read(INDEX_OFFSET_SIZE),
                                    ENDIANNESS)

        return record_loc
