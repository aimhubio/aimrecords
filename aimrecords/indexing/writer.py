from aimrecords.indexing.base import IndexBase
from aimrecords.indexing.consts import (
    INDEX_OFFSET_SIZE,
    ENDIANNESS,
)


class IndexWriter(IndexBase):
    def register_record(self, record_index):
        record_index_b = record_index.to_bytes(INDEX_OFFSET_SIZE, ENDIANNESS)
        self._get_offset_file().write(record_index_b)
        self.num_appended += 1

    def flush(self):
        self._get_offset_file().flush()

    def indexed_records_num(self):
        self.flush()
        return super(IndexWriter, self).indexed_records_num()
