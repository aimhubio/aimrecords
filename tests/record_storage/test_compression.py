import os
import tempfile
import unittest

from aimrecords.record_storage.reader import Reader
from aimrecords.record_storage.writer import Writer


class TestBucketCompression(unittest.TestCase):
    def test_gzip_compression(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression='gzip')
            length = 1000
            for i in range(length):
                writer.append_record(str(i).encode())
            writer.close()

            reader = Reader(path)
            assert reader.get_records_num() == length

            for index in range(length):
                assert index == int(reader.get(index).decode())
