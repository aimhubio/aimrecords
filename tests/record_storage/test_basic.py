import os
import tempfile
import unittest

from aimrecords.record_storage.reader import Reader
from aimrecords.record_storage.writer import Writer


class TestBasicStuff(unittest.TestCase):
    def test_simple_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression=None)
            length = 1000
            for index in range(length):
                writer.append_record(str(index).encode())
            writer.close()

            reader = Reader(path)
            assert reader.get_records_num() == length

            for index in range(length):
                assert index == int(reader.get(index).decode())

    def test_simple_binary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression=None)
            length = 5000
            for index in range(length):
                entry = (str(index) * index).encode()
                writer.append_record(entry)
            writer.close()

            reader = Reader(path)
            assert reader.get_records_num() == length

            for index in range(length):
                entry = (str(index) * index).encode()
                assert entry == reader.get(index)
