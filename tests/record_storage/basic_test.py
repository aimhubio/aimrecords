import os
import tempfile

from aimrecords.record_storage.reader import Reader
from aimrecords.record_storage.writer import Writer


class TestBasicStuff(object):
    def test_simple_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path)
            length = 1000
            for i in range(length):
                writer.append_record(str(i).encode())
            writer.finalize()

            reader = Reader(path)
            assert len(reader) == length

            for index in range(length):
                assert index == int(reader.get(index).decode())

    def test_simple_binary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path)
            length = 1000
            for index in range(length):
                entry = (str(index) * index).encode()
                writer.append_record(entry)
            writer.finalize()

            reader = Reader(path)
            assert len(reader) == length

            for index in range(length):
                entry = (str(index) * index).encode()
                assert entry == reader.get(index)
