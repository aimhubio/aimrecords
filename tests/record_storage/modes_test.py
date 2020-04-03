import os
import tempfile

from aimrecords.record_storage.reader import Reader
from aimrecords.record_storage.writer import Writer


class TestWriteAppendModes(object):
    def test_append_mode_binary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            length = 1000
            chunks = 5
            chunk_len = length // chunks

            for chunk in range(chunks):
                writer = Writer(path, 'a')
                for index in range(chunk * chunk_len, (chunk + 1) * chunk_len):
                    entry = (str(index) * index).encode()
                    writer.append_record(entry)
                writer.finalize()

            reader = Reader(path)
            assert len(reader) == length

            for index in range(length):
                entry = (str(index) * index).encode()
                assert entry == reader.get(index)

    def test_write_mode_binary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            length = 1000

            writer = Writer(path, 'w')
            for index in range(length // 2):
                writer.append_record(b'0')
            writer.finalize()

            writer = Writer(path, 'w')
            for index in range(length // 2, length):
                entry = (str(index) * index).encode()
                writer.append_record(entry)
            writer.finalize()

            reader = Reader(path)
            assert len(reader) == length // 2

            for index in range(length // 2, length):
                entry = (str(index) * index).encode()
                assert entry == reader.get(index - length // 2)
