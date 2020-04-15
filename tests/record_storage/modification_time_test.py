import os
import tempfile

from aimrecords.record_storage.writer import Writer
from aimrecords.record_storage.reader import Reader


class TestModTime(object):
    def test_modification_time(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression=None)
            length = 100
            for index in range(length):
                writer.append_record(str(index).encode())
            writer.close()

            reader = Reader(path)
            first_mod_time = reader.get_modification_time()

            writer = Writer(path, compression=None)
            length = 100
            for index in range(length):
                writer.append_record(str(index).encode())
            writer.close()

            reader = Reader(path)
            second_mod_time = reader.get_modification_time()

            assert first_mod_time != second_mod_time
