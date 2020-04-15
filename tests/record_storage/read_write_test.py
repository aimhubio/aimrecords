import os
import tempfile

from aimrecords.record_storage.reader import Reader
from aimrecords.record_storage.writer import Writer


class TestReadWrite(object):
    def test_dirty_read(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression='gzip')

            length = 1000
            for index in range(length):
                writer.append_record(str(index).encode())
                writer.flush()

                reader = Reader(path, uncommitted_bucket_visible=True)
                assert reader.get_records_num() == index + 1
                assert index == int(reader.get(index).decode())
                reader.close()

            writer.close()

    def test_read_write_on_existing_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression='gzip')
            length = 1000
            for index in range(length):
                writer.append_record(str(index).encode())
            writer.close()

            writer = Writer(path, rewrite=False)
            for index in range(length, 2 * length):
                writer.append_record(str(index).encode())
                writer.flush()

                reader = Reader(path, uncommitted_bucket_visible=True)
                assert reader.get_records_num() == index + 1
                assert index == int(reader.get(index).decode())
                reader.close()

            writer.close()

    def test_uncommitted_read_on_closed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression='gzip')
            length = 1000
            for index in range(length):
                writer.append_record(str(index).encode())
            writer.close()

            reader = Reader(path, uncommitted_bucket_visible=True)
            assert reader.get_records_num() == length
            for index in range(length):
                assert index == int(reader.get(index).decode())
            reader.close()

    def test_read_write(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression=None)

            length = 1000
            for index in range(length):
                writer.append_record(str(index).encode())
                writer.flush()

                reader = Reader(path, uncommitted_bucket_visible=False)
                assert reader.get_records_num() == 0
                reader.close()

            writer.close()
