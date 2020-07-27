import os
import tempfile
import unittest

from aimrecords.record_storage.reader import ReaderIterator, Reader
from aimrecords.record_storage.writer import Writer
from aimrecords.artifact_storage.storage import Storage


class TestIndexing(unittest.TestCase):
    def test_simple_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression=None)
            for index in range(1000):
                writer.append_record(str(index).encode(),
                                     index=('train', 'domain'))
            for index in range(500):
                writer.append_record(str(index).encode(),
                                     index=('val', 'domain'))
            writer.close()

            writer = Writer(path, compression=None)
            for index in range(100):
                writer.append_record(str(index).encode(),
                                     index=('train', 'domain'))
            for index in range(100):
                writer.append_record(str(index).encode(),
                                     index=('val', 'domain'))
            writer.close()

            reader = Reader(path)
            for index in range(1000):
                assert index == int(reader.get(index, ('train', 'domain')))
            for index in range(1000, 1100):
                assert index - 1000 == int(reader.get(index, ('train', 'domain')))
            for index in range(500):
                assert index == int(reader.get(index, ('val', 'domain')))
            for index in range(500, 600):
                assert index - 500 == int(reader.get(index, ('val', 'domain')))
            reader.close()

    def test_iterator_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'loss')

            writer = Writer(path, compression=None)
            for index in range(1000):
                writer.append_record(str(index).encode(),
                                     index=('train', 'domain'))
            for index in range(500):
                writer.append_record(str(index).encode(),
                                     index=('val', 'domain'))
            for index in range(100):
                writer.append_record(str(index).encode(),
                                     index=('train', 'domain'))
            for index in range(100):
                writer.append_record(str(index).encode(),
                                     index=('val', 'domain'))
            writer.close()

            reader = ReaderIterator(path)

            reader.apply_index(('train', 'domain'))
            for i, record in enumerate(reader[0:1000]):
                assert i == int(record)
            for i, record in enumerate(reader[1000:1100]):
                assert i == int(record)

            reader.apply_index(('val', 'domain'))
            for i, record in enumerate(reader[0:500]):
                assert i == int(record)
            for i, record in enumerate(reader[500:600]):
                assert i == int(record)
            for i, record in enumerate(reader[0:600]):
                if i < 500:
                    assert i == int(record)
                else:
                    assert i - 500 == int(record)

            reader.close()

    def test_storage_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_writer = Storage(temp_dir, 'w')
            storage_writer.open('loss')

            for i in range(1000):
                storage_writer.append_record('loss', str(i).encode(), 'train')
            for i in range(100):
                storage_writer.append_record('loss', str(i+100).encode(), 'val')
            for i in range(100):
                storage_writer.append_record('loss', str(i*5).encode(), 'train')
            for i in range(100):
                storage_writer.append_record('loss', str(i).encode(), 'val')

            storage_writer.close()
            del storage_writer

            storage_reader = Storage(temp_dir, 'r')
            storage_reader.open('loss')

            assert storage_reader.get_records_num('loss') == 1300
            assert storage_reader.get_records_num('loss', 'train') == 1100
            assert storage_reader.get_records_num('loss', 'val') == 200

            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 1000),
                                                'train')):
                assert i == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(1000, 1100),
                                                'train')):
                assert i * 5 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 100),
                                                'val')):
                assert i + 100 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(100, 200),
                                                'val')):
                assert i == int(record.decode())

            storage_reader.close()
            del storage_reader

    def test_storage_uncommitted_data_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_writer = Storage(temp_dir, 'w')
            storage_writer.open('loss')

            for i in range(10):
                storage_writer.append_record('loss', str(i).encode())
            for i in range(10):
                storage_writer.append_record('loss', str(i).encode(), 'train')
            for i in range(10):
                storage_writer.append_record('loss', str(i+100).encode(), 'val')
            for i in range(10):
                storage_writer.append_record('loss', str(i*5).encode(), 'train')
            for i in range(10):
                storage_writer.append_record('loss', str(i).encode(), 'val')
            storage_writer.flush()

            storage_reader = Storage(temp_dir, 'r')
            storage_reader.open('loss', uncommitted_bucket_visible=True)

            assert storage_reader.get_records_num('loss', 'train') == 20
            assert storage_reader.get_records_num('loss', 'val') == 20

            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 10))):
                assert i == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 10),
                                                'train')):
                assert i == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(10, 20),
                                                'train')):
                assert i * 5 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 10),
                                                'val')):
                assert i + 100 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(10, 20),
                                                'val')):
                assert i == int(record.decode())

            storage_writer.close()
            del storage_writer

            storage_reader.close()
            del storage_reader

    def test_storage_uncommitted_data_false_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_writer = Storage(temp_dir, 'w')
            storage_writer.open('loss')

            for i in range(10):
                storage_writer.append_record('loss', str(i).encode(), 'train')
            for i in range(10):
                storage_writer.append_record('loss', str(i*10).encode(), 'val')

            storage_reader = Storage(temp_dir, 'r')
            storage_reader.open('loss', uncommitted_bucket_visible=False)

            assert storage_reader.get_records_num('loss') == 0
            assert storage_reader.get_records_num('loss', 'train') == 0
            assert storage_reader.get_records_num('loss', 'val') == 0

            storage_reader.close()
            del storage_reader

            storage_writer.close()
            del storage_writer
