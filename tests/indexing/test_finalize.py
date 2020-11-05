import tempfile
import unittest

from aimrecords.artifact_storage.storage import Storage


class TestFinalize(unittest.TestCase):
    def test_storage_uncommitted_data_int(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_writer = Storage(temp_dir, 'w')
            storage_writer.open('loss')

            for i in range(10):
                storage_writer.append_record('loss', str(i*10).encode(), {})
            for i in range(10):
                storage_writer.append_record('loss', str(i*20).encode(),
                                             {'subset:': 'train'})
            for i in range(10):
                storage_writer.append_record('loss', str(i*30).encode(),
                                             {'subset': 'val'})
            for i in range(10):
                storage_writer.append_record('loss', str(i*40).encode(),
                                             {'subset:': 'train'})
            for i in range(10):
                storage_writer.append_record('loss', str(i*50).encode(),
                                             {'subset': 'val'})
            storage_writer.flush()

            storage_reader = Storage(temp_dir, 'r')
            storage_reader.open('loss', uncommitted_bucket_visible=False)

            assert storage_reader.get_records_num('loss',
                                                  {'subset:': 'train'}) == 0
            assert storage_reader.get_records_num('loss',
                                                  {'subset': 'val'}) == 0

            storage_reader.close()

            storage_writer_2 = Storage(temp_dir, 'w')
            storage_writer_2.finalize_dangling_artifacts()

            storage_reader = Storage(temp_dir, 'r')
            storage_reader.open('loss', uncommitted_bucket_visible=False)

            assert storage_reader.get_records_num('loss',
                                                  {'subset:': 'train'}) == 20
            assert storage_reader.get_records_num('loss',
                                                  {'subset': 'val'}) == 20
            assert storage_reader.get_records_num('loss', {}) == 10
            assert storage_reader.get_records_num('loss') == 50

            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 10), {})):
                assert i * 10 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 10),
                                                {'subset:': 'train'})):
                assert i * 20 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(10, 20),
                                                {'subset:': 'train'})):
                assert i * 40 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(0, 10),
                                                {'subset': 'val'})):
                assert i * 30 == int(record.decode())
            for i, record in enumerate(
                    storage_reader.read_records('loss', slice(10, 20),
                                                {'subset': 'val'})):
                assert i * 50 == int(record.decode())
