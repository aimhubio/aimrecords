import tempfile
import unittest

from aimrecords import Storage


class TestMulitpleArtifacts(unittest.TestCase):
    def test_simple_int(self):
        len = 1000

        with tempfile.TemporaryDirectory() as temp_dir:
            storage_writer = Storage(temp_dir, 'w')

            storage_writer.open('loss')
            for i in range(len):
                storage_writer.append_record('loss', str(i).encode())
            storage_writer.close('loss')

            storage_writer.open('accuracy')
            for i in range(len, 2*len):
                storage_writer.append_record('accuracy', str(i).encode())
            storage_writer.close('accuracy')

            del storage_writer

            storage_reader = Storage(temp_dir, 'r')

            storage_reader.open('loss')
            assert storage_reader.get_records_num('loss') == len
            for i, record in enumerate(storage_reader.read_records('loss')):
                assert i == int(record.decode())
            storage_reader.close('loss')

            storage_reader.open('accuracy')
            assert storage_reader.get_records_num('accuracy') == len
            for i, record in enumerate(storage_reader.read_records('accuracy')):
                assert i + len == int(record.decode())
            storage_reader.close('accuracy')

            del storage_reader
