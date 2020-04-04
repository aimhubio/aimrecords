import tempfile

from aimrecords import Storage


class TestBasicStuff(object):
    def test_simple_int(self):
        len = 1000

        with tempfile.TemporaryDirectory() as temp_dir:
            aim_storage = Storage(temp_dir)

            with aim_storage.write('loss') as writer:
                for i in range(len):
                    writer.append_record(str(i).encode())

            with aim_storage.read('loss') as reader:
                assert reader.records_num == len
                for i, record in enumerate(reader[:]):
                    assert i == int(record.decode())

            with aim_storage.write('accuracy') as writer:
                for i in range(len, 2*len):
                    writer.append_record(str(i).encode())

            with aim_storage.read('accuracy') as reader:
                assert reader.records_num == len
                for i, record in enumerate(reader[:]):
                    assert i+len == int(record.decode())
