import os

from aimrecords import Storage


storage_path = os.getcwd()

storage_writer = Storage(storage_path, 'w')
storage_writer.open('test_metric')

for i in range(10):
    storage_writer.append_record('test_metric', str(i).encode(),
                                 {'subset:': 'train'})
for i in range(10):
    storage_writer.append_record('test_metric', str(i * 10).encode(),
                                 {'subset': 'val'})
