import os
import time

from aimrecords import Storage


storage_path = os.getcwd()

# Append records
writer_storage = Storage(storage_path, 'w')

writer_storage.open('loss')
for step in range(10000):
    writer_storage.append_record('loss', str(step).encode())
    writer_storage.flush()
    time.sleep(0.5)
writer_storage.close()
