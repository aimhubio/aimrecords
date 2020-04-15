import os
import time

from aimrecords import Storage


storage_path = os.getcwd()

# Select records
reader_storage = Storage(storage_path, 'r')

cursor = None
while True:
    reader_storage.open('loss', uncommitted_bucket_visible=True)
    for r in reader_storage.read_records('loss', slice(cursor, None)):
        print(r)
        cursor = int(r.decode()) + 1
    reader_storage.close('loss')
    time.sleep(0.01)
