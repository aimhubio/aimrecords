import os

from aimrecords import Storage


storage_path = os.getcwd()

# Append records
writer_storage = Storage(storage_path, 'w')

writer_storage.open('loss')
for step in range(1000):
    writer_storage.append_record('loss', str(step).encode(),
                                  secondary_indexing={'val': step})
    if step % 50 == 0:
        writer_storage.flush()
writer_storage.close()

# Select records
reader_storage = Storage(storage_path, 'r')

reader_storage.open('loss')
for r in reader_storage.read_records('loss', 0):
    print(r)
for r in reader_storage.read_records('loss', (1, 100, -1)):
    print(r)
for r in reader_storage.read_records('loss', slice(1, 10)):
    print(r)
for r in reader_storage.read_records('loss', slice(-20, None)):
    print(r)
reader_storage.close()
