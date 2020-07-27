import os

from aimrecords import Storage


storage_path = os.getcwd()

# Append records
writer_storage = Storage(storage_path, 'w')

writer_storage.open('loss')
for step in range(100):
    writer_storage.append_record('loss',
                                 str(step).encode(),
                                 indexing='train')
    if step % 4 == 0:
        writer_storage.append_record('loss',
                                     str(step).encode(),
                                     indexing='val')

writer_storage.close()

# Select records
reader_storage = Storage(storage_path, 'r')

reader_storage.open('loss')
for r in reader_storage.read_records('loss', slice(0, 20)):
    print(r)

print('-' * 25)

for r in reader_storage.read_records('loss', slice(0, 10), indexing='train'):
    print(r)

print('-' * 25)

for r in reader_storage.read_records('loss', slice(0, 10), indexing='val'):
    print(r)

reader_storage.close()
