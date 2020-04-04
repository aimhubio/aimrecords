from aimrecords import Storage


aim_storage = Storage()

with aim_storage.write('nums', mode='w', compression='gzip') as writer:
    for i in range(1000):
        writer.append_record('{},{},{}'.format(i, 2*i, 3*i).encode())

with aim_storage.read('nums') as reader:
    # Forward traversal
    for record in reader[:]:
        print(record.decode())

    # Reverse traversal
    for record in reader[::-1]:
        print(record.decode())

    # Select even records
    for record in reader[::2]:
        print(record.decode())

    # Select records
    for record in reader[1, 2, 50]:
        print(record.decode())

    # Print number of records
    print(len(reader))
