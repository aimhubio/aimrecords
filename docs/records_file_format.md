## Records File Format
Each file has its signature and keeps buckets of records in a sequential order.

### File signature
A file signature chunk must be present at the beginning of the file.

- data_size: File data size after decompression
- num_buckets: Number of buckets
- metadata: protobuf object
- signature_size: size of the signature
- etc
