## The Bucket Format
The file is segmented into what are called buckets.
A bucket is a collection of consecutive records/events that 
are optionally compressed together, and each bucket has it's type and header.
Buckets are intended to be O(MB)-sized, i.e. large enough for 
efficient compression and also much larger than the header data.

### Bucket Types

 - simple bucket with records
 - transposed bucket with records(?)

### Header
Each bucket has a header that describes the bucket.
 - magic number: a special sequence of 128 bits that identifies the start
of the bucket header
 - bucket_type: determines how to interpret data
 - num_records: number of records after decoding
 - compression_type: 
 
#### Header metadata fields
 - header_hash: hash of the rest of the header
 - header_size: size of the header
 - header metadata: protobuf object
 
#### Data fields
 - data_size: size of data (excluding intervening block headers)
 - data_hash: hash of data

### Bucket Contents
The contents of a bucket immediately follow the bucket header,
and consist of an optionally compressed  set of consecutive protobuf messages

### Compression
If the bucket is compressed as indicated by the protobuf header data,
the entire bucket is compressed together, and must be uncompressed 
prior to deserializing records.

### Records
Each record is a protobuf object.
