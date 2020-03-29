## The Data File Format
The file is segmented into what are called buckets.
A bucket is a collection of consecutive records/events that 
are optionally compressed together. Buckets are intended to be O(MB)-sized,
i.e. large enough for efficient compression and optimal for fast decompression.

### Bucket Contents
The bucket consist of an optionally compressed set of consecutive protobuf messages.

### Compression
The entire bucket is compressed together, and must be uncompressed 
prior to deserializing records.

### Records
Each record represents a protobuf object.
