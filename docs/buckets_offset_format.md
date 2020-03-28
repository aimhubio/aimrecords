## The Buckets Offset File Format

Offset file represents a set of consecutive tuples of values of types (`uint64`, `uint32`).
The first value is an offset of index of corresponding bucket inside data file.
And the second is the total number of records inside the bucket.
