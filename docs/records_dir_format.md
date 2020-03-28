## Directory Format
Each directory represents a single object and consists from 4 required files.

- Header file: contains general info about the directory
- Data file: contains all the stored records
- Records offset file
- Buckets offset file

### Header file
A light json file containing all general info about the files of the directory
including file hashes for validation, decompressed file sizes, metadata
and other useful information.

TODO: Enumerate all required properties
