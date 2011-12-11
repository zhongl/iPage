# Offset and Chunk

* Offset is global long value, started from 0.
* Every chunk has fixed size (default is 4096), and named by offset.

For example:

    offset: 0              4096             8192             12288
            |----------------|----------------|----------------|----------------|
            | <- chunk[0] -> | <- chunk[1] -> | <- chunk[2] -> | <- chunk[3] -> |

The name of `chunk[0]` is `0`, and so on.

A given offset can easy be located at only one chunk by binary search.

# Index

TODO

# Engine and Non-Blocking

TODO

# Garbage collect

TODO

# Recovery

TODO
