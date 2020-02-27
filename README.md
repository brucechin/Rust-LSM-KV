# Key-Value Store using LSM tree

Jiecheng Shi([@Dominicsjc](https://github.com/Dominicsjc)) and Lianke Qin([@brucechin](https://github.com/brucechin))

**Goal**

implement a Log-Structured Merge tree based key-value store in Rust language. achieve high performance read and write operations. support range read operation. support parallel operation. Use bloom filter to improve read efficiency. memtable is append-only to improve the throughput for write-dominated workloads. When a memtable reach its full capacity, it will be flushed back to disk for persistence.

**API design**

1. [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter)
    1. bloom_check
    2. bloom_add
    3. other hash functions
2. LSM tree
    1. new(data-file-path)
    2. deconstructor()
    3. search_buffer(key)
    4. search_disk(key)
    5. get(key)
    6. put(key, value)
    7. delete(key)
    8. range operation
    9. merge() //compact multiple SSTable to a larger one and remove duplicate entries and only save the latest unique entry.
3. File helper
    1. read(pos, len)
    2. write(pos)



**Test cases**

1. test open database file
2. test put(key, value) operations
3. test put(key, value) then get(key) operations
4. test put(key, value) then delete(key) operations.
5. test put(key, value) then update(key) operations.
6. test range operations
7. test throughput under different workloads(read/write ratio)


**Example**

