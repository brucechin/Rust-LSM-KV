# Rust-LSM-KV

**Goal**

implement a Log-Structured Merge tree based key-value store in Rust language. achieve high performance read and write operations. support range read operation. support parallel operation. 

**API design**

1. bloom filter(https://en.wikipedia.org/wiki/Bloom_filter)
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
    9. merge()



**Test cases**

1. test open database file
2. test put(key, value) operations
3. test put(key, value) then get(key) operations
4. test put(key, value) then delete(key) operations.
5. test put(key, value) then update(key) operations.
6. test range operations
7. test throughput under different workloads(read/write ratio)


**Example**
