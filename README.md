# Key-Value Store using LSM tree

Jiecheng Shi([@Dominicsjc](https://github.com/Dominicsjc)) and Lianke Qin([@brucechin](https://github.com/brucechin))

**Goal**

implement a Log-Structured Merge tree based key-value store in Rust language. achieve high performance read and write operations. support range read operation. support parallel operation. Use bloom filter to improve read efficiency. memtable is append-only to improve the throughput for write-dominated workloads. When a memtable reach its full capacity, it will be flushed back to disk for persistence.

**API design**

[LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)

```rust
    pub fn new(buf_max_entries: u64, dep: u64, fanout: u64, bf_bits_per_entry: f32, num_threads: u64, tree_name:  &str) -> LSMTree;
    pub fn put(&mut self, key_str: &str, value_str: &str) -> bool;
    pub fn get(&self, key_str: &str) -> Option<String>;
    pub fn range(&self, start_str: &str, end_str: &str) -> Vec<String>;
    pub fn del(&mut self, key_str: &str);
    pub fn close(&mut self);
    pub fn open(&mut self, filename: &str);
```





**Test cases**

    1. test open/close database file.
    2. test put(key, value) operations.
    3. test put(key, value) then get(key) operations.
    4. test put(key, value) then delete(key) operations.
    5. test put(key, value) then update(key) operations.
    6. test range(start_key, end_key) query operations.
    7. test many put operations then compaction process.
    8. test/benchmarking throughput under different workloads(read/write ratio).


**Simple Example**

```rust

    use lsm_kv::lsm;
    let mut lsm = lsm::LSMTree::new(100, 5, 10, 0.5, 4, "doc_test".to_string());
    lsm.put("hello", "world");
    lsm.put("facebook", "google");
    lsm.put("amazon", "linkedin");
    assert_eq!(lsm.get("hello"), Some("world".to_string()));
    assert_eq!(lsm.get("facebook"), Some("google".to_string()));
    lsm.del("hello");
    assert_eq!(lsm.get("hello"), None);
    lsm.range("amazon", "facebook");
    lsm.close();
    let mut lsm2 = lsm::LSMTree::new(100, 5, 10, 0.5, 4, "doc_test".to_string());
    lsm2.load();
    assert_eq!(lsm2.get("hello"), None);
    assert_eq!(lsm2.get("facebook"), Some("google".to_string()));

```

**Benchmark**