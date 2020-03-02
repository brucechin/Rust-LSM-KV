use crate::bloom_filter;

pub struct Entry {
    //TODO key and value set as i32 or Vec<u8>? if Vec<u8> we should fix its size for easier implementation. not sure about how to handle disk I/O problem.
}

pub struct Run {
    pub bloom_filer: bloom_filter::BloomFilter,
    pub filename: String,
    //in cs265-lsm, one run is a SSTable, corresponding to one file on disk.
    pub size: usize,
    //current size
    pub capacity: usize,  //after reach capacity, sort it then become immutable
}

impl Run {
    pub fn new() {}

    pub fn get(key: Vec<u8>) -> Vec<u8> {}

    pub fn put(entry: Entry) {}
}
