use crate::bloom_filter;
use crate::data_type;

pub struct Run {
    pub bloom_filer: bloom_filter::BloomFilter,
    pub filename: String,
    //in cs265-lsm, one run is a SSTable, corresponding to one file on disk.
    pub size: usize,
    //current size
    pub capacity: usize, //after reach capacity, sort it then become immutable
}

impl Run {
    pub fn new() {}

    pub fn get(key: data_type::KeyT) -> Vec<u8> {
        unimplemented!()
    }

    pub fn put(entry: data_type::EntryT) {}
}
