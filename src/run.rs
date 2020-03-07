use crate::bloom_filter;
use crate::data_type::{EntryT, KeyT, ValueT};
use page_size;
use std::fs::File;
use std::mem::size_of;

pub struct Run<'a> {
    bloom_filer: bloom_filter::BloomFilter,
    fence_pointers: Vec<KeyT>,
    max_key: KeyT,
    mapping: Option<&'a EntryT>,
    mapping_length: usize,
    mapping_fd: Option<File>,
    size: u64,
    max_size: u64,
    tmp_file: &'a str,
}

impl<'a> Run<'a> {
    pub fn new(max_size: u64, bf_bits_per_entry: u64) -> Run<'a> {
        Run {
            bloom_filer: bloom_filter::BloomFilter::new_with_size(max_size * bf_bits_per_entry),
            fence_pointers: Vec::with_capacity((max_size / page_size::get() as u64) as usize),
            max_key: KeyT::default(),
            mapping: None,
            mapping_length: 0,
            mapping_fd: None,
            size: 0,
            max_size: max_size,
            tmp_file: "/tmp/lsm-XXXXXX",
        }
    }

    pub fn map_read_default(&self) -> EntryT {
        unimplemented!()
    }

    pub fn map_read(&self, len: usize, offset: usize) -> EntryT {
        unimplemented!()
    }

    pub fn map_write(&self) -> EntryT {
        unimplemented!()
    }

    pub fn unmap(&self) {
        unimplemented!()
    }

    pub fn get(&self, key: KeyT) -> Option<ValueT> {
        unimplemented!()
    }

    pub fn range(&self, start: KeyT, end: KeyT) -> Vec<EntryT> {
        unimplemented!()
    }

    pub fn put(&mut self, key: KeyT, value: ValueT) -> bool {
        unimplemented!()
    }

    fn file_size(&self) -> u64 {
        self.max_size * size_of::<EntryT>() as u64
    }
}
