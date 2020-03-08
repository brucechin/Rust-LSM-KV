use crate::bloom_filter;
use crate::data_type::{EntryT, KeyT, ValueT, KEY_SIZE, VALUE_SIZE};
use libc;
use mktemp::Temp;
use mmap::{MapOption, MemoryMap};
use page_size;
use std::borrow::Borrow;
use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::os::raw;
use std::os::raw::c_void;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

pub struct Run {
    bloom_filer: bloom_filter::BloomFilter,
    fence_pointers: Vec<KeyT>,
    max_key: KeyT,
    mapping: Option<MemoryMap>,
    mapping_fd: raw::c_int,
    size: u64,
    max_size: u64,
    tmp_file: PathBuf,
}

impl Run {
    pub fn new(max_size: u64, bf_bits_per_entry: u64) -> Run {
        Run {
            bloom_filer: bloom_filter::BloomFilter::new_with_size(max_size * bf_bits_per_entry),
            fence_pointers: Vec::with_capacity((max_size / page_size::get() as u64) as usize),
            max_key: KeyT::default(),
            mapping: None,
            mapping_fd: -1,
            size: 0,
            max_size: max_size,
            tmp_file: Temp::new_file_in("/tmp/").unwrap().as_ref().to_owned(),
        }
    }

    pub fn map_read_default(&mut self) -> Vec<EntryT> {
        self.map_read(size_of::<EntryT>() * self.max_size as usize, 0)
    }

    pub fn map_read(&mut self, len: usize, offset: usize) -> Vec<EntryT> {
        assert!(self.mapping.is_none());

        match File::open(self.tmp_file.as_path()) {
            Ok(fd) => {
                self.mapping_fd = fd.as_raw_fd();
            }
            Err(_) => panic!("Open temp file failed!"),
        };
        match MemoryMap::new(
            len,
            &[
                MapOption::MapReadable,
                MapOption::MapFd(self.mapping_fd),
                MapOption::MapOffset(offset),
                MapOption::MapNonStandardFlags(0x01),
            ],
        ) {
            Ok(map) => unsafe {
                assert_eq!(map.len(), KEY_SIZE + VALUE_SIZE);
                self.mapping = Some(map);
                let mut res: Vec<EntryT> = Vec::new();
                for i in 0..self.size {
                    res.push(EntryT {
                        key: std::slice::from_raw_parts(
                            self.mapping
                                .as_ref()
                                .unwrap()
                                .data()
                                .add(size_of::<EntryT>() * i as usize),
                            KEY_SIZE,
                        )
                        .to_vec(),
                        value: std::slice::from_raw_parts(
                            self.mapping
                                .as_ref()
                                .unwrap()
                                .data()
                                .add(size_of::<EntryT>() * i as usize + KEY_SIZE),
                            VALUE_SIZE,
                        )
                        .to_vec(),
                    })
                }
                res
            },
            Err(_) => panic!("Mapping failed!"),
        }
    }

    pub fn map_write(&mut self) {
        assert!(self.mapping.is_none());

        let len = size_of::<EntryT>() * self.max_size as usize;

        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.tmp_file.as_path())
        {
            Ok(fd) => {
                self.mapping_fd = fd.as_raw_fd();
            }
            Err(_) => panic!("Open temp file failed!"),
        };
        unsafe {
            assert!(libc::lseek(self.mapping_fd, len as i64, 0) != -1);
            assert!(libc::write(self.mapping_fd, "".as_ptr() as *const c_void, 1) != -1);
        }

        match MemoryMap::new(
            len,
            &[
                MapOption::MapWritable,
                MapOption::MapFd(self.mapping_fd),
                MapOption::MapOffset(0),
                MapOption::MapNonStandardFlags(0x01),
            ],
        ) {
            Ok(map) => {
                assert_eq!(map.len(), KEY_SIZE + VALUE_SIZE);
                self.mapping = Some(map);
            }
            Err(_) => panic!("Mapping failed!"),
        }
    }

    pub fn unmap(&mut self) {
        assert!(self.mapping.is_some());

        self.mapping = None;
        unsafe {
            libc::close(self.mapping_fd);
        }
        self.mapping_fd = -1;
    }

    pub fn get(&self, key: &KeyT) -> Option<ValueT> {
        if *key < self.fence_pointers[0] || *key > self.max_key {
            return None;
        }

        None
    }

    pub fn range(&self, start: &KeyT, end: &KeyT) -> Vec<EntryT> {
        unimplemented!()
    }

    pub fn put(&mut self, key: KeyT, value: ValueT) -> bool {
        unimplemented!()
    }

    fn file_size(&self) -> u64 {
        self.max_size * size_of::<EntryT>() as u64
    }
}
