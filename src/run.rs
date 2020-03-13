use crate::data_type::{EntryT, KeyT, ValueT, KEY_SIZE, VALUE_SIZE};
use libc;
use mktemp::Temp;
use mmap::{MapOption, MemoryMap};
use page_size;
use std::cmp::max;
//use std::collections::linked_list::Iter;
use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::os::raw;
use std::os::raw::c_void;
use std::os::unix::prelude::*;
use std::path::PathBuf;

pub struct Run {
    pub bloom_filter: bloomfilter::Bloom<KeyT>,
    //bloom_filer: bloom_filter::BloomFilter,
    pub fence_pointers: Vec<KeyT>,
    pub max_key: KeyT,
    pub mapping: Option<MemoryMap>,
    pub mapping_fd: raw::c_int,
    pub size: u64,
    pub max_size: u64,
    pub tmp_file: PathBuf,
    pub level_index: usize,
}

impl Run {
    // pub fn new(max_size: u64, bf_bits_per_entry: f32) -> Run {
    //     Run {
    //         bloom_filter: bloomfilter::Bloom::new(
    //             (bf_bits_per_entry * max_size as f32) as usize,
    //             max_size as usize,
    //         ),
    //         //bloom_filer: bloom_filter::BloomFilter::new_with_size(max_size * bf_bits_per_entry),
    //         fence_pointers: Vec::with_capacity((max_size / page_size::get() as u64) as usize),
    //         max_key: KeyT::default(),
    //         mapping: None,
    //         mapping_fd: -1,
    //         size: 0,
    //         max_size: max_size,
    //         tmp_file: Temp::new_file_in("/tmp/").unwrap().as_ref().to_owned(),
    //     }
    // }

    pub fn new(
        max_size: u64,
        bf_bits_per_entry: f32,
        lsm_name: &str,
        level: usize,
        id: usize,
    ) -> Run {
        Run {
            bloom_filter: bloomfilter::Bloom::new(
                (bf_bits_per_entry * max_size as f32) as usize,
                max_size as usize,
            ),
            //bloom_filer: bloom_filter::BloomFilter::new_with_size(max_size * bf_bits_per_entry),
            fence_pointers: Vec::with_capacity((max_size / page_size::get() as u64) as usize),
            max_key: KeyT::default(),
            mapping: None,
            mapping_fd: -1,
            size: 0,
            max_size: max_size,
            level_index: level,
            tmp_file: PathBuf::from(format!(r"/tmp/{}/{}/run_file-{}.txt", lsm_name, level, id)),
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
            Err(e) => panic!("Open temp file failed because {}!", e),
        };

        let len = size_of::<EntryT>() * self.max_size as usize;

        unsafe {
            assert!(libc::lseek(self.mapping_fd, (len - 1) as i64, 0) != -1);
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
                self.mapping = Some(map);
            }
            Err(e) => panic!("Mapping failed because {}", e),
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

    pub fn get(&mut self, key: &KeyT) -> Option<ValueT> {
        //bloom_filter
        if self.bloom_filter.check(key) {
            //it is very likely that this Run contains target entry. False positives may occur.
            if *key < self.fence_pointers[0] || *key > self.max_key {
                return None;
            }
            let next_page = self.fence_pointers.binary_search(key).unwrap();
            assert!(next_page >= 1);
            let page_index = next_page - 1;

            self.map_read(page_size::get(), page_index * page_size::get());

            let mut val: ValueT = vec![];
            unsafe {
                for i in 0..page_size::get() / size_of::<EntryT>() {
                    if std::slice::from_raw_parts(
                        self.mapping
                            .as_ref()
                            .unwrap()
                            .data()
                            .add(size_of::<EntryT>() * i as usize),
                        KEY_SIZE,
                    )
                    .to_vec()
                        == *key
                    {
                        val = std::slice::from_raw_parts(
                            self.mapping
                                .as_ref()
                                .unwrap()
                                .data()
                                .add(size_of::<EntryT>() * i as usize + KEY_SIZE),
                            VALUE_SIZE,
                        )
                        .to_vec()
                    }
                }
            }

            self.unmap();

            Some(val)
        } else {
            //not in this run according to bloom filter
            println!("not in this Run according to bloom filter");
            None
        }
    }

    pub fn range(&mut self, start: &KeyT, end: &KeyT) -> Vec<EntryT> {
        let mut res: Vec<EntryT> = Vec::new();

        let page_start: usize;
        let page_end: usize;

        if *start > self.max_key || self.fence_pointers[0] > *end {
            return res;
        }

        if *start < self.fence_pointers[0] {
            page_start = 0;
        } else {
            page_start = self.fence_pointers.binary_search(start).unwrap() - 1;
        }

        if *end > self.max_key {
            page_end = 0;
        } else {
            page_end = self.fence_pointers.binary_search(end).unwrap() - 1;
        }

        assert!(page_start < page_end);
        let num_pages = page_end - page_start;
        self.map_read(num_pages * page_size::get(), page_start * page_size::get());

        let num_entries = num_pages * page_size::get() / size_of::<EntryT>();
        res.reserve(num_entries);

        unsafe {
            for i in 0..num_entries {
                let entry = EntryT {
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
                };
                if *start <= entry.key && entry.key <= *end {
                    res.push(entry);
                }
            }
        }

        self.unmap();

        res
    }

    pub fn put(&mut self, entry: &EntryT) {
        assert!(self.size < self.max_size);

        if self.size % page_size::get() as u64 == 0 {
            self.fence_pointers.push(entry.key.clone());
        }

        self.max_key = max(entry.key.clone(), self.max_key.clone());

        let mut entry_data: Vec<u8> = Vec::new();
        entry_data.extend(entry.key.iter());
        entry_data.extend(entry.value.iter());

        unsafe {
            for byte in entry_data {
                std::ptr::write(
                    self.mapping
                        .as_ref()
                        .unwrap()
                        .data()
                        .add(size_of::<EntryT>() * self.size as usize),
                    byte,
                );
            }
        }
        //set true for this key in this Run. For later more efficient search and avoid unnecessary file I/O operations.
        self.bloom_filter.set(&entry.key);
    }

    // fn file_size(&self) -> u64 {
    //     self.max_size * size_of::<EntryT>() as u64
    // }
}
