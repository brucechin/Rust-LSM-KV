use crate::data_type::{EntryT, KeyT, ValueT, KEY_SIZE, VALUE_SIZE};
use libc;
use memmap::{Mmap, MmapMut, MmapOptions};
use mktemp::Temp;
use mmap::{MapOption, MemoryMap};
use page_size;
use std::cmp::max;
use std::io::prelude::*;
use std::ops::DerefMut;
//use std::collections::linked_list::Iter;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::os::raw;
use std::os::raw::c_void;
use std::os::unix::prelude::*;
use std::path::PathBuf;
use std::str;

pub struct Run {
    pub bloom_filter: bloomfilter::Bloom<KeyT>,
    //bloom_filer: bloom_filter::BloomFilter,
    pub fence_pointers: Vec<KeyT>,
    pub max_key: KeyT,
    pub mapping: Option<MmapMut>,
    pub mapping_file: Option<File>,
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
            mapping_file: None,
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

        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.tmp_file.as_path())
        {
            Ok(file) => {
                self.mapping_file = Some(file);
            }
            Err(_) => panic!("Open temp file failed!"),
        };
        unsafe {
            match MmapOptions::new()
                .len(len)
                .offset(offset as u64)
                .map(self.mapping_file.as_ref().unwrap())
            {
                Ok(mmap) => {
                    self.mapping = Some(mmap.make_mut().unwrap());
                    let mut res: Vec<EntryT> = Vec::new();
                    for i in 0..len / size_of::<EntryT>() {
                        let offset = size_of::<EntryT>() * i as usize;
                        res.push(EntryT {
                            key: self.mapping.as_ref().unwrap().as_ref()[offset..offset + KEY_SIZE]
                                .to_vec(),
                            value: self.mapping.as_ref().unwrap().as_ref()
                                [offset + KEY_SIZE..offset + KEY_SIZE + VALUE_SIZE]
                                .to_vec(),
                        })
                    }
                    res
                }
                Err(_) => panic!("Mapping failed!"),
            }
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
            Ok(file) => {
                self.mapping_file = Some(file);
            }
            Err(e) => panic!("Open temp file failed because {}!", e),
        };

        let len = size_of::<EntryT>() * self.max_size as usize;

        let mut fill: Vec<u8> = vec![32; len as usize];
        fill.push(10);
        self.mapping_file.as_ref().unwrap().write_all(fill.as_ref());

        unsafe {
            match Mmap::map(self.mapping_file.as_ref().unwrap()) {
                Ok(mmap) => {
                    self.mapping = Some(mmap.make_mut().unwrap());
                }
                Err(e) => panic!("Mapping failed because {}", e),
            }
        }
    }

    pub fn unmap(&mut self) {
        assert!(self.mapping.is_some());

        self.mapping = None;
        self.mapping_file = None;
    }

    pub fn get(&mut self, key: &KeyT) -> Option<ValueT> {
        //bloom_filter
        if self.bloom_filter.check(key) {
            //it is very likely that this Run contains target entry. False positives may occur.
            if *key < self.fence_pointers[0] || *key > self.max_key {
                return None;
            }
            let page_index: usize;
            match self.fence_pointers.binary_search(key) {
                Ok(find) => {
                    page_index = find;
                }
                Err(not) => {
                    page_index = not - 1;
                }
            }
            assert!(page_index >= 0);

            self.map_read(page_size::get(), page_index * page_size::get());

            let mut val: ValueT = vec![];
            for i in 0..page_size::get() / size_of::<EntryT>() {
                let offset = size_of::<EntryT>() * i as usize;
                if self.mapping.as_ref().unwrap().as_ref()[offset..offset + KEY_SIZE].to_vec()
                    == *key
                {
                    val = self.mapping.as_ref().unwrap().as_ref()
                        [offset + KEY_SIZE..offset + KEY_SIZE + VALUE_SIZE]
                        .to_vec();
                }
            }

            self.unmap();

            Some(val)
        } else {
            //not in this run according to bloom filter
            //println!("not in this Run according to bloom filter");
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
            match self.fence_pointers.binary_search(start) {
                Ok(find) => {
                    page_start = find;
                }
                Err(not) => {
                    page_start = not - 1;
                }
            }
        }

        if *end > self.max_key {
            page_end = 0;
        } else {
            match self.fence_pointers.binary_search(end) {
                Ok(find) => {
                    page_end = find;
                }
                Err(not) => {
                    page_end = not - 1;
                }
            }
        }

        assert!(page_start < page_end);
        let num_pages = page_end - page_start;
        self.map_read(num_pages * page_size::get(), page_start * page_size::get());

        let num_entries = num_pages * page_size::get() / size_of::<EntryT>();
        res.reserve(num_entries);

        for i in 0..num_entries {
            let offset = size_of::<EntryT>() * i as usize;
            let entry = EntryT {
                key: self.mapping.as_ref().unwrap().as_ref()[offset..offset + KEY_SIZE].to_vec(),
                value: self.mapping.as_ref().unwrap().as_ref()
                    [offset + KEY_SIZE..offset + KEY_SIZE + VALUE_SIZE]
                    .to_vec(),
            };
            if *start <= entry.key && entry.key <= *end {
                res.push(entry);
            }
        }

        self.unmap();

        res
    }

    pub fn put(&mut self, entry: &EntryT) {
        assert!(self.size < self.max_size);

        if self.size % (page_size::get() / size_of::<EntryT>()) as u64 == 0 {
            self.fence_pointers.push(entry.key.clone());
        }

        self.max_key = max(entry.key.clone(), self.max_key.clone());

        let mut entry_data: Vec<u8> = Vec::new();
        entry_data.extend(entry.key.iter());
        entry_data.extend(entry.value.iter());

        let mut offset = size_of::<EntryT>() * self.size as usize;
        for byte in entry_data {
            self.mapping.as_mut().unwrap().as_mut()[offset] = byte;
            offset += 1;
        }

        //set true for this key in this Run. For later more efficient search and avoid unnecessary file I/O operations.
        self.bloom_filter.set(&entry.key);
        self.size += 1;
    }

    // fn file_size(&self) -> u64 {
    //     self.max_size * size_of::<EntryT>() as u64
    // }
}

#[test]
fn run_test() {
    use crate::run;
    use std::fs;
    fs::create_dir("/tmp/unit_test");
    fs::create_dir("/tmp/unit_test/0");
    let mut run = run::Run::new(10, 0.5, "unit_test", 0, 0);
    let entry1 = EntryT {
        key: vec![97; 8],
        value: vec![98; 32],
    };
    let entry2 = EntryT {
        key: vec![98; 8],
        value: vec![99; 32],
    };
    run.map_write();
    run.put(&entry1);
    run.put(&entry2);
    run.unmap();
    let key1: Vec<u8> = vec![97; 8];
    let key2: Vec<u8> = vec![98; 8];
    println!("{}", std::str::from_utf8(&run.get(&key1).unwrap()).unwrap());
    println!("{}", std::str::from_utf8(&run.get(&key2).unwrap()).unwrap());
}
