use crate::buffer;
use crate::data_type::{EntryT, ValueT, ENTRY_SIZE, TOMBSTONE};
use crate::level;
use crate::merge;
use crate::run;
use rand::Rng;
use std::{io, thread};
//use bit_vec::Iter;
//use rand::distributions::weighted::WeightedError::TooMany;
//use std::borrow::Borrow;
use std::collections::HashMap;
//use std::ptr::null;
//use std::sync::{Arc, Mutex};
use crate::data_type;
use std::cmp::max;
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::{fs, str};

pub static DEFAULT_TREE_DEPTH: u64 = 5;
pub static DEFAULT_TREE_FANOUT: u64 = 10;
pub static DEFAULT_BUFFER_NUM_PAGES: u64 = 1000;
pub static DEFAULT_THREAD_COUNT: u64 = 4;
pub static DEFAULT_BF_BITS_PER_ENTRY: f32 = 0.5;
pub static DEFAULT_TREE_NAME: &str = "rust";

pub struct LSMTree {
    levels: Vec<level::Level>,
    buffer: buffer::Buffer,
    worker_pool: threadpool::ThreadPool,
    bf_bits_per_entry: f32,
    //used for bloom filter initialization
    depth: u64,
    tree_name: String,
}

impl LSMTree {
    /// Returns a LSM tree based key value store
    ///
    /// # Arguments
    ///
    /// * `buf_max_entries` - Max number of entries in memory buffer
    /// * `dep` - depth of LSM tree
    /// * `fanout` - A factor that determines how to scale Run size for deeper levels
    /// * `bf_bits_per_entry` - Used for bloom filter size initialization
    /// * `num_threads` - Used for thread pool initialization
    ///
    /// # Example
    ///
    /// ```
    ///
    /// use lsm_kv::lsm;
    /// let mut lsm = lsm::LSMTree::new(100, 5, 10, 0.5, 4, "doc_test".to_string());
    /// lsm.put("hello", "world");
    /// lsm.put("facebook", "google");
    /// lsm.put("amazon", "linkedin");
    /// assert_eq!(lsm.get("hello"), Some("world".to_string()));
    /// assert_eq!(lsm.get("facebook"), Some("google".to_string()));
    /// lsm.del("hello");
    /// assert_eq!(lsm.get("hello"), None);
    /// lsm.range("amazon", "facebook");
    /// lsm.close();
    /// let mut lsm2 = lsm::LSMTree::new(100, 5, 10, 0.5, 4, "doc_test".to_string());
    /// lsm2.load();
    /// assert_eq!(lsm2.get("hello"), None);
    /// assert_eq!(lsm2.get("facebook"), Some("google".to_string()));
    ///
    /// ```
    //TODO after loading the read will fail due to bloom filter problem
    pub fn new(
        buf_max_entries: u64,
        dep: u64,
        fanout: u64,
        bf_bits_per_entry: f32,
        num_threads: u64,
        tree_name: String,
    ) -> LSMTree {
        let mut max_run_size = buf_max_entries;
        let mut tmp_levels: Vec<level::Level> = Vec::new();
        let mut tmp_deps = dep;
        //create a directory for store files on disk
        fs::create_dir(format!("/tmp/{}/", tree_name));
        while tmp_deps > 0 {
            //level id starts from 0 to depth-1
            fs::create_dir(format!(
                "/tmp/{}/{}/",
                tree_name,
                tmp_levels.len().to_string()
            ));
            tmp_levels.push(level::Level::new(fanout as usize, max_run_size as usize));
            //create a subdir for corresponding level
            max_run_size *= fanout;
            tmp_deps -= 1;
        }

        LSMTree {
            levels: tmp_levels,
            depth: dep,
            bf_bits_per_entry: bf_bits_per_entry,
            worker_pool: threadpool::ThreadPool::new(num_threads as usize),
            buffer: buffer::Buffer::new(buf_max_entries as usize),
            tree_name: tree_name,
        }
    }

    pub fn get_run(&mut self, mut run_id: usize) -> Option<&mut run::Run> {
        for level in &mut self.levels {
            //println!("level len : {}", level.runs.len());
            if run_id < level.runs.len() {
                //println!("get run {}", run_id);
                return level.runs.get_mut(run_id);
            } else {
                run_id -= level.runs.len();
            }
        }
        None
    }

    pub fn num_runs(&self) -> usize {
        let mut res: usize = 0;
        for level in self.levels.iter() {
            res += level.runs.len();
        }
        res
    }

    //compact level i data to level i+1
    fn merge_down(&mut self, current: usize) {
        let mut merge_ctx: merge::MergeContextT = merge::MergeContextT::new();
        let mut entry: EntryT;
        let next: usize;
        //assert!(current >= self.levels.iter());
        if self.levels[current].remaining() > 0 {
            //no need for compaction and merge down
            return;
        } else if current == self.levels.len() - 1 {
            //can not merge down anymore
            println!("No more space in tree");
            return;
        } else {
            next = current + 1;
        }

        /*
         * If the next level does not have space for the current level,
         * recursively merge the next level downwards to create some
         */
        if self.levels[next].remaining() == 0 {
            self.merge_down(next);
            //ensure that after merge down, level next has free space now.
            assert!(self.levels[next].remaining() > 0)
        }

        /*
         * Merge all runs in the current level into the first
         * run in the next level
         */
        for run in self.levels[current].runs.iter_mut() {
            //add all entries in current levels for merging
            merge_ctx.add(run.map_read_default(), run.size as usize);
        }
        let size = self.levels[next].max_run_size as u64;
        let id = self.levels[next].runs.len();
        self.levels[next].runs.push_front(run::Run::new(
            size,
            self.bf_bits_per_entry,
            &self.tree_name,
            next,
            id,
        ));
        //start writing back this compacted run in next level to a new file on disk
        self.levels[next].runs[0].map_write();
        //merge_ctx.print();
        while !merge_ctx.done() {
            entry = merge_ctx.next();
            //TODO merge_ctx.next() return a wrong value.
            //println!("{}", str::from_utf8(&entry.value).unwrap());
            if !(next == self.levels.len() - 1 && entry.value == TOMBSTONE.as_bytes().to_vec()) {
                self.levels[next].runs[0].put(&entry);
            }
        }
        self.levels[next].runs[0].unmap();
        //finish writing back for compacted run

        //unmap the old runs and clear these files
        for run in self.levels[current].runs.iter_mut() {
            run.unmap();
        }
        self.levels[current].runs.clear();
    }

    fn fill_str_with_witespace(&self, input: &str, length: usize) -> Vec<u8> {
        let mut res = vec![' ' as u8; length - input.len()];
        let res2: Vec<u8> = input.as_bytes().to_vec();
        res.extend(res2);
        assert_eq!(res.len(), length);
        res
    }

    fn vec_u8_to_str(&self, input: &Vec<u8>) -> String {
        let res: String = str::from_utf8(input).unwrap().trim().to_owned();
        res
    }

    pub fn put(&mut self, key_str: &str, value_str: &str) -> bool {
        let key = self.fill_str_with_witespace(key_str, data_type::KEY_SIZE);
        let value = self.fill_str_with_witespace(value_str, data_type::VALUE_SIZE);
        if self.buffer.full() == false {
            //put to buffer success
            self.buffer.put(key, value);
            return true;
        } else {
            /*
             * If the buffer is full, flush level 0 if necessary
             * to create space
             */
            //println!("start merge");
            self.merge_down(0);

            /*
             * Flush the buffer to level 0.
             */
            let size = self.levels[0].max_run_size as u64;
            let id = self.levels[0].runs.len();
            self.levels[0].runs.push_front(run::Run::new(
                size,
                self.bf_bits_per_entry,
                &self.tree_name,
                0,
                id,
            ));
            self.levels[0].runs[0].map_write();

            for entry_in_buf in self.buffer.entries.iter() {
                self.levels[0].runs[0].put(&entry_in_buf);
            }
            self.levels[0].runs[0].unmap();

            //buffer already written to levels.front().runs.front(). We can clear it now for inserting new entry.
            self.buffer.empty();
            self.buffer.put(key, value);
            true
        }
    }

    pub fn get(&mut self, key_str: &str) -> Option<String> {
        let key = self.fill_str_with_witespace(key_str, data_type::KEY_SIZE);
        let res: String;
        //read from buffer first. then from level 0 to max_level. return first match entry.
        let mut latest_val: ValueT = ValueT::new();
        let mut latest_run: i32 = -1;
        //multi threading searching on multiple Runs is not available for now
        match self.buffer.get(&key) {
            Some(v) => {
                //found in buffer, return the result;
                res = self.vec_u8_to_str(&v);
                if res != TOMBSTONE.to_string() {
                    return Some(res);
                } else {
                    return None;
                }
            }
            _ => {
                //not found in buffer, start searching in vector<Level>
                //println!("key {} not found in buffer", str::from_utf8(&key).unwrap());
                for current_run in 0..self.num_runs() as u64 {
                    let current_val: ValueT;
                    // println!(
                    //     "search in Run {}, latest_run is {}",
                    //     current_run, latest_run
                    // );
                    //let mut run: run::Run;
                    if latest_run >= 0 || (self.get_run(current_run as usize).is_none()) {
                        // Stop search if we discovered a key in another run, or
                        // if there are no more runs to search
                        break;
                    } else {
                        let run = self.get_run(current_run as usize).unwrap();
                        if run.get(&key).is_none() {
                            // Couldn't find the key in the current run, so we need
                            // to keep searching.
                            //search();
                            // println!(
                            //     "key {} not found in Run {}",
                            //     str::from_utf8(&key).unwrap(),
                            //     current_run
                            // );
                        } else {
                            // Update val if the run is more recent than the
                            // last, then stop searching since there's no need
                            // to search later runs.
                            current_val = run.get(&key).unwrap();
                            if latest_run < 0 || current_run < latest_run as u64 {
                                latest_run = current_run as i32;
                                latest_val = current_val;
                            }
                            break; //find the newest entry and break the for loop.
                        }
                    }
                }

                if latest_run >= 0 {
                    res = self.vec_u8_to_str(&latest_val);
                    if res != TOMBSTONE.to_string() {
                        return Some(res);
                    }
                }
            }
        }
        None
    }

    pub fn range(&mut self, start_str: &str, end_str: &str) -> Vec<String> {
        let start = self.fill_str_with_witespace(start_str, data_type::KEY_SIZE);
        let end = self.fill_str_with_witespace(end_str, data_type::KEY_SIZE);
        let mut buffer_range: Vec<String> = Vec::new(); //this is return value list
        if end < start {
            //invalid input
            return buffer_range;
        }
        let mut ranges: HashMap<usize, Vec<EntryT>> = HashMap::new(); //record candidates in each level.
        let mut merge_ctx = merge::MergeContextT::new();
        let mut entry: EntryT;
        //search in buffer and record result
        ranges.insert(0, self.buffer.range(&start, &end));

        for current_run in 0..self.depth {
            match self.get_run(current_run as usize) {
                Some(r) => {
                    //start and end are used multiple times which causes "use of moved value"
                    ranges.insert((current_run + 1) as usize, r.range(&start, &end));
                }
                _ => {}
            }
        }

        for kv in ranges.iter() {
            //TODO is to_vec() a good option????
            merge_ctx.add(kv.1.to_vec(), kv.1.len());
        }
        while !merge_ctx.done() {
            entry = merge_ctx.next();
            let res = self.vec_u8_to_str(&entry.value);
            if res != TOMBSTONE.to_string() {
                buffer_range.push(res);
            }
        }

        buffer_range
    }

    pub fn del(&mut self, key_str: &str) {
        self.put(key_str, TOMBSTONE);
    }

    pub fn load(&mut self) -> io::Result<()> {
        //TODO iterate through every level subdir in the directory "/tmp/tree_name/" and load Runs
        for depth in 0..self.levels.len() {
            let level_dir_str = format!("/tmp/{}/{}/", self.tree_name, depth).to_string();
            let level_dir: &Path = Path::new(&level_dir_str);
            let max_size = self.levels[depth].max_run_size;
            //visit every run and load into LSMTree vec<Level>
            if level_dir.is_dir() {
                let files = fs::read_dir(level_dir)?;
                let mut entries: Vec<PathBuf> = files
                    .filter(Result::is_ok)
                    .map(|e| e.unwrap().path())
                    .collect();
                //make sure we read from Run-0.text to Run-max_runs.txt
                entries.sort();
                for run_file in entries {
                    let run_file_entry = run_file;
                    //println!("cur file path is {:?}", run_file_entry);
                    let mut cur_run = run::Run::from(
                        max_size as u64,
                        self.bf_bits_per_entry,
                        depth,
                        run_file_entry,
                    );
                    //reconstruct the bloom filter for cur_run. information like max_key, fence_pointers could be stored for easier reconstructions
                    let mut counter = 0;
                    for key in cur_run.get_keys().iter() {
                        //println!("{} set", str::from_utf8(key).unwrap());
                        if counter % (page_size::get() / ENTRY_SIZE) as u64 == 0 {
                            cur_run.fence_pointers.push(key.clone());
                        }
                        cur_run.bloom_filter.set(key);
                        counter += 1;
                        cur_run.max_key = max(key.clone(), cur_run.max_key.clone());
                    }
                    cur_run.size = counter;
                    self.levels[depth].runs.push_back(cur_run);
                }
            }
            //println!("cur level has {} Runs", self.levels[depth].runs.len());
        }

        Ok(())
    }

    pub fn clear(&mut self) {
        //remove all files and clear all Runs in self.levels
        if let Ok(dir) = read_dir(format!("/tmp/{}/", self.tree_name)) {
            for entry in dir {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if entry.path().is_dir() {
                        fs::remove_dir_all(path);
                    } else {
                        fs::remove_file(path);
                    }
                }
            }
            self.levels.clear();
            self.buffer.empty();
        }
    }

    pub fn close(&mut self) {
        //save the buffer as a Run in level 0 even if it is not full.
        self.merge_down(0);

        /*
         * Flush the buffer to level 0.
         */
        let size = self.levels[0].max_run_size as u64;
        let id = self.levels[0].runs.len();
        self.levels[0].runs.push_front(run::Run::new(
            size,
            self.bf_bits_per_entry,
            &self.tree_name,
            0,
            id,
        ));
        self.levels[0].runs[0].map_write();

        for entry_in_buf in self.buffer.entries.iter() {
            self.levels[0].runs[0].put(&entry_in_buf);
        }
        self.levels[0].runs[0].unmap();

        //buffer already written to levels.front().runs.front(). We can clear it now for inserting new entry.
        self.buffer.empty();
    }
}

#[test]
fn test_close_load() {
    let test_size = 1000;
    let mut lsm = LSMTree::new(8, 5, 8, 0.5, 4, "close_load_test".to_string());
    for i in 0..test_size {
        lsm.put(&i.to_string(), &i.to_string());
    }
    for j in 0..test_size {
        assert_eq!(Some(j.to_string()), lsm.get(&j.to_string()));
    }
    lsm.close();
    println!("close done");
    let mut lsm2 = LSMTree::new(8, 5, 8, 0.5, 4, "close_load_test".to_string());
    lsm2.load();
    println!("load done");
    for j in 0..test_size {
        assert_eq!(Some(j.to_string()), lsm2.get(&j.to_string()));
    }
}

#[test]
fn test_range() {
    let mut lsm = LSMTree::new(100, 5, 10, 0.5, 4, "hello".to_string());
    lsm.put("hello", "world");
    lsm.put("facebook", "google");
    lsm.put("amazon", "linkedin");
    assert_eq!(vec!["linkedin", "google"], lsm.range("amazon", "facebook"));
}

#[test]
fn test_clear() {
    let test_size = 1000;
    let mut lsm = LSMTree::new(8, 5, 8, 0.5, 4, "clear_test".to_string());
    for i in 0..test_size {
        lsm.put(&i.to_string(), &i.to_string());
    }
    lsm.clear();
    for j in 0..test_size {
        assert_eq!(None, lsm.get(&j.to_string()));
    }
}

// #[test]
// fn test_multithreading() {
//     let num_threads = 10;
//     let test_size = 1000;
//     let mut lsm = LSMTree::new(8, 5, 8, 0.5, 4, "clear_test".to_string());
//     for i in 0..test_size {
//         lsm.put(&i.to_string(), &i.to_string());
//     }
//
//         let handle = thread:spawn(|| {
//             for i in 0..10 {
//                 assert_eq!(Some(i.to_string()), lsm.get(&i.to_string()));
//             }
//         });
//         handle.join().unwrap();
//
//
// }
