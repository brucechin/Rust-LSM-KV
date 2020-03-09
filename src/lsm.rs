use crate::buffer;
use crate::data_type::{EntryT, ValueT, TOMBSTONE};
use crate::level;
use crate::merge;
use crate::run;
//use bit_vec::Iter;
//use rand::distributions::weighted::WeightedError::TooMany;
//use std::borrow::Borrow;
use std::collections::HashMap;
//use std::ptr::null;
//use std::sync::{Arc, Mutex};
use crate::data_type;
use std::str;
pub static DEFAULT_TREE_DEPTH: u64 = 5;
pub static DEFAULT_TREE_FANOUT: u64 = 10;
pub static DEFAULT_BUFFER_NUM_PAGES: u64 = 1000;
pub static DEFAULT_THREAD_COUNT: u64 = 4;
pub static DEFAULT_BF_BITS_PER_ENTRY: f32 = 0.5;

pub struct LSMTree {
    levels: Vec<level::Level>,
    buffer: buffer::Buffer,
    worker_pool: threadpool::ThreadPool,
    bf_bits_per_entry: f32, //used for bloom filter initialization
    depth: u64,
}

impl LSMTree {
    pub fn new(
        buf_max_entries: u64,
        dep: u64,
        fanout: u64,
        bf_bits_per_entry: f32,
        num_threads: u64,
    ) -> LSMTree {
        //TODO implment constructor
        let mut max_run_size = buf_max_entries;
        let mut tmp_levels: Vec<level::Level> = Vec::new();
        let mut tmp_deps = dep;
        while tmp_deps > 0 {
            tmp_levels.push(level::Level::new(fanout as usize, max_run_size as usize));
            max_run_size *= fanout;
            tmp_deps -= 1;
        }
        LSMTree {
            levels: tmp_levels,
            depth: dep,
            bf_bits_per_entry: bf_bits_per_entry,
            worker_pool: threadpool::ThreadPool::new(num_threads as usize),
            buffer: buffer::Buffer::new(buf_max_entries as usize),
        }
    }

    pub fn get_run(&self, run_id: usize) -> Option<run::Run> {
        let mut index = run_id;
        for level in self.levels.iter() {
            if run_id < level.runs.len() {
                level.runs.get(index).unwrap();
            } else {
                index -= level.runs.len();
            }
        }
        None
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
        self.levels[next]
            .runs
            .push_front(run::Run::new(size, self.bf_bits_per_entry));
        //start writing back this compacted run in next level to a new file on disk
        self.levels[next].runs[0].map_write();
        while !merge_ctx.done() {
            entry = merge_ctx.next();
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

            self.merge_down(0);

            /*
             * Flush the buffer to level 0.
             */
            let size = self.levels[0].max_run_size as u64;
            self.levels[0]
                .runs
                .push_front(run::Run::new(size, self.bf_bits_per_entry));
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

    pub fn get(&self, key_str: &str) -> Option<String> {
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

                for current_run in 0..self.depth {
                    let current_val: ValueT;
                    //let mut run: run::Run;
                    if latest_run >= 0 || (self.get_run(current_run as usize).is_none()) {
                        // Stop search if we discovered a key in another run, or
                        // if there are no more runs to search
                        break;
                    } else {
                        let mut run = self.get_run(current_run as usize).unwrap();
                        if run.get(&key).is_none() {
                            // Couldn't find the key in the current run, so we need
                            // to keep searching.
                            //search();
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

                if latest_run >= 0 && latest_val != TOMBSTONE.as_bytes().to_vec() {
                    res = self.vec_u8_to_str(&latest_val);
                    return Some(res);
                }
            }
        }
        None
    }

    pub fn range(&self, start_str: &str, end_str: &str) -> Vec<String> {
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

        //search in runs. TODO search range should be num of Runs
        for current_run in 0..self.depth {
            match self.get_run(current_run as usize) {
                Some(mut r) => {
                    //start and end are used multiple times which causes "use of moved value"
                    ranges.insert((current_run + 1) as usize, r.range(&start, &end));
                }
                _ => {}
            }
        }

        //TODO Merge ranges and return values. because there could be old values in ranges to be eliminated.
        // Only the latest values should be kept

        for kv in ranges.iter() {
            //TODO is to_vec() a good option????
            merge_ctx.add(kv.1.to_vec(), kv.1.len());
        }
        while !merge_ctx.done() {
            entry = merge_ctx.next();
            if entry.value != TOMBSTONE.as_bytes().to_vec() {
                buffer_range.push(self.vec_u8_to_str(&entry.value));
            }
        }

        buffer_range
    }

    pub fn del(&mut self, key_str: &str) {
        self.put(key_str, TOMBSTONE);
    }

    //TODO load lsm tree from disk file

    //    pub fn load(&mut self, filename : &str){
    //
    //    }
}

#[test]
fn test_lsm() {
    println!("hello lsm test");
}
