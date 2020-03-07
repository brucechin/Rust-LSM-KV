use crate::buffer;
use crate::data_type::{Entry, ValueT, TOMBSTONE};
use crate::level;
use crate::run;
use rand::distributions::weighted::WeightedError::TooMany;
use spinlock::Spinlock;
use std::collections::HashMap;
use std::ptr::null;
use threadpool::ThreadPool;

pub struct LSMTree {
    //TODO need threadpool, multiple-levels, in-memory buffer
    levels: Vec<level::Level>,
    buffer: buffer::Buffer,
    workerpool: threadpool::ThreadPool,
    bf_bits_per_entry: u64, //used for bloom filter initialization
}

impl LSMTree {
    pub fn new() {}
    fn get_run(self, run_id: usize) -> Option<run::Run> {
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
    pub fn merge_down() {}

    pub fn put(&mut self, entry: Entry) -> bool {
        //TODO entry must be fixed size for easier put implementation.
        if self.buffer.full() == false {
            //put to buffer success
            self.buffer.put(entry.key, entry.value);
            true
        } else {
            /*
             * If the buffer is full, flush level 0 if necessary
             * to create space
             */
            self.merge_down();

            /*
             * Flush the buffer to level 0.
             */
            self.levels[0].runs.push_front(run::Run::new(
                self.levels[0].max_run_size as u64,
                self.bf_bits_per_entry,
            ));
            self.levels[0].runs.front().unwrap().map_write();

            for entry_in_buf in self.buffer.entries {
                self.levels[0]
                    .runs
                    .front()
                    .unwrap()
                    .put(entry_in_buf.key, entry_in_buf.value);
            }
            self.levels[0].runs.front().unwrap().unmap();

            //buffer already written to levels.front().runs.front(). We can clear it now for inserting new entry.
            self.buffer.empty();
            self.buffer.put(entry.key, entry.value)
        }
    }

    pub fn get(&self, key: Vec<u8>) -> Option<ValueT> {
        //read from buffer first. then from level 0 to max_level. return first match entry.
        let mut latest_val: ValueT = "NULL".collect();
        let mut latest_run: usize;
        let mut counter: usize; //TODO counter should be atomic<usize> according to c++ codebase.
        match self.buffer.get(key) {
            Some(v) => {
                //found in buffer, return the result;
                if v != TOMBSTONE {
                    v
                } else {
                    None
                }
            }
            _ => {
                //not found in buffer, start searching in vector<Level>
                let lock = spinlock::Spinlock::new(0);
                counter = 0;
                latest_run = -1;
                self.workerpool.execute(|| {
                    let mut current_run = counter;
                    counter += 1;
                    let mut current_val: ValueT;
                    let mut run: run::Run;
                    if latest_run >= 0 || (self.get_run(current_run).is_none()) {
                        // Stop search if we discovered a key in another run, or
                        // if there are no more runs to search
                        //TODO how to terminate this task thread here?
                        return;
                    } else {
                        run = self.get_run(current_run).unwrap();
                        if run.get(key).is_none() {
                            // Couldn't find the key in the current run, so we need
                            // to keep searching.
                            //search(); //TODO how to call this task again??? in c++ codebase, the search is task abstraction for threadpool to execute
                        } else {
                            // Update val if the run is more recent than the
                            // last, then stop searching since there's no need
                            // to search later runs.
                            current_val = run.get(key).unwrap();
                            lock.lock();
                            if latest_run < 0 || current_run < latest_run {
                                latest_run = current_run;
                                lastest_val = current_val;
                            }
                            lock.unlock();
                        }
                    }
                });
                self.workerpool.join();

                if latest_run >= 0 && latest_val != TOMBSTONE {
                    latest_val
                }
                None
            }
        }
        None
    }

    pub fn range(&self, start: &Vec<u8>, end: &Vec<u8>) -> Option<Vec<ValueT>> {
        if end <= start {
            None
        }

        let lock = spinlock::Spinlock::new(0);
        let mut counter: usize; //TODO counter should be atomic
        let mut buffer_range: Vec<Entry>;
        let mut ranges: HashMap<int, Vec<Entry>>; //record candidates in each level.

        //search in buffer and record result
        ranges.insert(0, buffer.range(start, end));

        //search in runs
        counter = 0;
        self.workerpool.execute(|| {
            let mut current_run: usize = counter;
            counter += 1;
            match self.get_run(current_run) {
                Some(r) => {
                    lock.lock();
                    //start and end are used multiple times which causes "use of moved value"
                    ranges.insert(current_run + 1, r.range(start, end));
                    lock.unlock();

                    //TODO call this task again.
                    search();
                }
                _ => None,
            }
        });
        self.workerpool.join();

        //TODO Merge ranges and return values. because there could be old values in ranges to be eliminated.
        // Only the latest values should be kept

        None
    }

    pub fn del(&mut self, key: Vec<u8>) {
        let entry = Entry::new(key, TOMBSTONE.clone());
        self.put(entry);
    }

    //load lsm tree from disk file
    //    pub fn load(&mut self, filename : &str){
    //
    //    }
}
