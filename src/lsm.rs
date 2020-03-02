use crate::buffer;
use crate::level;
use crate::run;
use threadpool::ThreadPool;

pub struct LSMTree {
    //TODO need threadpool, multiple-levels, in-memory buffer
    levels: Vec<level::Level>,
    buffer: buffer::Buffer,
    workerpool: threadpool::ThreadPool,
}

impl LSMTree {
    fn get_run(run_id: usize) -> run::Run {}

    //compact level i data to level i+1
    fn merge_down() {}

    pub fn put(&mut self, entry: run::Entry) {}

    pub fn get(&self, key: Vec<u8>) {
        //read from buffer first. then from level 0 to max_level. return first match entry.
    }

    pub fn range(&self, key1: Vec<u8>, key2: Vec<u8>) {}

    pub fn del(&mut self, key: Vec<u8>) {}

    //load lsm tree from disk file
    //    pub fn load(&mut self, filename : &str){
    //
    //    }
}
