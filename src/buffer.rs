use crate::run;
use std::collections::HashSet;

pub struct Buffer {
    pub max_size: usize,
    pub entries: HashSet<run::Entry>,
}

impl Buffer {
    pub fn new(size: usize) -> Buffer {
        //        Buffer{
        //            max_size : size,
        //            entries : HashSet<run::Entry>::new()
        //        }
    }

    pub fn get(key: Vec<u8>) -> Vec<u8> {}

    pub fn range(key1: Vec<u8>, key2: Vec<u8>) -> Vec<Vec<u8>> {}

    pub fn put(entry: run::Entry) -> bool {}
}
