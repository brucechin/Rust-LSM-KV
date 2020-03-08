use std::cmp::Ordering;
/*
 *  use for components
 */
pub type KeyT = Vec<u8>;
pub type ValueT = Vec<u8>;

pub static KEY_SIZE: usize = 8;
pub static FILENAME_SIZE: usize = 32;
pub static VALUE_SIZE: usize = 32;
pub static TOMBSTONE: ValueT = vec![0, 0];
#[derive(Eq, Default, Debug, Clone)]
pub struct Entry {
    pub key: KeyT,
    pub value: ValueT,
}

impl Entry {
    pub fn new(k: KeyT, val: ValueT) -> Entry {
        Entry { key: k, value: val }
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

pub type EntryT = Entry;

/*
 *  use for bloom
 */
pub static BLOOM_SIZE: u64 = 10000000;
pub static HASHES: u64 = 5;
