use crate::data_type::{EntryT, KeyT, ValueT};
use std::collections::BTreeSet;
use std::ops::Bound::Included;

pub struct Buffer {
    pub max_size: usize,
    pub entries: BTreeSet<EntryT>,
}

impl Buffer {
    pub fn new(size: usize) -> Buffer {
        Buffer {
            max_size: size,
            entries: BTreeSet::new(),
        }
    }

    pub fn get(&self, key: KeyT) -> Option<ValueT> {
        let search_entry = EntryT {
            key: key,
            value: ValueT::default(),
        };
        if let Some(entry) = self.entries.get(&search_entry) {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn range(&self, start: KeyT, end: KeyT) -> Vec<EntryT> {
        let lower_bound = EntryT {
            key: start,
            value: ValueT::default(),
        };
        let upper_bound = EntryT {
            key: end,
            value: ValueT::default(),
        };
        let mut res: Vec<EntryT> = Vec::new();
        for elem in self
            .entries
            .range((Included(lower_bound), Included(upper_bound)))
        {
            res.push(elem.clone());
        }
        res
    }

    pub fn put(&mut self, key: KeyT, value: ValueT) {
        let entry = EntryT {
            key: key,
            value: value,
        };
        self.entries.replace(entry);
    }

    pub fn empty(&mut self) {
        self.entries.clear();
    }

    pub fn full(&self) -> bool {
        if self.entries.len() == self.max_size {
            true
        } else {
            false
        }
    }
}
