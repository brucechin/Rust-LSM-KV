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

    pub fn get(&self, key: &KeyT) -> Option<ValueT> {
        let search_entry = EntryT {
            key: key.clone(),
            value: ValueT::default(),
        };
        if let Some(entry) = self.entries.get(&search_entry) {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn range(&self, start: &KeyT, end: &KeyT) -> Vec<EntryT> {
        let lower_bound = EntryT {
            key: start.clone(),
            value: ValueT::default(),
        };
        let upper_bound = EntryT {
            key: end.clone(),
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

#[test]
fn test_size() {
    let mut buf = Buffer::new(10);
    buf.put("helloworld".as_bytes().to_vec(), "worldhello".as_bytes().to_vec());
    buf.put("hello".as_bytes().to_vec(), "world".as_bytes().to_vec());
    assert_eq!(2, buf.entries.len());
    assert_eq!(10, buf.max_size);
    assert!(false == buf.full());
}

#[test]
fn test_put_get() {
    let mut buf = Buffer::new(10);
    for i in 0..10u8{
        buf.put(vec![i], vec![i]);
    }
    for j in 0..10u8{
        assert_eq!(vec![j], buf.get(&vec![j]).unwrap());
    }
}

#[test]
fn test_range() {
    let mut buf = Buffer::new(10);
    for i in 0..10u8{
        buf.put(vec![i], vec![i]);
    }
    for j in 0..5u8{
        assert_eq!(vec![j + 1], buf.range(&vec![1], &vec![5])[j as usize].value);
    }
    //println!("{:?}", buf.range(&vec![1], &vec![5])[0].value);
}