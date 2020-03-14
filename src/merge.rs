use crate::data_type::EntryT;
//use rand::distributions::Open01;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
//use std::hash::{Hash, Hasher};
use std::str;
#[derive(Eq, Debug, Hash, Clone)]
struct MergeEntry {
    pub precedence: usize,
    pub entries: Vec<EntryT>,
    pub num_entries: usize,
    pub current_index: usize,
}

impl MergeEntry {
    pub fn new(ent: Vec<EntryT>, num: usize, pre: usize) -> MergeEntry {
        MergeEntry {
            precedence: pre,
            entries: ent,
            num_entries: num,
            current_index: 0,
        }
    }

    pub fn head(&self) -> EntryT {
        self.entries[self.current_index].clone()
    }

    pub fn done(&self) -> bool {
        self.current_index == self.num_entries
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.head() == other.head() {
            other.precedence.cmp(&self.precedence)
            //self.precedence.cmp(&other.precedence).reverse()
        } else {
            other.head().cmp(&self.head())
            //self.head().cmp(&other.head())
        }
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.precedence == other.precedence
            && self.entries == other.entries
            && self.current_index == other.current_index
            && self.num_entries == other.num_entries
    }
}

type MergeEntryT = MergeEntry;

pub struct MergeContext {
    //priority_queue: PriorityQueue<MergeEntryT, MergeEntryT>,
    priority_queue: BinaryHeap<MergeEntryT>,
}

impl MergeContext {
    pub fn new() -> MergeContext {
        MergeContext {
            priority_queue: BinaryHeap::new(),
        }
    }

    pub fn add(&mut self, entries: Vec<EntryT>, num_entries: usize) {
        if num_entries > 0 {
            let merge_entry: MergeEntry =
                MergeEntry::new(entries, num_entries, self.priority_queue.len());
            // merge_entry.entries = entries;
            // merge_entry.num_entries = num_entries;
            // merge_entry.precedence = self.priority_queue.len();
            self.priority_queue.push(merge_entry);
        }
    }

    pub fn next(&mut self) -> EntryT {
        //TODO priority_queue return both item and its priority
        let mut next: MergeEntryT;
        let current = self.priority_queue.peek().unwrap().clone();
        //println!("{}", str::from_utf8(&current.head().value).unwrap());
        while !self.priority_queue.is_empty()
            && self.priority_queue.peek().unwrap().head().key == current.head().key
        {
            next = self.priority_queue.pop().unwrap();
            next.current_index += 1;
            if !next.done() {
                self.priority_queue.push(next);
            }
        }
        //println!("{}", str::from_utf8(&current.head().value).unwrap());
        current.head()
    }

    pub fn print(&mut self) {
        println!("merge ctx print start");
        for tmp in &self.priority_queue{
            for entry in tmp.entries.iter() {
                println!("{}", str::from_utf8(&entry.value).unwrap());
            }
        }
        //println!("{:?}", str::from_utf8(&self.priority_queue.peek().unwrap().entries[0].value));
    }

    pub fn done(&self) -> bool {
        self.priority_queue.is_empty()
    }
}

pub type MergeContextT = MergeContext;

#[test]
fn test_merge_entry_cmp() {
    println!("hello merge");
}
