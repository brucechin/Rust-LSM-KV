use crate::data_type::EntryT;
use rand::distributions::Open01;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};

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
            self.precedence.cmp(&other.precedence)
        } else {
            self.head().cmp(&other.head())
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

pub type MergeEntryT = MergeEntry;

struct MergeContext {
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
            let priority = merge_entry.clone();
            self.priority_queue.push(merge_entry);
        }
    }

    pub fn next(&mut self) -> EntryT {
        //TODO priority_queue return both item and its priority
        let mut next = self.priority_queue.pop().unwrap();
        let current = next.clone();

        while !self.priority_queue.is_empty() {
            next.current_index += 1;
            if !next.done() {
                let priority = next.clone();
                self.priority_queue.push(next);
            }
            if self.priority_queue.peek().unwrap().head().key == current.head().key {
                next = self.priority_queue.pop().unwrap();
            } else {
                break;
            }
        }
        current.head()
    }

    pub fn done(&self) -> bool {
        self.priority_queue.is_empty()
    }
}

#[test]
fn test_merge_entry_cmp() {
    println!("hello merge");
}
