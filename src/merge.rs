use crate::data_type::Entry;
use priority_queue::PriorityQueue;

struct MergeEntry {
    precedence: i32,
    entries: Vec<Entry>,
    num_entries: i32,
    current_index: i32,
}

impl MergeEntry {
    pub fn new() {unimplemented!()}

    pub fn head() -> Entry {unimplemented!()}

    pub fn done() -> bool {unimplemented!()}

    //TODO overload comparator for MergeEntry for sorting
}

struct MergeContext {
    priority_queue: PriorityQueue<i32,i32>,//TODO this need to be changed.
}

impl MergeContext {
    pub fn new() {unimplemented!()}

    pub fn add(entry: Entry, num_entries: i32) {unimplemented!()}

    pub fn next() -> Entry {unimplemented!()}

    pub fn done() -> bool {unimplemented!()}
}
