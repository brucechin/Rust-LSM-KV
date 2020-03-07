use crate::data_type::Entry;
use priority_queue::PriorityQueue;

struct MergeEntry {
    precedence: int,
    entries: Vec<Entry>,
    num_entries: long,
    current_index: int,
}

impl MergeEntry {
    pub fn new() {}

    pub fn head() -> Entry {}

    pub fn done() -> bool {}

    //TODO overload comparator for MergeEntry for sorting
}

struct MergeContext {
    priority_queue: PriorityQueue,
}

impl MergeContext {
    pub fn new() {}

    pub fn add(entry: Entry, num_entries: long) {}

    pub fn next() -> Entry {}

    pub fn done() -> bool {}
}
