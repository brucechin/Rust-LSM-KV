use crate::data_type::Entry;
use priority_queue::PriorityQueue;
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

#[derive(PartialEq, PartialOrd, Eq, Debug, Hash)]
struct MergeEntry {
    pub precedence: usize,
    pub entries: Vec<Entry>,
    pub num_entries: usize,
    pub current_index: usize,
}

impl MergeEntry {
    pub fn new(ent : Vec<Entry>, num :usize, pre : usize) -> MergeEntry {
        MergeEntry {
            precedence: pre,
            entries : ent,
            num_entries: num,
            current_index: 0,
        }
    }

    pub fn head(&self) -> Entry {
        self.entries[self.current_index].clone()
    }

    pub fn done(&self) -> bool {
        self.current_index == self.num_entries
    }

}

impl Ord for MergeEntry{
    fn cmp(&self, other: &MergeEntry) -> Ordering{
        if(self.head() == other.head()){
            self.precedence.cmp(&other.precedence)
        }else{
            self.head().cmp(&other.head())
        }
    }
}


struct MergeContext {
    priority_queue: PriorityQueue<MergeEntry, usize> //TODO this need to be changed.
}

impl MergeContext {
    pub fn new() -> MergeContext{
        MergeContext{
            priority_queue : PriorityQueue::new(),
        }
    }

    pub fn add(&mut self, entries: Vec<Entry>, num_entries: usize) {
        
        if(num_entries > 0){
            let mut merge_entry : MergeEntry = MergeEntry::new(entries, num_entries, self.priority_queue.len());
            // merge_entry.entries = entries;
            // merge_entry.num_entries = num_entries;
            // merge_entry.precedence = self.priority_queue.len();
            //TODO set priority for merge entry.
            self.priority_queue.push(merge_entry, 1);
        }
    }

    pub fn next(&mut self) -> Entry {
        //TODO priority_queue return both item and its priority
        let mut currrent  = self.priority_queue.peek().unwrap().0;
        let mut next = currrent;
        let entry : Entry;

        while(next.head().key == currrent.head().key && !self.priority_queue.is_empty()){
            self.priority_queue.pop();
            next.current_index += 1;
            if(!next.done()){
                self.priority_queue.push(next, 1);
            }
            next = self.priority_queue.peek().unwrap().0;
        }
        currrent.head()

    }

    pub fn done(&self) -> bool {
        self.priority_queue.is_empty()
    }
}



#[test]
fn test_merge_entry_cmp(){
    println!("hello merge");
}