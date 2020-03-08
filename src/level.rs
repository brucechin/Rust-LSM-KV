use crate::run;
use core::fmt::Alignment::Left;
use std::collections::VecDeque;

pub struct Level {
    pub runs: VecDeque<run::Run>,
    pub max_runs: usize,
    pub max_run_size: usize,
}

impl Level {
    pub fn new() -> Level {
        Level {
            runs: VecDeque::new(),
            max_runs: 10,
            max_run_size: 1000,
        }
    }

    pub fn remaining(&self) -> usize {
        self.max_runs - self.runs.len()
    }
}

// pub fn access_iter(cur: &Level) {
//     print!("{}", cur.max_runs);
// }
//
// #[test]
// fn test_iter() {
//     let mut tmp: Vec<Level> = Vec::new();
//     for i in 1..10 {
//         tmp.push(Level::new());
//     }
//     for i in tmp.iter() {
//         access_iter(i);
//     }
// }
