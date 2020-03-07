use crate::run;
use std::collections::VecDeque;

pub struct Level<'a> {
    pub runs: VecDeque<run::Run<'a>>,
    pub max_runs: usize,
    //TODO do we need max_run_size variable in cs265-lsm code?
}

impl<'a> Level<'a> {
    pub fn new() {}

    pub fn remaining(&self) -> usize {
        self.max_runs - self.runs.len()
    }
}
