use getopts::Options;
use lsm_kv::lsm;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut buffer_num_pages = lsm::DEFAULT_BUFFER_NUM_PAGES;
    let mut depth = lsm::DEFAULT_TREE_DEPTH;
    let mut fanout = lsm::DEFAULT_TREE_FANOUT;
    let mut num_threads = lsm::DEFAULT_THREAD_COUNT;
    let mut bf_bits_per_entry = lsm::DEFAULT_BF_BITS_PER_ENTRY;

    let mut opts = Options::new();
    opts.optopt("b", "", "number of pages in buffer", "PAGE_NUM");
    opts.optopt("d", "", "number of levels", "LEVEL_NUM");
    opts.optopt("f", "", "level fanout", "FANOUT");
    opts.optopt("t", "", "number of threads", "THREADS_NUM");
    opts.optopt("r", "", "bloom filter bits per entry", "BLOOM_BITS");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };
    if matches.opt_str("b").is_some() {
        buffer_num_pages = matches.opt_str("b").unwrap().parse().unwrap()
    }
    if matches.opt_str("d").is_some() {
        depth = matches.opt_str("d").unwrap().parse().unwrap()
    }
    if matches.opt_str("f").is_some() {
        fanout = matches.opt_str("f").unwrap().parse().unwrap()
    }
    if matches.opt_str("t").is_some() {
        num_threads = matches.opt_str("t").unwrap().parse().unwrap()
    }
    if matches.opt_str("r").is_some() {
        bf_bits_per_entry = matches.opt_str("r").unwrap().parse().unwrap()
    }
}
