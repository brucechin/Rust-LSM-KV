use getopts::Options;
use lsm_kv::data_type::{EntryT, ENTRY_SIZE};
use lsm_kv::lsm;
use lsm_kv::lsm::LSMTree;
use std::io::BufRead;
use std::{env, io};

fn command_loop(lsm_tree: &mut LSMTree, input: impl BufRead) {
    for line in input.lines() {
        match line {
            Ok(line) => {
                let tokens: Vec<&str> = line.split_whitespace().collect();
                if tokens.is_empty() {
                    continue;
                } else {
                    match tokens[0] {
                        "p" => {
                            lsm_tree.put(tokens[1], tokens[2]);
                            println!("The k-v ({}, {}) has been inserted!", tokens[1], tokens[2]);
                        }
                        "g" => {
                            let val = lsm_tree.get(tokens[1]);
                            if val.is_some() {
                                println!("The value of key {} is {}", tokens[1], val.unwrap());
                            } else {
                                println!("No value with key {} in the DB!", tokens[1]);
                            }
                        }
                        "r" => {
                            let vals = lsm_tree.range(tokens[1], tokens[2]);
                            if vals.is_empty() {
                                println!(
                                    "No value with key between {} and {} in the DB!",
                                    tokens[1], tokens[2]
                                );
                            } else {
                                println!(
                                    "Values with keys between {} and {} are:",
                                    tokens[1], tokens[2]
                                );
                                for val in vals {
                                    print!("{} ", val);
                                }
                                println!();
                            }
                        }
                        "d" => {
                            lsm_tree.del(tokens[1]);
                            println!("The k-v with key {} has been deleted!", tokens[1]);
                        }
                        "l" => {}
                        _ => {
                            println!("Invalid command!");
                        }
                    }
                }
            }
            Err(e) => panic!("error {} during reading input\n", e),
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut buffer_num_pages = lsm::DEFAULT_BUFFER_NUM_PAGES;
    let mut depth = lsm::DEFAULT_TREE_DEPTH;
    let mut fanout = lsm::DEFAULT_TREE_FANOUT;
    let mut num_threads = lsm::DEFAULT_THREAD_COUNT;
    let mut bf_bits_per_entry = lsm::DEFAULT_BF_BITS_PER_ENTRY;
    let mut tree_name = lsm::DEFAULT_TREE_NAME;

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

    let buffer_max_entries = buffer_num_pages * page_size::get() as u64 / ENTRY_SIZE as u64;

    let mut lsm_tree = LSMTree::new(
        buffer_max_entries,
        depth,
        fanout,
        bf_bits_per_entry,
        num_threads,
        tree_name.to_string(),
    );

    command_loop(&mut lsm_tree, io::stdin().lock())
}
