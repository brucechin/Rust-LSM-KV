//This is helper function for this project
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Read, SeekFrom};

//one component -> one file on disk
pub fn get_files_name(
    name: &String,
    component_id: &String,
    component_type: &str,
    filename_size: usize,
) -> String {
    let mut filename = String::with_capacity(filename_size + 8);
    // let mut filename = String::new();
    filename.push_str(name);
    filename.push_str("/");
    filename.push_str(component_type);
    filename.push_str(&component_id);

    filename
}

//one key/value a line
pub fn load_file_to_vec(resvec: &mut Vec<Vec<u8>>, fname: &String, one_size: usize, ne: usize) {
    println!("load filename is {}", fname);
    let f = match OpenOptions::new().read(true).open(fname) {
        Err(why) => panic!("could not open due to  {}", why),
        Ok(fkeys) => fkeys,
    };

    let file = BufReader::new(&f);

    let mut rawvec: Vec<u8> = Vec::with_capacity(one_size * ne);
    //take unit is bytes
    match file.take((one_size * ne) as u64).read_to_end(&mut rawvec) {
        Err(why) => panic!("cannot read from file due to {}", why),
        Ok(readsize) => println!("Succefully read {} bytes", readsize),
    };
    for i in 0..ne {
        resvec.push(rawvec[i * one_size..(i * one_size + one_size)].to_vec());
        //println!("i is {}",i );
    }
}

pub fn flush_vec_to_file(u8vec: &mut Vec<Vec<u8>>, fname: &String) {
    //using scope to control file open range
    //Reading files
    let f = match OpenOptions::new().append(true).open(&fname) {
        Err(why) => panic!("could not open due to  {}", why),
        Ok(f) => f,
    };
    println!("flush filename is {}", fname);
    let mut f = BufWriter::new(f);

    for lvec in u8vec {
        match f.write_all(&lvec) {
            Err(why) => panic!("Could not write due to {}", why),
            Ok(_writtensize) => (),
        }
    }
}

pub fn append_last_n_to_file(u8vec: &mut Vec<Vec<u8>>, fname: &String, n: usize) {
    println!("current u8vec len is {}", u8vec.len());
    assert!(
        n <= u8vec.len(),
        "There is no more than {} element in component",
        n
    );
    let f = match OpenOptions::new().append(true).open(&fname) {
        Err(why) => panic!("could not open due to  {}", why),
        Ok(f) => f,
    };
    let mut f = BufWriter::new(f);
    for i in (u8vec.len() - n)..u8vec.len() {
        match f.write_all(&u8vec[i]) {
            Err(why) => panic!("Could not write due to {}", why),
            Ok(_writtensize) => (),
        }
    }
}

//read a vec<u8> of len item_size from fname file
pub fn read_from_index(fname: &String, index: usize, item_size: usize) -> Vec<u8> {
    let f = match OpenOptions::new().read(true).open(&fname) {
        Err(why) => panic!("could not open due to  {}", why),
        Ok(f) => f,
    };
    let mut file = BufReader::new(f);
    let mut resvec: Vec<u8> = Vec::with_capacity(item_size);

    match file.seek(SeekFrom::Start((index * item_size) as u64)) {
        Err(why) => panic!("cannot seek from file due to {}", why),
        Ok(currentpos) => (),
    }

    //take unit is bytes
    match file.take((item_size) as u64).read_to_end(&mut resvec) {
        Err(why) => panic!("cannot read from file due to {}", why),
        Ok(readsize) => println!("Succefully read {} bytes", readsize),
    };
    resvec
}
