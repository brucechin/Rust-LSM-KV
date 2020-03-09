//extern crate rand;
use crate::data_type;
use bit_vec::BitVec;
//use rand::prelude::*;
use rand::thread_rng;
use rand::Rng;
//use std::ptr::hash;
use bloomfilter::Bloom;

pub type IndexT = u64;

#[test]
fn test_Bloom() {
    let mut bloom: Bloom<Vec<u8>> = Bloom::new(100000, 1000);
    bloom.set(&vec![1, 2, 3, 4]);
    bloom.set(&"asd".as_bytes().to_vec());
    assert!(true, bloom.check(&"asd".as_bytes().to_vec()));
    //assert!(bloom.check("hello"), true);
    //println!("{}", bloom.check("helloo"));
}

#[derive(Debug)]
pub struct BloomFilter {
    hashes: IndexT,
    size: IndexT,
    count: IndexT,
    table: bit_vec::BitVec,
}

impl BloomFilter {
    pub fn new_init() -> BloomFilter {
        BloomFilter {
            hashes: data_type::HASHES,
            size: data_type::BLOOM_SIZE,
            count: 0,
            table: BitVec::from_elem(data_type::BLOOM_SIZE as usize, false),
        }
    }

    pub fn new(hashes: IndexT, size: IndexT, count: IndexT) -> BloomFilter {
        BloomFilter {
            hashes: hashes,
            size: size,
            count: count,
            table: BitVec::from_elem(size as usize, false),
        }
    }

    pub fn new_with_size(size: IndexT) -> BloomFilter {
        BloomFilter {
            hashes: data_type::HASHES,
            size: size,
            count: 0,
            table: BitVec::from_elem(size as usize, false),
        }
    }

    pub fn set_bit(&mut self, i: IndexT) {
        assert!(i < self.size);

        self.table.set(i as usize, true);
    }

    pub fn get_bit(&self, i: IndexT) -> Option<bool> {
        assert!(i < self.size);

        self.table.get(i as usize)
    }

    pub fn hash1(&self, k: IndexT) -> IndexT {
        let mut key = k;
        key = (!key).wrapping_add(key << 21); // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key.wrapping_add(key << 3)).wrapping_add(key << 8); // key * 265
        key = key ^ (key >> 14);
        key = (key.wrapping_add(key << 2)).wrapping_add(key << 4); // key * 21
        key = key ^ (key >> 28);
        key = key.wrapping_add(key << 31);
        key
    }

    pub fn hash2(&self, k: IndexT) -> IndexT {
        let mut key = k;
        key = (key + 0x7ed55d16) + (key << 12);
        key = (key ^ 0xc761c23c) ^ (key >> 19);
        key = (key + 0x165667b1) + (key << 5);
        key = (key + 0xd3a2646c) ^ (key << 9);
        key = (key + 0xfd7046c5) + (key << 3);
        key = (key ^ 0xb55a4f09) ^ (key >> 16);
        key
    }

    pub fn hash3(&self, k: IndexT) -> IndexT {
        let mut key = k;
        key = (key ^ 61) ^ (key >> 16);
        key = key + (key << 3);
        key = key ^ (key >> 4);
        key = key * 0x27d4eb2d;
        key = key ^ (key >> 15);
        key
    }

    pub fn bloom_check(&self, k: IndexT) -> bool {
        for i in 0..self.hashes {
            let hash = (self.hash1(k).wrapping_add(i.wrapping_mul(self.hash3(k)))) % self.size;
            if self.get_bit(hash) == Some(false) {
                return false;
            }
        }
        return true;
    }

    pub fn bloom_add(&mut self, k: IndexT) {
        for i in 0..self.hashes {
            let hash: IndexT =
                (self.hash1(k).wrapping_add(i.wrapping_mul(self.hash3(k)))) % self.size;
            if self.get_bit(hash) == Some(false) {
                self.set_bit(hash);
                self.count = self.count + 1;
            }
        }
    }
}

#[test]
fn test_bloom() {
    let mut b = BloomFilter::new(3, 128, 1);
    b.set_bit(0);
    let temp: Option<bool> = b.get_bit(0);
    let temp1: Option<bool> = b.get_bit(1);
    // print!("{:?}",temp);
    assert!(temp == Some(true));
    assert!(temp1 == Some(false));
}

#[test]
fn test_hash() {
    //esting hash
    let test_keys: [IndexT; 6] = [0, 1, 2, 3, 13, 97];
    let mut b = BloomFilter::new(3, 128, 1);
    b.hash1(1);
    assert_eq!(b.hash1(test_keys[0]), 8633297058295171728);
    assert_eq!(b.hash1(test_keys[5]), 14582706179898628597);
}

#[test]
fn test_bloom_basic() {
    let mut b: BloomFilter = BloomFilter::new(3, 1000, 1);
    for k in 0..71 {
        b.bloom_add(k);
    }
    println!(
        "Test of check: \n key present  {0} \n key absent {1} \n",
        b.bloom_check(1),
        b.bloom_check(71)
    );
}

#[test]
fn test_bloom_occupancy() {
    let size_bits: IndexT = 1000000;
    let mut b: BloomFilter = BloomFilter::new(7, size_bits, 1);
    for k in 1..size_bits {
        b.bloom_add(k);
    }
}

#[test]
fn test_bloom_false_positive() {
    let size_bits: IndexT = 1000000;
    let mut b: BloomFilter = BloomFilter::new(7, size_bits, 1);
    let rand_max: IndexT = 100000000;
    let top: IndexT = 100000;
    let mut test_occurences: IndexT = 0;
    let mut occurences: IndexT = 0;
    let mut rng = thread_rng();
    for _i in 1..(top + 1) {
        let r: IndexT = rng.gen_range(0, rand_max);
        b.bloom_add(r);
        if b.bloom_check(r) {
            test_occurences = test_occurences + 1;
        }
    }
    println!("Test occurences : {0} / 100", test_occurences);

    for _i in 1..(top + 1) {
        let r: IndexT = rng.gen_range(0, rand_max);
        if b.bloom_check(r) {
            occurences = occurences + 1;
        }
    }

    println!(
        "Occupancy: {0} bits false positives: {1}",
        b.count, occurences
    );
}
