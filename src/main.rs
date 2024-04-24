use std::{
    hash::BuildHasherDefault,
    io::{stdout, Write},
    thread::{available_parallelism, ScopedJoinHandle},
};

use dashmap::DashMap;
use rustc_hash::FxHasher;

static INPUT: &str = "/home/arthur/1BRC/data/measurements.txt";

struct Entries<'a> {
    inner: &'a [u8],
}

fn split_on(s: &[u8], c: u8) -> Option<(&[u8], &[u8])> {
    let index = s.iter().position(|&x| x == c)?;
    Some((&s[..index], &s[index + 1..]))
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a [u8], f32);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, rest) = split_on(self.inner, b';')?;
        let (v, rest) = split_on(rest, b'\n')?;
        self.inner = rest;
        let v = fast_float::parse(v).expect("valid float");
        Some((k, v))
    }
}

#[derive(Debug)]
struct Acc {
    min: f32,
    sum: f32,
    max: f32,
    size: usize,
}

impl Default for Acc {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            sum: 0.,
            max: f32::MIN,
            size: 0,
        }
    }
}

impl Acc {
    fn add_value(&mut self, v: f32) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.sum += v;
        self.size += 1;
    }

    fn into_res(self) -> Res {
        Res {
            max: self.max,
            min: self.min,
            avg: self.sum / (self.size as f32),
            size: self.size,
        }
    }
}

struct Res {
    min: f32,
    avg: f32,
    max: f32,
    size: usize,
}

fn split_on_inclusive_from(s: &[u8], from: usize, c: u8) -> Option<(&[u8], &[u8])> {
    let index = from + s.get(from..)?.iter().position(|&x| x == c)?;
    Some((&s[..index + 1], &s[index + 1..]))
}

fn main() {
    let input_file = std::fs::read(INPUT).unwrap();
    let n_chunks = available_parallelism().unwrap().get();
    let chunk_size = input_file.len() / n_chunks;
    let data: DashMap<&[u8], Acc, BuildHasherDefault<FxHasher>> =
        DashMap::with_capacity_and_hasher(1000, Default::default());

    let mut remaining = input_file.as_slice();
    std::thread::scope(|scope| {
        while !remaining.is_empty() {
            let (chunk, rem) =
                split_on_inclusive_from(remaining, chunk_size, b'\n').unwrap_or((remaining, b""));
            remaining = rem;
            let data = &data;
            let _: ScopedJoinHandle<_> = scope.spawn(move || {
                for (k, v) in (Entries { inner: chunk }) {
                    data.entry(k).or_default().add_value(v);
                }
            });
        }
    });
    let mut res = Vec::new();
    for (k, v) in data {
        res.push((k, v.into_res()));
    }
    res.sort_by_key(|(k, _)| *k);
    let mut stdout = stdout().lock();
    for (k, v) in res {
        stdout.write_all(k).unwrap();
        writeln!(
            stdout,
            ": {:.1}/{:.1}/{:.1} ({})",
            v.min, v.avg, v.max, v.size
        )
        .unwrap();
    }
}
