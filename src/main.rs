use std::{
    hash::{BuildHasherDefault, Hash},
    io::{stdout, Write},
    ops::Neg,
    thread::{available_parallelism, ScopedJoinHandle},
};

use dashmap::DashMap;
use fmmap::MmapFileExt;
use rustc_hash::{FxHashMap, FxHasher};

static INPUT: &str = "/home/arthur/1BRC/data/measurements-10th.txt";

struct Entries<'a> {
    inner: &'a [u8],
}

fn split_on(s: &[u8], c: u8) -> Option<(&[u8], &[u8])> {
    let index = s.iter().position(|&x| x == c)?;
    Some((&s[..index], &s[index + 1..]))
}

fn parse_int(input: &[u8]) -> i16 {
    let f = |idx, fact| parse_digit(input[idx]) * fact;
    match input.len() {
        5 => {
            // -XX.X
            (f(1, 100) + f(2, 10) + f(4, 1)).neg()
        }
        4 => {
            if input[0] == b'-' {
                // -D.D
                (f(1, 10) + f(3, 1)).neg()
            } else {
                // DD.D
                f(0, 100) + f(1, 10) + f(3, 1)
            }
        }
        3 => {
            // D.D
            f(0, 10) + f(2, 1)
        }
        _ => 0,
    }
}

fn parse_digit(b: u8) -> i16 {
    (b - b'0') as i16
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct ShortStationId {
    inner: [u64; 2],
}

impl Hash for ShortStationId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for n in self.inner {
            n.hash(state)
        }
    }
}

impl ShortStationId {
    const fn max_size() -> usize {
        std::mem::size_of::<Self>()
    }

    fn as_bytes(&self) -> &[u8] {
        let first_null = self
            .inner
            .iter()
            .flat_map(|n| n.to_ne_bytes())
            .position(|x| x == 0)
            .unwrap_or(ShortStationId::max_size());
        unsafe { std::slice::from_raw_parts(self.inner.as_ptr().cast(), first_null) }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut inner: [u64; 2] = Default::default();

        // dbg!(bytes.len(),std::mem::size_of_val(&inner));
        assert!(bytes.len() <= ShortStationId::max_size());
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), inner.as_mut_ptr().cast(), bytes.len())
        }
        Self { inner }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
enum StationId<'a> {
    Short(ShortStationId),
    Long(&'a [u8]),
}

impl<'a> StationId<'a> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            StationId::Short(s) => s.as_bytes(),
            StationId::Long(l) => l,
        }
    }
    fn to_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap()
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        if bytes.len() > ShortStationId::max_size() {
            Self::Long(bytes)
        } else {
            Self::Short(ShortStationId::from_bytes(bytes))
        }
    }
}

impl<'a> Iterator for Entries<'a> {
    type Item = (StationId<'a>, i16);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, rest) = split_on(self.inner, b';')?;
        let (v, rest) = split_on(rest, b'\n')?;
        self.inner = rest;
        let v = parse_int(v);
        Some((StationId::from_bytes(k), v))
    }
}

#[derive(Debug)]
struct Acc {
    min: i16,
    sum: i32,
    max: i16,
    size: i32,
}

impl Default for Acc {
    fn default() -> Self {
        Self {
            min: i16::MAX,
            sum: 0,
            max: i16::MIN,
            size: 0,
        }
    }
}

impl Acc {
    fn add_value(&mut self, v: i16) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.sum += v as i32;
        self.size += 1;
    }

    fn add_acc(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.size += other.size;
    }

    fn to_res(&self) -> Res {
        Res {
            max: self.max as f32 / 10.,
            min: self.min as f32 / 10.,
            avg: self.sum as f32 / (self.size as f32),
            size: self.size,
        }
    }
}

struct Res {
    min: f32,
    avg: f32,
    max: f32,
    size: i32,
}

fn split_on_inclusive_from(s: &[u8], from: usize, c: u8) -> Option<(&[u8], &[u8])> {
    let index = from + s.get(from..)?.iter().position(|&x| x == c)?;
    Some((&s[..index + 1], &s[index + 1..]))
}

fn main() {
    // let input_file = std::fs::read(INPUT).unwrap();
    let input_file = fmmap::sync::MmapFile::open(INPUT).unwrap();
    let input_file = input_file.as_slice();
    let n_chunks = available_parallelism().unwrap().get();
    let chunk_size = input_file.len() / n_chunks;
    let global_map: DashMap<StationId, Acc, BuildHasherDefault<FxHasher>> =
        DashMap::with_capacity_and_hasher(1000, Default::default());

    let mut remaining = input_file;
    std::thread::scope(|scope| {
        while !remaining.is_empty() {
            let (chunk, rem) =
                split_on_inclusive_from(remaining, chunk_size, b'\n').unwrap_or((remaining, b""));
            remaining = rem;
            let global_map = &global_map;
            let _: ScopedJoinHandle<_> = scope.spawn(move || {
                let mut local_map: FxHashMap<_, Acc> =
                    FxHashMap::with_capacity_and_hasher(1000, Default::default());
                for (k, v) in (Entries { inner: chunk }) {
                    local_map.entry(k).or_default().add_value(v);
                }
                for (k, v) in local_map {
                    global_map.entry(k).or_default().add_acc(&v);
                }
            });
        }
    });
    let mut res = Vec::new();
    let data = global_map.into_read_only();
    for (k, v) in data.iter() {
        res.push((k.to_str(), v.to_res()));
    }
    res.sort_by_key(|(k, _)| *k);
    let mut stdout = stdout().lock();
    for (k, v) in res {
        stdout.write_all(k.as_bytes()).unwrap();
        writeln!(
            stdout,
            ": {:.1}/{:.1}/{:.1} ({}) {}",
            v.min,
            v.avg,
            v.max,
            v.size,
            k.len()
        )
        .unwrap();
    }
}
