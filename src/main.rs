use std::fs::read_to_string;

use rustc_hash::FxHashMap;

type Map<K, V> = FxHashMap<K, V>;
static INPUT: &str = "/home/arthur/1BRC/data/measurements.txt";

fn parse_line(s: &str) -> (&str, f32) {
    let (key, v) = s.split_once(';').expect("Line should be well formatted.");
    let v = v.parse().expect("Value should be parseable as a float");
    (key, v)
}

struct Res {
    min: f32,
    avg: f32,
    max: f32,
}

fn main() {
    let input_file = read_to_string(INPUT).unwrap();
    let mut data: Map<&str, Vec<f32>> = Default::default();
    for l in input_file.lines() {
        let (k, v) = parse_line(l);
        data.entry(k).or_default().push(v);
    }
    let mut res = Vec::new();
    for (k, vs) in data {
        let min = vs.iter().copied().reduce(|x, y| f32::min(x, y)).unwrap();
        let max = vs.iter().copied().reduce(|x, y| f32::max(x, y)).unwrap();
        let avg = vs.iter().sum::<f32>() / (vs.len() as f32);
        res.push((k, Res { min, avg, max }));
    }
    res.sort_by_key(|(k, _)| *k);
    for (k, v) in res {
        println!("{k}: {:.1}/{:.1}/{:.1}", v.min, v.avg, v.max);
    }
}
