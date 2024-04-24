use rustc_hash::FxHashMap;

type Map<K, V> = FxHashMap<K, V>;
static INPUT: &str = "/home/arthur/1BRC/data/measurements.txt";

struct Entries<'a> {
    inner: &'a [u8],
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a str, f32);

    fn next(&mut self) -> Option<Self::Item> {
        let mut it = self.inner.splitn(3, |&b| b == b';' || b == b'\n');
        let k = it.next()?;
        let k = std::str::from_utf8(k).expect("Correct uft8");
        let v = it.next()?;
        let v = fast_float::parse(v).expect("valid float");
        self.inner = it.next().unwrap_or_default();
        Some((k, v))
    }
}

struct Res {
    min: f32,
    avg: f32,
    max: f32,
    size: usize,
}

fn main() {
    let input_file = std::fs::read(INPUT).unwrap();
    let mut data: Map<&str, Vec<f32>> = Default::default();
    for (k, v) in (Entries { inner: &input_file }) {
        data.entry(k)
            .or_insert_with(|| Vec::with_capacity(2000))
            .push(v);
    }
    let mut res = Vec::new();
    for (k, vs) in data {
        let min = vs.iter().copied().reduce(|x, y| f32::min(x, y)).unwrap();
        let max = vs.iter().copied().reduce(|x, y| f32::max(x, y)).unwrap();
        let avg = vs.iter().sum::<f32>() / (vs.len() as f32);
        res.push((
            k,
            Res {
                min,
                avg,
                max,
                size: vs.len(),
            },
        ));
    }
    res.sort_by_key(|(k, _)| *k);
    for (k, v) in res {
        println!("{k}: {:.1}/{:.1}/{:.1} ({})", v.min, v.avg, v.max, v.size);
    }
}
