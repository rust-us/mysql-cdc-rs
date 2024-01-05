use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Stu {
    name: String,
    age: u32,
    address: String,
    sex: u8,
}

fn get_stus(num: usize) -> Vec<Stu> {
    let mut stus = Vec::with_capacity(num);
    for _ in 0..num {
        let s = Stu {
            name: "张三".to_string(),
            age: 18,
            address: "杭州余杭区".to_string(),
            sex: 1,
        };
        stus.push(s);
    }
    stus
}

/// Bincode
fn bincode_serialize<T: Sized + Serialize>(t: Vec<T>) -> Vec<Vec<u8>> {
    let mut bytes = Vec::new();
    for i in t.iter() {
        let result = bincode::serialize(i).unwrap();
        bytes.push(result);
    }
    bytes
}

fn bincode_deserialize<'a, T: Sized + Deserialize<'a>>(bytes: &'a Vec<Vec<u8>>) -> Vec<T> {
    let mut ss = Vec::with_capacity(bytes.len());
    for b in bytes.iter() {
        let s = bincode::deserialize(b).unwrap();
        ss.push(s);
    }
    ss
}

fn bincode_serialize_deserialize(num: usize) {
    let stus = get_stus(num);

    let bytes = bincode_serialize(stus);

    let des: Vec<Stu> = bincode_deserialize(&bytes);
}

fn bincode_serialize_benchmark(c: &mut Criterion) {
    c.bench_function("bincode_serialize 20", |b| b.iter(|| bincode_serialize_deserialize(20)));
}


criterion_group!(benches, bincode_serialize_benchmark);
criterion_main!(benches);