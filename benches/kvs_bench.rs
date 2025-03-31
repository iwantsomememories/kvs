use criterion::{Criterion, BatchSize};
use criterion::{criterion_group, criterion_main};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use tempfile::TempDir;
use sled;

use kvs::{KvStore, KvsEngine, SledEngine};

/// 生成100个随机长度键值对
fn gennerate_kvpairs() -> Vec<(String, String)>  {
    let mut key_value_pairs = Vec::with_capacity(100);
    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        let key_len = rng.gen_range(1, 100_001);

        let value_len = rng.gen_range(1, 100_001);

        let key: String = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(key_len)
            .collect();
        
        let value: String = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(value_len)
            .collect();

        key_value_pairs.push((key, value));
    }

    key_value_pairs
}

fn set_bench(c: &mut Criterion) {
    let key_value_pairs = gennerate_kvpairs();
    let mut group = c.benchmark_group("set_bench");

    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                // 打开一个kvs引擎
                let temp_dir = TempDir::new().unwrap();
                (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
            }, 
            |(mut store, _temp_dir)| {
                for (k, v) in key_value_pairs.iter() {
                    let res = store.set(String::from(k), String::from(v));
                    assert!(res.is_ok());
                }
            }, 
            BatchSize::SmallInput
        );
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                // 打开一个sled引擎
                let temp_dir = TempDir::new().unwrap();
                (SledEngine::new(sled::open(temp_dir.path()).unwrap()), temp_dir)
            }, 
            |(mut db, _temp_dir)| {
                for (k, v) in key_value_pairs.iter() {
                    let res = db.set(String::from(k), String::from(v));
                    assert!(res.is_ok());
                }
            }, 
            BatchSize::SmallInput
        );
    });

    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let key_value_pairs = gennerate_kvpairs();
    let mut request_keys = Vec::with_capacity(1000);
    let mut rng = rand::thread_rng();
    for _ in 0..1000 {
        let index: usize = rng.gen_range(0, 100);
        request_keys.push(&key_value_pairs[index].0);
    }

    let mut group = c.benchmark_group("get_bench");

    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                // 打开一个kvs引擎并设置键值对
                let temp_dir = TempDir::new().unwrap();
                let mut store = KvStore::open(temp_dir.path()).unwrap();
                for (k, v) in key_value_pairs.iter() {
                    let res = store.set(String::from(k), String::from(v));
                    assert!(res.is_ok());
                }
                (store, temp_dir)
            }, |(mut store, _temp_dir)| {
                for &key in request_keys.iter() {
                    let res = store.get(String::from(key));
                    assert!(res.is_ok_and(|x| x.is_some()));
                }
            }, 
            BatchSize::SmallInput);
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                // 打开一个sled引擎并设置键值对
                let temp_dir = TempDir::new().unwrap();
                let mut db = SledEngine::new(sled::open(&temp_dir
                ).unwrap());
                for (k, v) in key_value_pairs.iter() {
                    let res = db.set(String::from(k), String::from(v));
                    assert!(res.is_ok());
                }
                (db, temp_dir)
            }, |(mut db, _temp_dir)| {
                for &key in request_keys.iter() {
                    let res = db.get(String::from(key));
                    assert!(res.is_ok_and(|x| x.is_some()));
                }
            }, 
            BatchSize::SmallInput);
    });

    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
