use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use meow_matching_engine::{EngineConfig, MarketId, MatchingEngine};

fn construct_engine(c: &mut Criterion) {
    let config = EngineConfig::new(MarketId::new(1));

    c.bench_function("foundation/construct_engine", |b| {
        b.iter(|| MatchingEngine::new(black_box(config.clone())));
    });
}

criterion_group!(benches, construct_engine);
criterion_main!(benches);
