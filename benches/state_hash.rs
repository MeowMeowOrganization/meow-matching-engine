mod common;

use std::{hint::black_box, time::Duration};

use common::{build_terminal_history, seed_balanced_book};
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};

fn bench_w16_book_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W16_state_hash/book_depth");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(1));

    for levels in [16_usize, 128, 1_024, 8_192] {
        let (engine, _) = seed_balanced_book(levels, 4, 1);
        let active_orders = levels.checked_mul(8).expect("active order count overflow");
        let parameter = format!("P{levels}_N{active_orders}");

        group.bench_with_input(
            BenchmarkId::new("state_hash", parameter),
            &levels,
            |b, _| {
                b.iter(|| {
                    black_box(
                        engine
                            .state_hash()
                            .expect("W16 canonical state must remain valid"),
                    );
                });
            },
        );
    }

    group.finish();
}

fn bench_w16_lifecycle_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W16_state_hash/lifecycle_history");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    for history in [1_000_usize, 100_000, 1_000_000] {
        let engine = build_terminal_history(history);

        group.bench_with_input(BenchmarkId::new("state_hash", history), &history, |b, _| {
            b.iter(|| {
                black_box(
                    engine
                        .state_hash()
                        .expect("W16 canonical state must remain valid"),
                );
            });
        });
    }

    group.finish();
}

fn bench_w16_encoding_diagnostic(c: &mut Criterion) {
    let mut group = c.benchmark_group("diagnostic/W16_canonical_encoding");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(1));

    // This is diagnostic only. Headline W16 remains engine.state_hash(), which
    // includes canonicalization, validation, allocation, encoding, and SHA-256.
    for history in [1_000_usize, 100_000] {
        let engine = build_terminal_history(history);

        group.bench_with_input(
            BenchmarkId::new("canonical_state_bytes", history),
            &history,
            |b, _| {
                b.iter(|| {
                    black_box(
                        engine
                            .canonical_state_bytes()
                            .expect("canonical encoding must remain valid"),
                    );
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_w16_book_depth,
    bench_w16_lifecycle_history,
    bench_w16_encoding_diagnostic,
);
criterion_main!(benches);
