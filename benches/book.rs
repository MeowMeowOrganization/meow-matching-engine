mod common;

use std::hint::black_box;

use common::{
    BASE_PRICE, DEFAULT_QUANTITY, resting_order, same_level_book_cancel_fixture,
    seed_balanced_order_book,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use meow_matching_engine::Side;

fn bench_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("diagnostic/book/W01_insertion");
    group.throughput(Throughput::Elements(1));

    for levels in [16_usize, 128, 1_024, 8_192] {
        for existing in [false, true] {
            let label = if existing {
                "existing_level"
            } else {
                "new_level"
            };

            group.bench_with_input(BenchmarkId::new(label, levels), &levels, |b, &levels| {
                b.iter_batched(
                    || {
                        let (book, next_id) = seed_balanced_order_book(levels, 4, DEFAULT_QUANTITY);
                        let px = if existing {
                            BASE_PRICE - 2
                        } else {
                            BASE_PRICE - 1
                        };
                        let order = resting_order(next_id, Side::Buy, px, DEFAULT_QUANTITY);
                        (book, order)
                    },
                    |(mut book, order)| {
                        book.insert(order)
                            .expect("diagnostic insertion must succeed");
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

fn bench_same_level_cancellation(c: &mut Criterion) {
    let mut group = c.benchmark_group("diagnostic/book/W02_same_level_cancel");
    group.throughput(Throughput::Elements(1));

    for order_count in [8_usize, 64, 1_024, 16_384] {
        let positions = [
            ("front", 0),
            ("quarter", order_count / 4),
            ("middle", order_count / 2),
            ("three_quarter", (order_count * 3) / 4),
            ("back", order_count - 1),
        ];

        for (position_name, target_index) in positions {
            group.bench_with_input(
                BenchmarkId::new(position_name, order_count),
                &(order_count, target_index),
                |b, &(order_count, target_index)| {
                    b.iter_batched(
                        || same_level_book_cancel_fixture(order_count, target_index),
                        |(mut book, target_id)| {
                            black_box(
                                book.remove(target_id)
                                    .expect("book must stay consistent")
                                    .expect("target must exist"),
                            );
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_insertion, bench_same_level_cancellation);
criterion_main!(benches);
