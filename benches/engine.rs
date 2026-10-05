mod common;

use std::{hint::black_box, time::Duration};

use common::{
    HOT_TRACE_LEN, MIXED_TRACE_LEN, MatchShape, MixedProfile, boundary_fixture,
    build_hot_churn_workload, build_mixed_workload, build_terminal_history, deep_cancel_fixture,
    deep_one_maker_fixture, event_count_fixture, execute_trace, insertion_fixture,
    multi_level_sweep_fixture, one_maker_fixture, partial_cancel_fixture,
    partial_final_maker_fixture, same_level_cancel_fixture, same_price_sweep_fixture,
    terminal_cancel_fixture,
};
use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use meow_matching_engine::Side;

fn bench_w01_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W01_insertion");
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
                    || insertion_fixture(levels, existing),
                    |(mut engine, command)| {
                        black_box(engine.process(command).expect("W01 engine stays healthy"));
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

fn bench_w02_same_level_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W02_same_level_cancel");
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
                        || same_level_cancel_fixture(order_count, target_index),
                        |(mut engine, command)| {
                            black_box(engine.process(command).expect("W02 engine stays healthy"));
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_w03_deep_book_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W03_deep_book_cancel");
    group.throughput(Throughput::Elements(1));

    for levels in [16_usize, 128, 1_024, 8_192] {
        let targets = [
            ("best_region", 0),
            ("middle_region", levels / 2),
            ("worst_region", levels - 1),
        ];

        for (name, target_level) in targets {
            group.bench_with_input(
                BenchmarkId::new(name, levels),
                &(levels, target_level),
                |b, &(levels, target_level)| {
                    b.iter_batched(
                        || deep_cancel_fixture(levels, 4, target_level, 2),
                        |(mut engine, command)| {
                            black_box(engine.process(command).expect("W03 engine stays healthy"));
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_w04_one_maker(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W04_one_maker_match");
    group.throughput(Throughput::Elements(1));

    let shapes = [
        ("exact", MatchShape::Exact),
        ("partial_maker", MatchShape::PartialMaker),
        ("partial_taker", MatchShape::PartialTaker),
    ];

    for side in [Side::Buy, Side::Sell] {
        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        for (shape_name, shape) in shapes {
            let id = format!("{side_name}/{shape_name}");
            group.bench_function(id, |b| {
                b.iter_batched(
                    || one_maker_fixture(side, shape),
                    |(mut engine, command)| {
                        black_box(engine.process(command).expect("W04 engine stays healthy"));
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

fn bench_w05_same_price_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W05_same_price_fifo_sweep");
    group.throughput(Throughput::Elements(1));

    for side in [Side::Buy, Side::Sell] {
        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        for makers in [1_usize, 4, 16, 64, 256, 1_024] {
            group.bench_with_input(
                BenchmarkId::new(side_name, makers),
                &makers,
                |b, &makers| {
                    b.iter_batched(
                        || same_price_sweep_fixture(side, makers),
                        |(mut engine, command)| {
                            let result = engine.process(command).expect("W05 engine stays healthy");
                            assert_eq!(result.events().len(), makers);
                            black_box(result);
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_w06_multi_level_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W06_multi_level_sweep");
    group.throughput(Throughput::Elements(1));

    let shapes = [
        (4_usize, 1_usize),
        (16, 1),
        (64, 1),
        (16, 4),
        (64, 4),
        (256, 4),
    ];

    for side in [Side::Buy, Side::Sell] {
        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        for (levels, makers_per_level) in shapes {
            let maker_count = levels * makers_per_level;
            let parameter = format!("L{levels}_M{maker_count}");
            group.bench_with_input(
                BenchmarkId::new(side_name, parameter),
                &(levels, makers_per_level),
                |b, &(levels, makers_per_level)| {
                    b.iter_batched(
                        || multi_level_sweep_fixture(side, levels, makers_per_level),
                        |(mut engine, command)| {
                            let result = engine.process(command).expect("W06 engine stays healthy");
                            assert_eq!(result.events().len(), levels * makers_per_level);
                            black_box(result);
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_w07_deep_book_one_maker(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W07_deep_book_one_maker");
    group.throughput(Throughput::Elements(1));

    for side in [Side::Buy, Side::Sell] {
        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        for levels in [16_usize, 128, 1_024, 8_192] {
            group.bench_with_input(
                BenchmarkId::new(side_name, levels),
                &levels,
                |b, &levels| {
                    b.iter_batched(
                        || deep_one_maker_fixture(side, levels),
                        |(mut engine, command)| {
                            let result = engine.process(command).expect("W07 engine stays healthy");
                            assert_eq!(result.events().len(), 1);
                            black_box(result);
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_w08_crossing_boundary(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W08_crossing_boundary");
    group.throughput(Throughput::Elements(1));

    for side in [Side::Buy, Side::Sell] {
        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        for levels in [16_usize, 128, 1_024, 8_192] {
            for crosses in [false, true] {
                let boundary_name = if crosses {
                    "equality_cross"
                } else {
                    "one_tick_miss"
                };
                let id = format!("{side_name}/{boundary_name}");

                group.bench_with_input(BenchmarkId::new(id, levels), &levels, |b, &levels| {
                    b.iter_batched(
                        || boundary_fixture(side, levels, crosses),
                        |(mut engine, command)| {
                            let result = engine.process(command).expect("W08 engine stays healthy");
                            let expected = usize::from(crosses);
                            assert_eq!(result.events().len(), expected);
                            black_box(result);
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
    }

    group.finish();
}

fn bench_w09_partial_final_maker(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W09_partial_final_maker");
    group.throughput(Throughput::Elements(1));

    for side in [Side::Buy, Side::Sell] {
        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        for makers in [4_usize, 16, 64, 256] {
            group.bench_with_input(
                BenchmarkId::new(side_name, makers),
                &makers,
                |b, &makers| {
                    b.iter_batched(
                        || partial_final_maker_fixture(side, makers),
                        |(mut engine, command)| {
                            let result = engine.process(command).expect("W09 engine stays healthy");
                            assert_eq!(result.events().len(), makers);
                            black_box(result);
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_w10_event_envelope_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W10_event_envelope_scaling");
    group.throughput(Throughput::Elements(1));

    for event_count in [0_usize, 1, 4, 16, 64, 256, 1_024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(event_count),
            &event_count,
            |b, &event_count| {
                b.iter_batched(
                    || event_count_fixture(event_count),
                    |(mut engine, command)| {
                        let result = engine.process(command).expect("W10 engine stays healthy");
                        assert_eq!(result.events().len(), event_count);
                        black_box(result);
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

fn bench_w11_partial_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W11_cancel_partially_filled");
    group.throughput(Throughput::Elements(1));

    group.bench_function("remaining_60_of_100", |b| {
        b.iter_batched(
            partial_cancel_fixture,
            |(mut engine, command)| {
                black_box(engine.process(command).expect("W11 engine stays healthy"));
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_w12_terminal_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W12_terminal_unknown_cancel");
    group.throughput(Throughput::Elements(1));

    for kind in ["unknown", "filled", "cancelled", "rejected"] {
        group.bench_function(kind, |b| {
            let (mut engine, command) = terminal_cancel_fixture(kind);

            b.iter(|| {
                black_box(
                    engine
                        .process(command)
                        .expect("W12 business rejection keeps engine healthy"),
                );
            });
        });
    }

    group.finish();
}

fn bench_w13_mixed_workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W13_mixed_workload");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(8));

    for profile in [
        MixedProfile::Passive,
        MixedProfile::Balanced,
        MixedProfile::Aggressive,
    ] {
        group.throughput(Throughput::Elements(MIXED_TRACE_LEN as u64));
        group.bench_function(profile.name(), |b| {
            b.iter_batched(
                || build_mixed_workload(profile, MIXED_TRACE_LEN),
                |(mut engine, commands)| {
                    black_box(execute_trace(&mut engine, commands));
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_w14_hot_price_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W14_hot_price_churn");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(HOT_TRACE_LEN as u64));

    group.bench_function("top5_concentrated", |b| {
        b.iter_batched(
            || build_hot_churn_workload(HOT_TRACE_LEN),
            |(mut engine, commands)| {
                black_box(execute_trace(&mut engine, commands));
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_w15_lifecycle_history_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/W15_lifecycle_history_growth");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for history in [1_000_usize, 100_000, 1_000_000] {
        group.bench_with_input(
            BenchmarkId::new("already_cancelled", history),
            &history,
            |b, &history| {
                let mut engine = build_terminal_history(history);
                let command = common::cancel(1);

                b.iter(|| {
                    black_box(
                        engine
                            .process(command)
                            .expect("W15 terminal cancellation is deterministic"),
                    );
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("unknown", history),
            &history,
            |b, &history| {
                let mut engine = build_terminal_history(history);
                let unknown_id = u64::try_from(history)
                    .expect("history size fits u64")
                    .checked_add(10_000)
                    .expect("benchmark id overflow");
                let command = common::cancel(unknown_id);

                b.iter(|| {
                    black_box(
                        engine
                            .process(command)
                            .expect("W15 unknown cancellation is deterministic"),
                    );
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_w01_insertion,
    bench_w02_same_level_cancel,
    bench_w03_deep_book_cancel,
    bench_w04_one_maker,
    bench_w05_same_price_sweep,
    bench_w06_multi_level_sweep,
    bench_w07_deep_book_one_maker,
    bench_w08_crossing_boundary,
    bench_w09_partial_final_maker,
    bench_w10_event_envelope_scaling,
    bench_w11_partial_cancel,
    bench_w12_terminal_cancel,
    bench_w13_mixed_workloads,
    bench_w14_hot_price_churn,
    bench_w15_lifecycle_history_lookup,
);
criterion_main!(benches);
