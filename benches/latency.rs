mod common;

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use common::{
    HOT_TRACE_LEN, MIXED_TRACE_LEN, MixedProfile, build_hot_churn_workload, build_mixed_workload,
};
use meow_matching_engine::{Command, MatchingEngine};

const DEFAULT_ROUNDS: usize = 32;

#[derive(Debug)]
struct Summary {
    samples: usize,
    commands: usize,
    elapsed: Duration,
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
    p999_ns: u128,
    max_ns: u128,
}

fn main() {
    println!("CEX 10 raw per-command latency runner");
    println!("This measures only MatchingEngine::process(Command).");
    println!("Fixture construction is outside each timed command.");
    println!();

    let timer_overhead = timer_overhead_ns();
    println!("observed Instant timing-floor p50: {timer_overhead} ns");
    println!("Treat sub-microsecond tails cautiously when close to this floor.");
    println!();

    for profile in [
        MixedProfile::Passive,
        MixedProfile::Balanced,
        MixedProfile::Aggressive,
    ] {
        let summary = measure_trace_rounds(DEFAULT_ROUNDS, || {
            build_mixed_workload(profile, MIXED_TRACE_LEN)
        });
        print_summary(&format!("W13/{}", profile.name()), &summary);
    }

    let hot = measure_trace_rounds(DEFAULT_ROUNDS, || build_hot_churn_workload(HOT_TRACE_LEN));
    print_summary("W14/hot_price_churn", &hot);
}

fn measure_trace_rounds<F>(rounds: usize, mut build: F) -> Summary
where
    F: FnMut() -> (MatchingEngine, Vec<Command>),
{
    let mut latencies_ns = Vec::new();
    let mut command_count = 0usize;
    let wall_start = Instant::now();

    for _ in 0..rounds {
        let (mut engine, commands) = build();
        latencies_ns.reserve(commands.len());

        for command in commands {
            let start = Instant::now();
            let result = engine
                .process(command)
                .expect("latency trace must preserve engine invariants");
            let elapsed = start.elapsed();

            black_box(result);
            latencies_ns.push(elapsed.as_nanos());
            command_count = command_count
                .checked_add(1)
                .expect("command count overflow");
        }
    }

    let elapsed = wall_start.elapsed();
    latencies_ns.sort_unstable();

    Summary {
        samples: latencies_ns.len(),
        commands: command_count,
        elapsed,
        p50_ns: percentile(&latencies_ns, 500),
        p95_ns: percentile(&latencies_ns, 950),
        p99_ns: percentile(&latencies_ns, 990),
        p999_ns: percentile(&latencies_ns, 999),
        max_ns: *latencies_ns.last().expect("latency samples exist"),
    }
}

// permille is 500 => p50, 950 => p95, 990 => p99, 999 => p99.9.
fn percentile(sorted_ns: &[u128], permille: usize) -> u128 {
    assert!(!sorted_ns.is_empty());
    assert!(permille <= 1_000);

    let n = sorted_ns.len();
    let rank_numerator = permille.checked_mul(n).expect("percentile rank overflow");
    let rank = rank_numerator
        .checked_add(999)
        .expect("percentile rank overflow")
        / 1_000;
    let index = rank.saturating_sub(1).min(n - 1);

    sorted_ns[index]
}

fn timer_overhead_ns() -> u128 {
    let mut samples = Vec::with_capacity(10_000);

    for _ in 0..10_000 {
        let start = Instant::now();
        black_box(());
        samples.push(start.elapsed().as_nanos());
    }

    samples.sort_unstable();
    percentile(&samples, 500)
}

fn print_summary(name: &str, summary: &Summary) {
    let seconds = summary.elapsed.as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let commands_per_second = if seconds > 0.0 {
        summary.commands as f64 / seconds
    } else {
        f64::INFINITY
    };

    println!("{name}");
    println!("  samples:      {}", summary.samples);
    println!("  commands/s:   {commands_per_second:.0}");
    println!("  p50:          {} ns", summary.p50_ns);
    println!("  p95:          {} ns", summary.p95_ns);
    println!("  p99:          {} ns", summary.p99_ns);
    println!("  p99.9:        {} ns", summary.p999_ns);
    println!("  max:          {} ns", summary.max_ns);
    println!();
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::percentile;

    #[test]
    fn percentile_uses_nearest_rank() {
        let values: Vec<u128> = (1..=1_000).collect();

        assert_eq!(percentile(&values, 500), 500);
        assert_eq!(percentile(&values, 950), 950);
        assert_eq!(percentile(&values, 990), 990);
        assert_eq!(percentile(&values, 999), 999);
    }
}
