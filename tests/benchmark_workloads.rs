#[path = "../benches/common/mod.rs"]
mod bench_common;

use bench_common::{
    MatchShape, boundary_fixture, build_terminal_history, multi_level_sweep_fixture,
    one_maker_fixture, partial_cancel_fixture, same_price_sweep_fixture,
};
use meow_matching_engine::{OrderId, OrderState, Side};

#[test]
fn w04_exact_match_produces_one_execution_for_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let (mut engine, command) = one_maker_fixture(side, MatchShape::Exact);
        let result = engine.process(command).expect("engine remains healthy");

        assert_eq!(result.events().len(), 1);
        assert!(engine.book().is_empty());
    }
}

#[test]
fn w05_same_price_sweep_event_count_equals_maker_count() {
    let makers = 16;
    let (mut engine, command) = same_price_sweep_fixture(Side::Buy, makers);
    let result = engine.process(command).expect("engine remains healthy");

    assert_eq!(result.events().len(), makers);
    assert!(engine.book().is_empty());
}

#[test]
fn w06_multi_level_sweep_event_count_equals_total_makers() {
    let levels = 4;
    let makers_per_level = 4;
    let (mut engine, command) = multi_level_sweep_fixture(Side::Sell, levels, makers_per_level);
    let result = engine.process(command).expect("engine remains healthy");

    assert_eq!(result.events().len(), levels * makers_per_level);
    assert!(engine.book().is_empty());
}

#[test]
fn w08_one_tick_miss_and_equality_have_distinct_paths() {
    let (mut miss_engine, miss_command) = boundary_fixture(Side::Buy, 16, false);
    let miss = miss_engine
        .process(miss_command)
        .expect("miss is a valid committed command");
    assert!(miss.events().is_empty());

    let (mut cross_engine, cross_command) = boundary_fixture(Side::Buy, 16, true);
    let cross = cross_engine
        .process(cross_command)
        .expect("equality cross is valid");
    assert_eq!(cross.events().len(), 1);
}

#[test]
fn w11_partial_cancel_fixture_really_is_partially_filled() {
    let (mut engine, command) = partial_cancel_fixture();

    let lifecycle = engine
        .order_lifecycle(OrderId::new(1))
        .expect("target lifecycle exists");
    assert_eq!(lifecycle.state(), OrderState::PartiallyFilled);
    assert_eq!(lifecycle.executed_quantity().get(), 40);

    let result = engine.process(command).expect("cancellation succeeds");
    assert_eq!(result.events().len(), 1);

    let lifecycle = engine
        .order_lifecycle(OrderId::new(1))
        .expect("terminal lifecycle remains");
    assert_eq!(lifecycle.state(), OrderState::Cancelled);
    assert_eq!(lifecycle.executed_quantity().get(), 40);
}

#[test]
fn w16_terminal_history_fixture_is_hashable_and_book_empty() {
    let engine = build_terminal_history(32);

    assert!(engine.book().is_empty());
    assert!(engine.state_hash().is_ok());
    assert!(engine.canonical_state_bytes().is_ok());
}
