use meow_matching_engine::{
    CancelOrder, Command, EngineConfig, MarketId, MatchingEngine, OrderId, PlaceLimitOrder,
    PriceTicks, QuantityLots, Side,
};

fn engine() -> MatchingEngine {
    MatchingEngine::new(EngineConfig::new(MarketId::new(1)))
}

fn price(value: i64) -> PriceTicks {
    PriceTicks::new(value).expect("valid price")
}

fn quantity(value: i64) -> QuantityLots {
    QuantityLots::new(value).expect("valid quantity")
}

fn place(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Command {
    PlaceLimitOrder::new(
        OrderId::new(id),
        side,
        price(price_value),
        quantity(quantity_value),
    )
    .expect("valid command")
    .into()
}

fn cancel(id: u64) -> Command {
    CancelOrder::new(OrderId::new(id)).into()
}

#[test]
fn replay_produces_identical_canonical_state_after_every_command() {
    let commands = [
        place(10, Side::Sell, 101, 4),
        place(20, Side::Sell, 100, 6),
        place(30, Side::Buy, 101, 7),
        place(40, Side::Buy, 99, 8),
        cancel(10),
        cancel(999),
        place(50, Side::Sell, 99, 3),
    ];

    let mut primary = engine();
    let mut replay = engine();

    for command in commands {
        let primary_result = primary.process(command).expect("primary remains healthy");

        let replay_result = replay.process(command).expect("replay remains healthy");

        assert_eq!(primary_result, replay_result,);

        assert_eq!(
            primary
                .canonical_state_bytes()
                .expect("primary state valid"),
            replay.canonical_state_bytes().expect("replay state valid"),
        );

        assert_eq!(
            primary.state_hash().expect("primary hash"),
            replay.state_hash().expect("replay hash"),
        );
    }

    assert_eq!(primary, replay);
}

#[test]
fn sequence_only_transition_changes_state_hash() {
    let untouched = engine();
    let mut advanced = engine();

    advanced
        .process(cancel(999))
        .expect("unknown cancellation is a valid business outcome");

    assert_eq!(untouched.book(), advanced.book(),);

    assert_ne!(
        untouched.state_hash().expect("hash"),
        advanced.state_hash().expect("hash"),
    );
}

#[test]
fn different_fifo_priority_produces_different_state_hash() {
    let mut first = engine();

    first
        .process(place(10, Side::Buy, 100, 5))
        .expect("command");

    first
        .process(place(20, Side::Buy, 100, 5))
        .expect("command");

    let mut second = engine();

    second
        .process(place(20, Side::Buy, 100, 5))
        .expect("command");

    second
        .process(place(10, Side::Buy, 100, 5))
        .expect("command");

    assert_eq!(
        first.last_committed_sequence(),
        second.last_committed_sequence(),
    );

    assert_ne!(
        first.state_hash().expect("first hash"),
        second.state_hash().expect("second hash"),
    );
}

#[test]
fn lifecycle_history_affects_state_hash_even_when_book_is_empty() {
    let mut with_history = engine();

    with_history
        .process(place(1, Side::Buy, 100, 5))
        .expect("place");

    with_history.process(cancel(1)).expect("cancel");

    let mut without_history = engine();

    // Advance sequence to the same value without creating order history.
    without_history
        .process(cancel(900))
        .expect("unknown cancel");

    without_history
        .process(cancel(901))
        .expect("unknown cancel");

    assert!(with_history.book().is_empty());

    assert!(without_history.book().is_empty());

    assert_eq!(
        with_history.last_committed_sequence(),
        without_history.last_committed_sequence(),
    );

    assert_ne!(
        with_history.state_hash().expect("history hash"),
        without_history.state_hash().expect("empty-history hash"),
    );
}
