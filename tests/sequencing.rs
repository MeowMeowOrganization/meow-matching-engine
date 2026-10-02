use meow_matching_engine::{
    CancelOrder, CancelRejectionReason, Command, EngineConfig, Event, EventOrdinal, MarketId,
    MarketSequence, MatchingEngine, OrderId, PlaceLimitOrder, PriceTicks, QuantityLots, Side,
};

fn price(value: i64) -> PriceTicks {
    PriceTicks::new(value).expect("valid canonical price")
}

fn quantity(value: i64) -> QuantityLots {
    QuantityLots::new(value).expect("valid canonical quantity")
}

fn engine() -> MatchingEngine {
    MatchingEngine::new(EngineConfig::new(MarketId::new(1)))
}

fn place_command(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Command {
    PlaceLimitOrder::new(
        OrderId::new(id),
        side,
        price(price_value),
        quantity(quantity_value),
    )
    .expect("valid place command")
    .into()
}

fn cancel_command(id: u64) -> Command {
    CancelOrder::new(OrderId::new(id)).into()
}

#[test]
fn zero_event_commands_still_advance_market_sequence() {
    let mut matching_engine = engine();

    let first = matching_engine
        .process(place_command(1, Side::Buy, 100, 5))
        .expect("first command commits");

    assert_eq!(first.market_sequence(), MarketSequence::new(1),);

    assert!(first.events().is_empty());

    let second = matching_engine
        .process(place_command(2, Side::Buy, 99, 5))
        .expect("second command commits");

    assert_eq!(second.market_sequence(), MarketSequence::new(2),);

    assert!(second.events().is_empty());

    assert_eq!(
        matching_engine.last_committed_sequence(),
        MarketSequence::new(2),
    );
}

#[test]
fn multi_fill_uses_one_sequence_and_ordered_event_ordinals() {
    let mut matching_engine = engine();

    matching_engine
        .process(place_command(1, Side::Sell, 100, 2))
        .expect("first maker rests");

    matching_engine
        .process(place_command(2, Side::Sell, 100, 3))
        .expect("second maker rests");

    let result = matching_engine
        .process(place_command(3, Side::Buy, 100, 5))
        .expect("taker fills both makers");

    assert_eq!(result.market_sequence(), MarketSequence::new(3),);

    assert_eq!(result.events().len(), 2);

    let first = result.events()[0];
    let second = result.events()[1];

    assert_eq!(first.id().market_id(), MarketId::new(1),);

    assert_eq!(first.id().market_sequence(), MarketSequence::new(3),);

    assert_eq!(first.id().event_ordinal(), EventOrdinal::new(0),);

    assert_eq!(second.id().market_sequence(), MarketSequence::new(3),);

    assert_eq!(second.id().event_ordinal(), EventOrdinal::new(1),);

    let Event::Execution(first_execution) = first.into_event() else {
        panic!("expected first execution");
    };

    let Event::Execution(second_execution) = second.into_event() else {
        panic!("expected second execution");
    };

    assert_eq!(first_execution.maker_order_id(), OrderId::new(1),);

    assert_eq!(second_execution.maker_order_id(), OrderId::new(2),);
}

#[test]
fn cancellation_rejection_consumes_one_sequence() {
    let mut matching_engine = engine();

    let result = matching_engine
        .process(cancel_command(999))
        .expect("unknown cancel is a business outcome");

    assert_eq!(result.market_sequence(), MarketSequence::new(1),);

    assert_eq!(result.events().len(), 1);

    assert_eq!(result.events()[0].id().event_ordinal(), EventOrdinal::ZERO,);

    let Event::CancelRejected(rejected) = result.events()[0].into_event() else {
        panic!("expected cancellation rejection");
    };

    assert_eq!(rejected.reason(), CancelRejectionReason::UnknownOrder,);
}

#[test]
fn matching_engine_does_not_deduplicate_repeated_payloads() {
    let mut matching_engine = engine();

    let command = cancel_command(999);

    let first = matching_engine
        .process(command)
        .expect("first command commits");

    let second = matching_engine
        .process(command)
        .expect("second command also commits");

    assert_eq!(first.market_sequence(), MarketSequence::new(1),);

    assert_eq!(second.market_sequence(), MarketSequence::new(2),);

    let Event::CancelRejected(first_rejection) = first.events()[0].into_event() else {
        panic!("expected first rejection");
    };

    let Event::CancelRejected(second_rejection) = second.events()[0].into_event() else {
        panic!("expected second rejection");
    };

    assert_eq!(
        first_rejection.reason(),
        CancelRejectionReason::UnknownOrder,
    );

    assert_eq!(
        second_rejection.reason(),
        CancelRejectionReason::UnknownOrder,
    );
}

#[test]
fn same_command_stream_produces_identical_sequences_events_and_state() {
    let commands = [
        place_command(1, Side::Sell, 101, 4),
        place_command(2, Side::Sell, 100, 3),
        place_command(3, Side::Buy, 101, 5),
        cancel_command(1),
        place_command(4, Side::Buy, 99, 6),
        cancel_command(999),
    ];

    let mut primary_engine = engine();
    let mut replay_engine = engine();

    let mut primary_results = Vec::new();
    let mut replay_results = Vec::new();

    for command in commands {
        primary_results.push(
            primary_engine
                .process(command)
                .expect("primary engine remains healthy"),
        );

        replay_results.push(
            replay_engine
                .process(command)
                .expect("replay engine remains healthy"),
        );
    }

    assert_eq!(primary_results, replay_results,);

    assert_eq!(primary_engine, replay_engine,);

    assert_eq!(
        primary_engine.last_committed_sequence(),
        MarketSequence::new(6),
    );
}
