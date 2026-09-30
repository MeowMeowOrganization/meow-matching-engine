use meow_matching_engine::{
    Command, EngineConfig, Event, MarketId, MatchingEngine, OrderId, OrderRejectionReason,
    PlaceLimitOrder, PriceTicks, QuantityLots, Side,
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

fn command(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Command {
    PlaceLimitOrder::new(
        OrderId::new(id),
        side,
        price(price_value),
        quantity(quantity_value),
    )
    .expect("valid test command")
    .into()
}

fn process(
    engine: &mut MatchingEngine,
    id: u64,
    side: Side,
    price_value: i64,
    quantity_value: i64,
) -> Vec<Event> {
    engine
        .process(command(id, side, price_value, quantity_value))
        .expect("engine remains healthy")
}

fn assert_execution(
    event: &Event,
    expected_resting_id: u64,
    expected_incoming_id: u64,
    expected_side: Side,
    expected_price: i64,
    expected_quantity: i64,
) {
    let Event::Execution(execution) = event else {
        panic!("expected execution event");
    };

    assert_eq!(execution.market_id(), MarketId::new(1));
    assert_eq!(
        execution.maker_order_id(),
        OrderId::new(expected_resting_id),
    );
    assert_eq!(
        execution.taker_order_id(),
        OrderId::new(expected_incoming_id),
    );
    assert_eq!(execution.taker_side(), expected_side);
    assert_eq!(execution.price(), price(expected_price));
    assert_eq!(execution.quantity(), quantity(expected_quantity),);
}

#[test]
fn buy_no_match_rests_full_quantity() {
    let mut matching_engine = engine();

    let maker_events = process(&mut matching_engine, 1, Side::Sell, 101, 5);

    assert!(maker_events.is_empty());

    let incoming_events = process(&mut matching_engine, 2, Side::Buy, 100, 7);

    assert!(incoming_events.is_empty());

    let resting_buy = matching_engine
        .book()
        .order(OrderId::new(2))
        .expect("buy rests");

    assert_eq!(resting_buy.price(), price(100));
    assert_eq!(resting_buy.quantity(), quantity(7));

    assert_eq!(
        matching_engine
            .book()
            .best_ask()
            .expect("ask remains")
            .price(),
        price(101),
    );
}

#[test]
fn sell_no_match_rests_full_quantity() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Buy, 99, 5);

    let incoming_events = process(&mut matching_engine, 2, Side::Sell, 100, 8);

    assert!(incoming_events.is_empty());

    let resting_sell = matching_engine
        .book()
        .order(OrderId::new(2))
        .expect("sell rests");

    assert_eq!(resting_sell.price(), price(100));
    assert_eq!(resting_sell.quantity(), quantity(8));
}

#[test]
fn exact_buy_match_fills_both_orders() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 100, 5);

    let events = process(&mut matching_engine, 2, Side::Buy, 100, 5);

    assert_eq!(events.len(), 1);

    assert_execution(&events[0], 1, 2, Side::Buy, 100, 5);

    assert!(matching_engine.book().is_empty());
}

#[test]
fn exact_sell_match_crosses_on_equal_price() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Buy, 100, 5);

    let events = process(&mut matching_engine, 2, Side::Sell, 100, 5);

    assert_eq!(events.len(), 1);

    assert_execution(&events[0], 1, 2, Side::Sell, 100, 5);

    assert!(matching_engine.book().is_empty());
}

#[test]
fn execution_uses_maker_price_not_taker_limit() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 100, 5);

    let events = process(&mut matching_engine, 2, Side::Buy, 105, 5);

    assert_eq!(events.len(), 1);

    assert_execution(&events[0], 1, 2, Side::Buy, 100, 5);
}

#[test]
fn partial_maker_keeps_fifo_priority() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 100, 10);

    process(&mut matching_engine, 2, Side::Sell, 100, 20);

    process(&mut matching_engine, 3, Side::Sell, 100, 30);

    let events = process(&mut matching_engine, 4, Side::Buy, 100, 15);

    assert_eq!(events.len(), 2);

    assert_execution(&events[0], 1, 4, Side::Buy, 100, 10);

    assert_execution(&events[1], 2, 4, Side::Buy, 100, 5);

    assert!(matching_engine.book().order(OrderId::new(1)).is_none(),);

    let partial_maker = matching_engine
        .book()
        .order(OrderId::new(2))
        .expect("partially filled maker remains");

    assert_eq!(partial_maker.quantity(), quantity(15));

    let price_level = matching_engine
        .book()
        .level(Side::Sell, price(100))
        .expect("price level remains");

    assert_eq!(
        price_level.order_ids().collect::<Vec<_>>(),
        vec![OrderId::new(2), OrderId::new(3)],
    );

    assert_eq!(price_level.aggregate_quantity(), quantity(45),);
}

#[test]
fn partial_taker_rests_only_remaining_quantity() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 100, 4);

    let events = process(&mut matching_engine, 2, Side::Buy, 100, 10);

    assert_eq!(events.len(), 1);

    assert_execution(&events[0], 1, 2, Side::Buy, 100, 4);

    assert!(matching_engine.book().order(OrderId::new(1)).is_none(),);

    let taker_remainder = matching_engine
        .book()
        .order(OrderId::new(2))
        .expect("positive remainder rests");

    assert_eq!(taker_remainder.quantity(), quantity(6));
    assert_eq!(taker_remainder.side(), Side::Buy);
    assert_eq!(taker_remainder.price(), price(100));
}

#[test]
fn multiple_makers_at_same_price_execute_fifo() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 10, Side::Sell, 100, 2);

    process(&mut matching_engine, 20, Side::Sell, 100, 3);

    process(&mut matching_engine, 30, Side::Sell, 100, 4);

    let events = process(&mut matching_engine, 40, Side::Buy, 100, 9);

    assert_eq!(events.len(), 3);

    assert_execution(&events[0], 10, 40, Side::Buy, 100, 2);

    assert_execution(&events[1], 20, 40, Side::Buy, 100, 3);

    assert_execution(&events[2], 30, 40, Side::Buy, 100, 4);

    assert!(matching_engine.book().is_empty());
}

#[test]
fn buy_sweeps_asks_from_lowest_price_upward() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 100, 2);

    process(&mut matching_engine, 2, Side::Sell, 101, 3);

    process(&mut matching_engine, 3, Side::Sell, 103, 4);

    process(&mut matching_engine, 4, Side::Sell, 105, 8);

    let events = process(&mut matching_engine, 10, Side::Buy, 105, 12);

    assert_eq!(events.len(), 4);

    assert_execution(&events[0], 1, 10, Side::Buy, 100, 2);

    assert_execution(&events[1], 2, 10, Side::Buy, 101, 3);

    assert_execution(&events[2], 3, 10, Side::Buy, 103, 4);

    assert_execution(&events[3], 4, 10, Side::Buy, 105, 3);

    let final_maker = matching_engine
        .book()
        .order(OrderId::new(4))
        .expect("last maker was only partially filled");

    assert_eq!(final_maker.quantity(), quantity(5));
}

#[test]
fn sell_sweeps_bids_from_highest_price_downward() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Buy, 105, 2);

    process(&mut matching_engine, 2, Side::Buy, 103, 3);

    process(&mut matching_engine, 3, Side::Buy, 101, 4);

    process(&mut matching_engine, 4, Side::Buy, 99, 8);

    let events = process(&mut matching_engine, 10, Side::Sell, 101, 7);

    assert_eq!(events.len(), 3);

    assert_execution(&events[0], 1, 10, Side::Sell, 105, 2);

    assert_execution(&events[1], 2, 10, Side::Sell, 103, 3);

    assert_execution(&events[2], 3, 10, Side::Sell, 101, 2);

    assert_eq!(
        matching_engine
            .book()
            .order(OrderId::new(3))
            .expect("maker remains")
            .quantity(),
        quantity(2),
    );

    assert_eq!(
        matching_engine
            .book()
            .order(OrderId::new(4))
            .expect("worse big untouched")
            .quantity(),
        quantity(8),
    );
}

#[test]
fn matching_stops_before_price_outside_buy_limit() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 99, 3);

    process(&mut matching_engine, 2, Side::Sell, 101, 7);

    let events = process(&mut matching_engine, 3, Side::Buy, 100, 5);

    assert_eq!(events.len(), 1);

    assert_execution(&events[0], 1, 3, Side::Buy, 99, 3);

    assert_eq!(
        matching_engine
            .book()
            .order(OrderId::new(2))
            .expect("ask outside limit untouched")
            .quantity(),
        quantity(7),
    );

    assert_eq!(
        matching_engine
            .book()
            .order(OrderId::new(3))
            .expect("remaining taker quantity rests")
            .quantity(),
        quantity(2),
    );
}

#[test]
fn resting_remainder_joins_back_of_same_price_fifo() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Buy, 100, 4);

    let events = process(&mut matching_engine, 2, Side::Buy, 100, 6);

    assert!(events.is_empty());

    let price_level = matching_engine
        .book()
        .level(Side::Buy, price(100))
        .expect("bid level exists");

    assert_eq!(
        price_level.order_ids().collect::<Vec<_>>(),
        vec![OrderId::new(1), OrderId::new(2)],
    );

    assert_eq!(price_level.aggregate_quantity(), quantity(10),);
}

#[test]
fn fully_filled_taker_never_enters_book() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 100, 10);

    process(&mut matching_engine, 2, Side::Buy, 100, 10);

    assert!(matching_engine.book().order(OrderId::new(2)).is_none(),);

    assert!(matching_engine.book().is_empty());
}

#[test]
fn duplicate_active_order_id_is_rejected_before_matching() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 7, Side::Sell, 100, 5);

    let events = process(&mut matching_engine, 7, Side::Buy, 100, 5);

    assert_eq!(events.len(), 1);

    let Event::OrderRejected(rejection) = events[0] else {
        panic!("expected deterministic rejection");
    };

    assert_eq!(rejection.order_id(), OrderId::new(7));

    assert_eq!(rejection.reason(), OrderRejectionReason::DuplicateOrderId,);

    let original = matching_engine
        .book()
        .order(OrderId::new(7))
        .expect("original maker remains untouched");

    assert_eq!(original.side(), Side::Sell);
    assert_eq!(original.quantity(), quantity(5));
}

#[test]
fn aggregate_overflow_rejects_without_book_mutation() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Buy, 100, i64::MAX);

    let events = process(&mut matching_engine, 2, Side::Buy, 100, 1);

    assert_eq!(events.len(), 1);

    let Event::OrderRejected(rejection) = events[0] else {
        panic!("expected deterministic rejection");
    };

    assert_eq!(
        rejection.reason(),
        OrderRejectionReason::RestingAggregateQuantityOutOfRange,
    );

    assert_eq!(matching_engine.book().len(), 1);

    assert!(!matching_engine.book().contains_order(OrderId::new(2)),);

    let price_level = matching_engine
        .book()
        .level(Side::Buy, price(100))
        .expect("original level remains");

    assert_eq!(price_level.aggregate_quantity(), quantity(i64::MAX),);

    assert_eq!(
        price_level.order_ids().collect::<Vec<_>>(),
        vec![OrderId::new(1)],
    );
}

#[test]
fn successful_processing_leaves_book_uncrossed() {
    let mut matching_engine = engine();

    process(&mut matching_engine, 1, Side::Sell, 101, 5);

    process(&mut matching_engine, 2, Side::Sell, 103, 5);

    process(&mut matching_engine, 3, Side::Buy, 102, 8);

    let best_bid = matching_engine.book().best_bid();
    let best_ask = matching_engine.book().best_ask();

    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
        assert!(bid.price() < ask.price());
    }
}

#[test]
fn replaying_same_commands_produces_same_events_and_state() {
    let commands = [
        command(1, Side::Sell, 101, 4),
        command(2, Side::Sell, 100, 3),
        command(3, Side::Buy, 101, 5),
        command(4, Side::Buy, 99, 6),
        command(5, Side::Sell, 99, 10),
        command(6, Side::Buy, 100, 2),
    ];

    let mut primary_engine = engine();
    let mut replay_engine = engine();

    let mut primary_events = Vec::new();
    let mut replay_events = Vec::new();

    for next_command in commands {
        primary_events.push(
            primary_engine
                .process(next_command)
                .expect("primary engine remains healthy"),
        );

        replay_events.push(
            replay_engine
                .process(next_command)
                .expect("replay engine remains healthy"),
        );
    }

    assert_eq!(primary_events, replay_events);
    assert_eq!(primary_engine, replay_engine);
}
