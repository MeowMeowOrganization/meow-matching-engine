use meow_matching_engine::{
    CancelOrder, CancelRejectionReason, Command, EngineConfig, Event, MarketId, MatchingEngine,
    OrderId, OrderRejectionReason, OrderState, PlaceLimitOrder, PriceTicks, QuantityLots, Side,
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

fn place(
    matching_engine: &mut MatchingEngine,
    id: u64,
    side: Side,
    price_value: i64,
    quantity_value: i64,
) -> Vec<Event> {
    matching_engine
        .process(place_command(id, side, price_value, quantity_value))
        .expect("engine remains healthy")
        .into_domain_events()
}

fn cancel(matching_engine: &mut MatchingEngine, id: u64) -> Vec<Event> {
    matching_engine
        .process(cancel_command(id))
        .expect("engine remains healthy")
        .into_domain_events()
}

fn assert_cancelled(event: &Event, expected_id: u64, expected_quantity: i64) {
    let Event::OrderCancelled(cancelled) = event else {
        panic!("expected OrderCancelled");
    };

    assert_eq!(cancelled.market_id(), MarketId::new(1));
    assert_eq!(cancelled.order_id(), OrderId::new(expected_id),);
    assert_eq!(cancelled.cancelled_quantity(), quantity(expected_quantity),);
}

fn assert_cancel_rejected(event: &Event, expected_id: u64, expected_reason: CancelRejectionReason) {
    let Event::CancelRejected(rejection) = event else {
        panic!("expected CancelRejected");
    };

    assert_eq!(rejection.market_id(), MarketId::new(1));
    assert_eq!(rejection.order_id(), OrderId::new(expected_id),);
    assert_eq!(rejection.reason(), expected_reason);
}

#[test]
fn placement_drives_open_partial_and_filled_states() {
    let mut matching_engine = engine();

    // NEW -> OPEN
    let events = place(&mut matching_engine, 1, Side::Buy, 100, 10);

    assert!(events.is_empty());

    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("lifecycle exists");

    assert_eq!(lifecycle.state(), OrderState::Open);
    assert_eq!(lifecycle.submitted_quantity(), quantity(10),);
    assert_eq!(lifecycle.executed_quantity(), QuantityLots::ZERO,);

    // OPEN -> PARTIALLY_FILLED.
    //
    // Incoming sell 2 fully fills against four lots of maker 1.
    let events = place(&mut matching_engine, 2, Side::Sell, 100, 4);

    assert_eq!(events.len(), 1);

    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("maker lifecycle exists");

    assert_eq!(lifecycle.state(), OrderState::PartiallyFilled,);
    assert_eq!(lifecycle.executed_quantity(), quantity(4),);

    assert_eq!(
        matching_engine
            .book()
            .order(OrderId::new(1))
            .expect("maker remains")
            .quantity(),
        quantity(6),
    );

    let taker = matching_engine
        .order_lifecycle(OrderId::new(2))
        .expect("taker lifecycle exists");

    assert_eq!(taker.state(), OrderState::Filled);
    assert_eq!(taker.executed_quantity(), quantity(4),);

    // PARTIALLY_FILLED -> FILLED.
    let events = place(&mut matching_engine, 3, Side::Sell, 100, 6);

    assert_eq!(events.len(), 1);

    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("maker lifecycle retained");

    assert_eq!(lifecycle.state(), OrderState::Filled);
    assert_eq!(lifecycle.executed_quantity(), quantity(10),);

    assert!(matching_engine.book().order(OrderId::new(1)).is_none(),);
}

#[test]
fn aggregate_overflow_creates_rejected_lifecycle() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Buy, 100, i64::MAX);

    let events = place(&mut matching_engine, 2, Side::Buy, 100, 1);

    assert_eq!(events.len(), 1);

    let Event::OrderRejected(rejection) = events[0] else {
        panic!("expected placement rejection");
    };

    assert_eq!(
        rejection.reason(),
        OrderRejectionReason::RestingAggregateQuantityOutOfRange,
    );

    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(2))
        .expect("rejected lifecycle retained");

    assert_eq!(lifecycle.state(), OrderState::Rejected,);
    assert_eq!(lifecycle.executed_quantity(), QuantityLots::ZERO,);

    assert!(matching_engine.book().order(OrderId::new(2)).is_none(),);
}

#[test]
fn cancelling_open_order_removes_full_remainder() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Buy, 100, 10);

    let events = cancel(&mut matching_engine, 1);

    assert_eq!(events.len(), 1);
    assert_cancelled(&events[0], 1, 10);

    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("terminal lifecycle retained");

    assert_eq!(lifecycle.state(), OrderState::Cancelled,);
    assert_eq!(lifecycle.executed_quantity(), QuantityLots::ZERO,);

    assert!(matching_engine.book().order(OrderId::new(1)).is_none(),);

    assert_eq!(matching_engine.book().bid_level_count(), 0,);
}

#[test]
fn cancelling_partially_filled_order_cancels_only_remainder() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Sell, 100, 10);

    // Maker 1 executes four and retains six.
    place(&mut matching_engine, 2, Side::Buy, 100, 4);

    let before_cancel = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("maker lifecycle exists");

    assert_eq!(before_cancel.state(), OrderState::PartiallyFilled,);
    assert_eq!(before_cancel.executed_quantity(), quantity(4),);

    let events = cancel(&mut matching_engine, 1);

    assert_eq!(events.len(), 1);
    assert_cancelled(&events[0], 1, 6);

    let after_cancel = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("terminal lifecycle retained");

    assert_eq!(after_cancel.state(), OrderState::Cancelled,);

    // Cancellation never reverses previous executions.
    assert_eq!(after_cancel.executed_quantity(), quantity(4),);

    assert!(matching_engine.book().order(OrderId::new(1)).is_none(),);
}

#[test]
fn cancelling_filled_order_is_rejected_without_mutation() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Sell, 100, 5);

    place(&mut matching_engine, 2, Side::Buy, 100, 5);

    let before = *matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("filled lifecycle exists");

    assert_eq!(before.state(), OrderState::Filled);

    let events = cancel(&mut matching_engine, 1);

    assert_eq!(events.len(), 1);

    assert_cancel_rejected(&events[0], 1, CancelRejectionReason::AlreadyFilled);

    let after = *matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("filled lifecycle remains");

    assert_eq!(before, after);
    assert!(matching_engine.book().is_empty());
}

#[test]
fn cancelling_already_cancelled_order_is_rejected() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Buy, 100, 5);

    cancel(&mut matching_engine, 1);

    let events = cancel(&mut matching_engine, 1);

    assert_eq!(events.len(), 1);

    assert_cancel_rejected(&events[0], 1, CancelRejectionReason::AlreadyCancelled);

    assert_eq!(
        matching_engine
            .order_lifecycle(OrderId::new(1))
            .expect("lifecycle retained")
            .state(),
        OrderState::Cancelled,
    );
}

#[test]
fn cancelling_unknown_order_is_normal_business_rejection() {
    let mut matching_engine = engine();

    let events = cancel(&mut matching_engine, 999);

    assert_eq!(events.len(), 1);

    assert_cancel_rejected(&events[0], 999, CancelRejectionReason::UnknownOrder);

    assert!(matching_engine.order_lifecycle(OrderId::new(999)).is_none(),);

    assert!(matching_engine.book().is_empty());
}

#[test]
fn cancelling_rejected_order_is_rejected_as_already_rejected() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Buy, 100, i64::MAX);

    place(&mut matching_engine, 2, Side::Buy, 100, 1);

    assert_eq!(
        matching_engine
            .order_lifecycle(OrderId::new(2))
            .expect("rejected lifecycle exists")
            .state(),
        OrderState::Rejected,
    );

    let events = cancel(&mut matching_engine, 2);

    assert_eq!(events.len(), 1);

    assert_cancel_rejected(&events[0], 2, CancelRejectionReason::AlreadyRejected);

    assert_eq!(
        matching_engine
            .order_lifecycle(OrderId::new(2))
            .expect("rejected lifecycle retained")
            .state(),
        OrderState::Rejected,
    );
}

#[test]
fn cancellation_preserves_fifo_of_surviving_orders() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Buy, 100, 3);

    place(&mut matching_engine, 2, Side::Buy, 100, 4);

    place(&mut matching_engine, 3, Side::Buy, 100, 5);

    let events = cancel(&mut matching_engine, 2);

    assert_eq!(events.len(), 1);
    assert_cancelled(&events[0], 2, 4);

    let level = matching_engine
        .book()
        .level(Side::Buy, price(100))
        .expect("price level remains");

    assert_eq!(
        level.order_ids().collect::<Vec<_>>(),
        vec![OrderId::new(1), OrderId::new(3),],
    );

    assert_eq!(level.aggregate_quantity(), quantity(8),);
}

#[test]
fn cancelling_final_order_removes_empty_price_level() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Sell, 100, 5);

    cancel(&mut matching_engine, 1);

    assert!(matching_engine.book().best_ask().is_none());

    assert_eq!(matching_engine.book().ask_level_count(), 0,);

    assert!(matching_engine.book().is_empty());
}

#[test]
fn terminal_order_id_cannot_be_reused() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 7, Side::Buy, 100, 5);

    cancel(&mut matching_engine, 7);

    assert_eq!(
        matching_engine
            .order_lifecycle(OrderId::new(7))
            .expect("terminal lifecycle exists")
            .state(),
        OrderState::Cancelled,
    );

    let events = place(&mut matching_engine, 7, Side::Sell, 200, 10);

    assert_eq!(events.len(), 1);

    let Event::OrderRejected(rejection) = events[0] else {
        panic!("expected duplicate-id rejection");
    };

    assert_eq!(rejection.reason(), OrderRejectionReason::DuplicateOrderId,);

    // The rejected duplicate command does not overwrite the original
    // order's lifecycle.
    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(7))
        .expect("original lifecycle retained");

    assert_eq!(lifecycle.state(), OrderState::Cancelled,);
    assert_eq!(lifecycle.submitted_quantity(), quantity(5),);
}

#[test]
fn active_lifecycle_quantity_matches_book_remainder() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Sell, 100, 20);

    place(&mut matching_engine, 2, Side::Buy, 100, 7);

    let lifecycle = matching_engine
        .order_lifecycle(OrderId::new(1))
        .expect("lifecycle exists");

    let resting = matching_engine
        .book()
        .order(OrderId::new(1))
        .expect("partial maker remains");

    assert_eq!(lifecycle.state(), OrderState::PartiallyFilled,);

    assert_eq!(
        lifecycle.submitted_quantity().get(),
        lifecycle.executed_quantity().get() + resting.quantity().get(),
    );

    assert_eq!(resting.quantity(), quantity(13),);
}

#[test]
fn no_completed_command_leaves_new_lifecycle_state() {
    let mut matching_engine = engine();

    place(&mut matching_engine, 1, Side::Buy, 100, 5);

    assert_ne!(
        matching_engine
            .order_lifecycle(OrderId::new(1))
            .expect("lifecycle exists")
            .state(),
        OrderState::New,
    );

    place(&mut matching_engine, 2, Side::Sell, 100, 5);

    assert_ne!(
        matching_engine
            .order_lifecycle(OrderId::new(2))
            .expect("lifecycle exists")
            .state(),
        OrderState::New,
    );
}

#[test]
fn replay_with_cancellations_is_deterministic() {
    let commands = [
        place_command(1, Side::Sell, 100, 10),
        place_command(2, Side::Buy, 100, 4),
        cancel_command(1),
        cancel_command(1),
        place_command(3, Side::Buy, 99, 8),
        cancel_command(999),
        cancel_command(3),
    ];

    let mut primary_engine = engine();
    let mut replay_engine = engine();

    let mut primary_events = Vec::new();
    let mut replay_events = Vec::new();

    for command in commands {
        primary_events.push(
            primary_engine
                .process(command)
                .expect("primary engine remains healthy"),
        );

        replay_events.push(
            replay_engine
                .process(command)
                .expect("replay engine remains healthy"),
        );
    }

    assert_eq!(primary_events, replay_events);
    assert_eq!(primary_engine, replay_engine);
}
