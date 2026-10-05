#![allow(dead_code)]

use meow_matching_engine::{
    CancelOrder, Command, EngineConfig, MarketId, MatchingEngine, Order, OrderBook, OrderId,
    PlaceLimitOrder, PriceTicks, ProcessResult, QuantityLots, Side,
};

pub const MARKET_ID: u32 = 1;
pub const BASE_PRICE: i64 = 1_000_000;
pub const DEFAULT_QUANTITY: i64 = 1;
pub const MIXED_TRACE_LEN: usize = 4_096;
pub const HOT_TRACE_LEN: usize = 4_096;

#[derive(Debug, Clone, Copy)]
pub enum MatchShape {
    Exact,
    PartialMaker,
    PartialTaker,
}

#[derive(Debug, Clone, Copy)]
pub enum MixedProfile {
    Passive,
    Balanced,
    Aggressive,
}

impl MixedProfile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Passive => "passive",
            Self::Balanced => "balanced",
            Self::Aggressive => "aggressive",
        }
    }

    const fn thresholds(self) -> (usize, usize, usize) {
        match self {
            // place, cumulative cancel, cumulative small-match; remainder is sweep.
            Self::Passive => (65, 90, 98),
            Self::Balanced => (45, 70, 90),
            Self::Aggressive => (30, 50, 75),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IdSource {
    next: u64,
}

impl IdSource {
    pub const fn new(next: u64) -> Self {
        Self { next }
    }

    pub fn take(&mut self) -> u64 {
        let current = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("benchmark OrderId overflow");
        current
    }

    pub const fn peek(self) -> u64 {
        self.next
    }
}

pub fn engine() -> MatchingEngine {
    MatchingEngine::new(EngineConfig::new(MarketId::new(MARKET_ID)))
}

pub fn price(value: i64) -> PriceTicks {
    PriceTicks::new(value).expect("benchmark price must be positive")
}

pub fn quantity(value: i64) -> QuantityLots {
    QuantityLots::new(value).expect("benchmark quantity must be non-negative")
}

pub fn place(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Command {
    PlaceLimitOrder::new(
        OrderId::new(id),
        side,
        price(price_value),
        quantity(quantity_value),
    )
    .expect("benchmark place command must be valid")
    .into()
}

pub fn cancel(id: u64) -> Command {
    CancelOrder::new(OrderId::new(id)).into()
}

pub fn resting_order(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Order {
    Order::new(
        OrderId::new(id),
        side,
        price(price_value),
        quantity(quantity_value),
    )
    .expect("benchmark resting order must be valid")
}

pub fn process(engine: &mut MatchingEngine, command: Command) -> ProcessResult {
    engine
        .process(command)
        .expect("benchmark fixture must preserve engine invariants")
}

pub fn seed_balanced_book(
    levels_per_side: usize,
    orders_per_level: usize,
    quantity_per_order: i64,
) -> (MatchingEngine, u64) {
    let mut engine = engine();
    let mut ids = IdSource::new(1);

    for level in 0..levels_per_side {
        let level = i64::try_from(level).expect("level count fits i64");
        let bid_price = BASE_PRICE - 2 - level;
        let ask_price = BASE_PRICE + 2 + level;

        for _ in 0..orders_per_level {
            process(
                &mut engine,
                place(ids.take(), Side::Buy, bid_price, quantity_per_order),
            );
            process(
                &mut engine,
                place(ids.take(), Side::Sell, ask_price, quantity_per_order),
            );
        }
    }

    (engine, ids.peek())
}

pub fn seed_balanced_order_book(
    levels_per_side: usize,
    orders_per_level: usize,
    quantity_per_order: i64,
) -> (OrderBook, u64) {
    let mut book = OrderBook::new();
    let mut ids = IdSource::new(1);

    for level in 0..levels_per_side {
        let level = i64::try_from(level).expect("level count fits i64");
        let bid_price = BASE_PRICE - 2 - level;
        let ask_price = BASE_PRICE + 2 + level;

        for _ in 0..orders_per_level {
            book.insert(resting_order(
                ids.take(),
                Side::Buy,
                bid_price,
                quantity_per_order,
            ))
            .expect("benchmark bid fixture insertion");

            book.insert(resting_order(
                ids.take(),
                Side::Sell,
                ask_price,
                quantity_per_order,
            ))
            .expect("benchmark ask fixture insertion");
        }
    }

    (book, ids.peek())
}

pub fn insertion_fixture(
    levels_per_side: usize,
    existing_level: bool,
) -> (MatchingEngine, Command) {
    let (engine, next_id) = seed_balanced_book(levels_per_side, 4, DEFAULT_QUANTITY);

    let target_price = if existing_level {
        BASE_PRICE - 2
    } else {
        // Better than the current best bid, but still below the best ask.
        BASE_PRICE - 1
    };

    (
        engine,
        place(next_id, Side::Buy, target_price, DEFAULT_QUANTITY),
    )
}

pub fn same_level_cancel_fixture(
    order_count: usize,
    target_index: usize,
) -> (MatchingEngine, Command) {
    assert!(order_count > 0);
    assert!(target_index < order_count);

    let mut engine = engine();
    let mut ids = IdSource::new(1);
    let mut target = None;

    for index in 0..order_count {
        let id = ids.take();
        process(
            &mut engine,
            place(id, Side::Buy, BASE_PRICE - 2, DEFAULT_QUANTITY),
        );

        if index == target_index {
            target = Some(id);
        }
    }

    (engine, cancel(target.expect("target exists")))
}

pub fn same_level_book_cancel_fixture(
    order_count: usize,
    target_index: usize,
) -> (OrderBook, OrderId) {
    assert!(order_count > 0);
    assert!(target_index < order_count);

    let mut book = OrderBook::new();
    let mut target = None;

    for index in 0..order_count {
        let id = u64::try_from(index + 1).expect("order index fits u64");
        book.insert(resting_order(
            id,
            Side::Buy,
            BASE_PRICE - 2,
            DEFAULT_QUANTITY,
        ))
        .expect("benchmark same-level insertion");

        if index == target_index {
            target = Some(OrderId::new(id));
        }
    }

    (book, target.expect("target exists"))
}

pub fn deep_cancel_fixture(
    levels_per_side: usize,
    orders_per_level: usize,
    target_level: usize,
    target_position: usize,
) -> (MatchingEngine, Command) {
    assert!(levels_per_side > 0);
    assert!(target_level < levels_per_side);
    assert!(target_position < orders_per_level);

    let mut engine = engine();
    let mut ids = IdSource::new(1);
    let mut target = None;

    for level in 0..levels_per_side {
        let level_i64 = i64::try_from(level).expect("level count fits i64");
        let bid_price = BASE_PRICE - 2 - level_i64;
        let ask_price = BASE_PRICE + 2 + level_i64;

        for position in 0..orders_per_level {
            let bid_id = ids.take();
            process(
                &mut engine,
                place(bid_id, Side::Buy, bid_price, DEFAULT_QUANTITY),
            );

            if level == target_level && position == target_position {
                target = Some(bid_id);
            }

            process(
                &mut engine,
                place(ids.take(), Side::Sell, ask_price, DEFAULT_QUANTITY),
            );
        }
    }

    (engine, cancel(target.expect("target exists")))
}

pub fn one_maker_fixture(taker_side: Side, shape: MatchShape) -> (MatchingEngine, Command) {
    let mut engine = engine();
    let (maker_side, maker_price) = match taker_side {
        Side::Buy => (Side::Sell, BASE_PRICE + 2),
        Side::Sell => (Side::Buy, BASE_PRICE - 2),
    };

    process(&mut engine, place(1, maker_side, maker_price, 10));

    let taker_quantity = match shape {
        MatchShape::Exact => 10,
        MatchShape::PartialMaker => 5,
        MatchShape::PartialTaker => 20,
    };

    (engine, place(2, taker_side, maker_price, taker_quantity))
}

pub fn same_price_sweep_fixture(taker_side: Side, makers: usize) -> (MatchingEngine, Command) {
    assert!(makers > 0);

    let mut engine = engine();
    let mut ids = IdSource::new(1);
    let (maker_side, maker_price) = match taker_side {
        Side::Buy => (Side::Sell, BASE_PRICE + 2),
        Side::Sell => (Side::Buy, BASE_PRICE - 2),
    };

    for _ in 0..makers {
        process(
            &mut engine,
            place(ids.take(), maker_side, maker_price, DEFAULT_QUANTITY),
        );
    }

    let taker_quantity = i64::try_from(makers).expect("maker count fits i64");
    (
        engine,
        place(ids.take(), taker_side, maker_price, taker_quantity),
    )
}

pub fn multi_level_sweep_fixture(
    taker_side: Side,
    levels: usize,
    makers_per_level: usize,
) -> (MatchingEngine, Command) {
    assert!(levels > 0);
    assert!(makers_per_level > 0);

    let mut engine = engine();
    let mut ids = IdSource::new(1);

    for level in 0..levels {
        let level_i64 = i64::try_from(level).expect("level count fits i64");
        let maker_price = match taker_side {
            Side::Buy => BASE_PRICE + 2 + level_i64,
            Side::Sell => BASE_PRICE - 2 - level_i64,
        };
        let maker_side = match taker_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        for _ in 0..makers_per_level {
            process(
                &mut engine,
                place(ids.take(), maker_side, maker_price, DEFAULT_QUANTITY),
            );
        }
    }

    let last_level = i64::try_from(levels - 1).expect("level count fits i64");
    let limit_price = match taker_side {
        Side::Buy => BASE_PRICE + 2 + last_level,
        Side::Sell => BASE_PRICE - 2 - last_level,
    };
    let maker_count = levels
        .checked_mul(makers_per_level)
        .expect("maker count overflow");
    let taker_quantity = i64::try_from(maker_count).expect("maker count fits i64");

    (
        engine,
        place(ids.take(), taker_side, limit_price, taker_quantity),
    )
}

pub fn deep_one_maker_fixture(
    taker_side: Side,
    levels_per_side: usize,
) -> (MatchingEngine, Command) {
    let (engine, next_id) = seed_balanced_book(levels_per_side, 1, DEFAULT_QUANTITY);
    let limit = match taker_side {
        Side::Buy => BASE_PRICE + 2,
        Side::Sell => BASE_PRICE - 2,
    };

    (engine, place(next_id, taker_side, limit, DEFAULT_QUANTITY))
}

pub fn boundary_fixture(
    taker_side: Side,
    levels_per_side: usize,
    crosses_at_equality: bool,
) -> (MatchingEngine, Command) {
    let (engine, next_id) = seed_balanced_book(levels_per_side, 1, DEFAULT_QUANTITY);

    let limit = match (taker_side, crosses_at_equality) {
        (Side::Buy, true) => BASE_PRICE + 2,
        (Side::Buy, false) => BASE_PRICE + 1,
        (Side::Sell, true) => BASE_PRICE - 2,
        (Side::Sell, false) => BASE_PRICE - 1,
    };

    (engine, place(next_id, taker_side, limit, DEFAULT_QUANTITY))
}

pub fn partial_final_maker_fixture(taker_side: Side, makers: usize) -> (MatchingEngine, Command) {
    assert!(makers >= 2);

    let mut engine = engine();
    let mut ids = IdSource::new(1);
    let maker_side = match taker_side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    };

    for level in 0..makers {
        let level_i64 = i64::try_from(level).expect("maker count fits i64");
        let maker_price = match taker_side {
            Side::Buy => BASE_PRICE + 2 + level_i64,
            Side::Sell => BASE_PRICE - 2 - level_i64,
        };
        process(&mut engine, place(ids.take(), maker_side, maker_price, 10));
    }

    let last_level = i64::try_from(makers - 1).expect("maker count fits i64");
    let limit = match taker_side {
        Side::Buy => BASE_PRICE + 2 + last_level,
        Side::Sell => BASE_PRICE - 2 - last_level,
    };
    let full_before_last = i64::try_from(makers - 1).expect("maker count fits i64");
    let taker_quantity = full_before_last
        .checked_mul(10)
        .and_then(|value| value.checked_add(5))
        .expect("benchmark taker quantity overflow");

    (engine, place(ids.take(), taker_side, limit, taker_quantity))
}

pub fn event_count_fixture(event_count: usize) -> (MatchingEngine, Command) {
    if event_count == 0 {
        return boundary_fixture(Side::Buy, 16, false);
    }

    same_price_sweep_fixture(Side::Buy, event_count)
}

pub fn partial_cancel_fixture() -> (MatchingEngine, Command) {
    let mut engine = engine();

    process(&mut engine, place(1, Side::Buy, BASE_PRICE, 100));
    process(&mut engine, place(2, Side::Sell, BASE_PRICE, 40));

    (engine, cancel(1))
}

pub fn terminal_cancel_fixture(kind: &'static str) -> (MatchingEngine, Command) {
    match kind {
        "unknown" => (engine(), cancel(999_999)),
        "filled" => {
            let mut engine = engine();
            process(&mut engine, place(1, Side::Buy, BASE_PRICE, 5));
            process(&mut engine, place(2, Side::Sell, BASE_PRICE, 5));
            (engine, cancel(1))
        }
        "cancelled" => {
            let mut engine = engine();
            process(&mut engine, place(1, Side::Buy, BASE_PRICE, 5));
            process(&mut engine, cancel(1));
            (engine, cancel(1))
        }
        "rejected" => {
            let mut engine = engine();
            process(&mut engine, place(1, Side::Buy, BASE_PRICE, i64::MAX));
            process(&mut engine, place(2, Side::Buy, BASE_PRICE, 1));
            (engine, cancel(2))
        }
        other => panic!("unknown terminal cancel fixture: {other}"),
    }
}

pub fn build_terminal_history(entries: usize) -> MatchingEngine {
    let mut engine = engine();
    let mut ids = IdSource::new(1);

    for _ in 0..entries {
        let id = ids.take();
        process(
            &mut engine,
            place(id, Side::Buy, BASE_PRICE - 100, DEFAULT_QUANTITY),
        );
        process(&mut engine, cancel(id));
    }

    assert!(engine.book().is_empty());
    engine
}

pub fn build_mixed_workload(
    profile: MixedProfile,
    operation_count: usize,
) -> (MatchingEngine, Vec<Command>) {
    let central_levels = 256;
    let makers_per_level = 64;
    let (engine, next_id) = seed_balanced_book(central_levels, makers_per_level, 1);
    let mut ids = IdSource::new(next_id);
    let mut commands = Vec::with_capacity(operation_count);
    let mut cancellable = std::collections::VecDeque::new();
    let (place_end, cancel_end, small_match_end) = profile.thresholds();
    let mut passive_side = Side::Buy;
    let mut taker_side = Side::Buy;

    for index in 0..operation_count {
        let slot = index % 100;

        if slot < place_end {
            let id = ids.take();
            let px = match passive_side {
                Side::Buy => BASE_PRICE - 100_000,
                Side::Sell => BASE_PRICE + 100_000,
            };
            commands.push(place(id, passive_side, px, 1));
            cancellable.push_back(id);
            passive_side = opposite(passive_side);
        } else if slot < cancel_end {
            let id = cancellable
                .pop_front()
                .expect("profile must place enough cancellable orders");
            commands.push(cancel(id));
        } else if slot < small_match_end {
            let px = aggressive_limit(taker_side);
            commands.push(place(ids.take(), taker_side, px, 2));
            taker_side = opposite(taker_side);
        } else {
            let px = aggressive_limit(taker_side);
            commands.push(place(ids.take(), taker_side, px, 16));
            taker_side = opposite(taker_side);
        }
    }

    (engine, commands)
}

pub fn build_hot_churn_workload(operation_count: usize) -> (MatchingEngine, Vec<Command>) {
    let mut engine = engine();
    let mut ids = IdSource::new(1);

    // 80% of seed liquidity is concentrated in five levels around top of book.
    for level in 0..5 {
        let level_i64 = i64::from(level);
        for _ in 0..1_024 {
            process(
                &mut engine,
                place(ids.take(), Side::Buy, BASE_PRICE - 2 - level_i64, 1),
            );
            process(
                &mut engine,
                place(ids.take(), Side::Sell, BASE_PRICE + 2 + level_i64, 1),
            );
        }
    }

    // Remaining 20% is deliberately spread over deeper price levels.
    for level in 0..64 {
        let level_i64 = i64::from(level);
        for _ in 0..20 {
            process(
                &mut engine,
                place(ids.take(), Side::Buy, BASE_PRICE - 100 - level_i64, 1),
            );
            process(
                &mut engine,
                place(ids.take(), Side::Sell, BASE_PRICE + 100 + level_i64, 1),
            );
        }
    }

    let mut commands = Vec::with_capacity(operation_count);
    let mut cancellable = std::collections::VecDeque::new();
    let mut place_side = Side::Buy;
    let mut taker_side = Side::Buy;

    for index in 0..operation_count {
        match index % 10 {
            0..=3 => {
                let id = ids.take();
                let px = match place_side {
                    Side::Buy => BASE_PRICE - 2,
                    Side::Sell => BASE_PRICE + 2,
                };
                commands.push(place(id, place_side, px, 1));
                cancellable.push_back(id);
                place_side = opposite(place_side);
            }
            4..=6 => {
                let id = cancellable
                    .pop_front()
                    .expect("hot churn must create cancellable orders first");
                commands.push(cancel(id));
            }
            _ => {
                let px = match taker_side {
                    Side::Buy => BASE_PRICE + 2,
                    Side::Sell => BASE_PRICE - 2,
                };
                commands.push(place(ids.take(), taker_side, px, 1));
                taker_side = opposite(taker_side);
            }
        }
    }

    (engine, commands)
}

pub fn execute_trace(engine: &mut MatchingEngine, commands: Vec<Command>) -> usize {
    let mut event_count = 0usize;

    for command in commands {
        let result = process(engine, command);
        event_count = event_count
            .checked_add(result.events().len())
            .expect("benchmark event count overflow");
    }

    event_count
}

pub const fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

pub const fn aggressive_limit(side: Side) -> i64 {
    match side {
        Side::Buy => BASE_PRICE + 10_000,
        Side::Sell => BASE_PRICE - 10_000,
    }
}
