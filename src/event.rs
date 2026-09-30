use crate::domain::{MarketId, OrderId, PriceTicks, QuantityLots, Side};

/// One deterministic maker/taker execution.
///
/// The execution price is always the resting maker's price.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Execution {
    market_id: MarketId,
    maker_order_id: OrderId,
    taker_order_id: OrderId,
    taker_side: Side,
    price: PriceTicks,
    quantity: QuantityLots,
}

impl Execution {
    pub(crate) const fn new(
        market_id: MarketId,
        resting_order_id: OrderId,
        incoming_order_id: OrderId,
        taker_side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    ) -> Self {
        Self {
            market_id,
            maker_order_id: resting_order_id,
            taker_order_id: incoming_order_id,
            taker_side,
            price,
            quantity,
        }
    }

    #[must_use]
    pub const fn market_id(self) -> MarketId {
        self.market_id
    }

    #[must_use]
    pub const fn maker_order_id(self) -> OrderId {
        self.maker_order_id
    }

    #[must_use]
    pub const fn taker_order_id(self) -> OrderId {
        self.taker_order_id
    }

    #[must_use]
    pub const fn taker_side(self) -> Side {
        self.taker_side
    }

    #[must_use]
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    #[must_use]
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRejectionReason {
    DuplicateOrderId,
    RestingAggregateQuantityOutOfRange,
}

/// Deterministic business-level rejection.
///
/// This is not an [`crate::EngineError`]. The engine remained healthy and was able to deterministically process the command.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRejected {
    market_id: MarketId,
    order_id: OrderId,
    reason: OrderRejectionReason,
}

impl OrderRejected {
    pub(crate) const fn new(
        market_id: MarketId,
        order_id: OrderId,
        reason: OrderRejectionReason,
    ) -> Self {
        Self {
            market_id,
            order_id,
            reason,
        }
    }

    #[must_use]
    pub const fn market_id(self) -> MarketId {
        self.market_id
    }

    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    #[must_use]
    pub const fn reason(self) -> OrderRejectionReason {
        self.reason
    }
}

/// Ordered deterministic output produced by command processing.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Execution(Execution),
    OrderRejected(OrderRejected),
}
