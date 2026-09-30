use std::cmp::min;

use crate::{
    book::{OrderBook, PriceLevel},
    command::PlaceLimitOrder,
    domain::{OrderId, PriceTicks, QuantityLots, Side},
    error::EngineError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlannedExecution {
    maker_order_id: OrderId,
    price: PriceTicks,
    quantity: QuantityLots,
}

impl PlannedExecution {
    #[must_use]
    pub(crate) const fn maker_order_id(self) -> OrderId {
        self.maker_order_id
    }

    #[must_use]
    pub(crate) const fn price(self) -> PriceTicks {
        self.price
    }

    #[must_use]
    pub(crate) const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionPlan {
    fills: Vec<PlannedExecution>,
    remaining_quantity: QuantityLots,
}

impl ExecutionPlan {
    #[must_use]
    pub(crate) fn fills(&self) -> &[PlannedExecution] {
        &self.fills
    }

    #[must_use]
    pub(crate) fn remaining_quantity(&self) -> QuantityLots {
        self.remaining_quantity
    }

    #[must_use]
    pub(crate) fn into_fills(self) -> Vec<PlannedExecution> {
        self.fills
    }
}

pub(crate) fn plan_limit_order(
    book: &OrderBook,
    command: PlaceLimitOrder,
) -> Result<ExecutionPlan, EngineError> {
    match command.side() {
        Side::Buy => plan_buy(book, command),
        Side::Sell => plan_sell(book, command),
    }
}

fn plan_buy(book: &OrderBook, command: PlaceLimitOrder) -> Result<ExecutionPlan, EngineError> {
    let mut remaining_quantity = command.quantity();
    let mut fills = Vec::new();

    for price_level in book.ask_levels() {
        if price_level.price() > command.price() {
            break;
        }

        plan_price_level(
            book,
            price_level,
            command.side(),
            &mut remaining_quantity,
            &mut fills,
        )?;

        if remaining_quantity.is_zero() {
            break;
        }
    }

    Ok(ExecutionPlan {
        fills,
        remaining_quantity,
    })
}

fn plan_sell(book: &OrderBook, command: PlaceLimitOrder) -> Result<ExecutionPlan, EngineError> {
    let mut remaining_quantity = command.quantity();
    let mut fills = Vec::new();

    for price_level in book.bid_levels() {
        if price_level.price() < command.price() {
            break;
        }

        plan_price_level(
            book,
            price_level,
            command.side(),
            &mut remaining_quantity,
            &mut fills,
        )?;

        if remaining_quantity.is_zero() {
            break;
        }
    }

    Ok(ExecutionPlan {
        fills,
        remaining_quantity,
    })
}

fn plan_price_level(
    book: &OrderBook,
    price_level: &PriceLevel,
    incoming_side: Side,
    remaining_quantity: &mut QuantityLots,
    fills: &mut Vec<PlannedExecution>,
) -> Result<(), EngineError> {
    if price_level.is_empty() || price_level.aggregate_quantity().is_zero() {
        return Err(EngineError::OrderBookInvariantViolation);
    }

    for order_id in price_level.order_ids() {
        let maker_order = book
            .order(order_id)
            .ok_or(EngineError::OrderBookInvariantViolation)?;

        if maker_order.side() == incoming_side
            || maker_order.price() != price_level.price()
            || maker_order.quantity().is_zero()
        {
            return Err(EngineError::OrderBookInvariantViolation);
        }

        let execution_quantity = min(*remaining_quantity, maker_order.quantity());

        if execution_quantity.is_zero() {
            return Err(EngineError::OrderBookInvariantViolation);
        }

        fills.push(PlannedExecution {
            maker_order_id: order_id,
            price: maker_order.price(),
            quantity: execution_quantity,
        });

        *remaining_quantity = subtract_quantity(*remaining_quantity, execution_quantity)?;

        if remaining_quantity.is_zero() {
            break;
        }
    }

    Ok(())
}

fn subtract_quantity(
    current: QuantityLots,
    executed: QuantityLots,
) -> Result<QuantityLots, EngineError> {
    let remaining = i128::from(current.get())
        .checked_sub(i128::from(executed.get()))
        .ok_or(EngineError::ArithmeticOverflow)?;

    let remaining = i64::try_from(remaining).map_err(|_| EngineError::CanonicalIntegerOutRange)?;

    QuantityLots::new(remaining).map_err(|_| EngineError::OrderBookInvariantViolation)
}
