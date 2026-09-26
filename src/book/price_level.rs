use std::collections::VecDeque;

use crate::domain::{OrderId, PriceTicks, QuantityLots};

use super::error::OrderBookError;

/// All resting orders at exactly one price.
///
/// Order IDs are stored from oldest to newest. The front of the queue therefore owns time priority within this price level.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceLevel {
    price: PriceTicks,
    order_ids: VecDeque<OrderId>,
    aggregate_quantity: QuantityLots,
}

impl PriceLevel {
    pub(crate) fn new(price: PriceTicks) -> Self {
        Self {
            price,
            order_ids: VecDeque::new(),
            aggregate_quantity: QuantityLots::ZERO,
        }
    }

    #[must_use]
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    #[must_use]
    pub const fn aggregate_quantity(&self) -> QuantityLots {
        self.aggregate_quantity
    }

    #[must_use]
    pub fn order_count(&self) -> usize {
        self.order_ids.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order_ids.is_empty()
    }

    #[must_use]
    pub fn front_order_id(&self) -> Option<OrderId> {
        self.order_ids.front().copied()
    }

    #[must_use]
    pub fn order_ids(&self) -> impl ExactSizeIterator<Item = OrderId> + '_ {
        self.order_ids.iter().copied()
    }

    pub(crate) fn push_back(
        &mut self,
        order_id: OrderId,
        quantity: QuantityLots,
    ) -> Result<(), OrderBookError> {
        if quantity.is_zero() {
            return Err(OrderBookError::InconsistentState(order_id));
        }

        let aggregate = i128::from(self.aggregate_quantity.get())
            .checked_add(i128::from(quantity.get()))
            .ok_or(OrderBookError::AggregateQuantityOutOfRange { price: self.price })?;

        let aggregate = i64::try_from(aggregate)
            .map_err(|_| OrderBookError::AggregateQuantityOutOfRange { price: self.price })?;

        let aggregate = QuantityLots::new(aggregate)
            .map_err(|_| OrderBookError::InconsistentState(order_id))?;

        self.order_ids.push_back(order_id);
        self.aggregate_quantity = aggregate;

        Ok(())
    }

    pub(crate) fn remove(
        &mut self,
        order_id: OrderId,
        quantity: QuantityLots,
    ) -> Result<(), OrderBookError> {
        let Some(index) = self
            .order_ids
            .iter()
            .position(|candidate| *candidate == order_id)
        else {
            return Err(OrderBookError::InconsistentState(order_id));
        };

        let aggregate = i128::from(self.aggregate_quantity.get())
            .checked_sub(i128::from(quantity.get()))
            .ok_or(OrderBookError::InconsistentState(order_id))?;

        let aggregate =
            i64::try_from(aggregate).map_err(|_| OrderBookError::InconsistentState(order_id))?;

        let aggregate = QuantityLots::new(aggregate)
            .map_err(|_| OrderBookError::InconsistentState(order_id))?;

        let removed = self.order_ids.remove(index);

        if removed != Some(order_id) {
            return Err(OrderBookError::InconsistentState(order_id));
        }

        self.aggregate_quantity = aggregate;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price() -> PriceTicks {
        PriceTicks::new(100).expect("positive price")
    }

    fn quantity(value: i64) -> QuantityLots {
        QuantityLots::new(value).expect("non-negative quantity")
    }

    #[test]
    fn orders_are_appended_in_fifo_order() {
        let mut level = PriceLevel::new(price());

        level
            .push_back(OrderId::new(10), quantity(5))
            .expect("first insertion");

        level
            .push_back(OrderId::new(20), quantity(7))
            .expect("second insertion");

        level
            .push_back(OrderId::new(30), quantity(9))
            .expect("third insertion");

        assert_eq!(
            level.order_ids().collect::<Vec<_>>(),
            vec![OrderId::new(10), OrderId::new(20), OrderId::new(30)],
        );

        assert_eq!(level.front_order_id(), Some(OrderId::new(10)));
        assert_eq!(level.aggregate_quantity(), quantity(21));
    }

    #[test]
    fn removing_middle_order_preserves_relative_fifo_order() {
        let mut level = PriceLevel::new(price());

        level
            .push_back(OrderId::new(10), quantity(5))
            .expect("first insertion");

        level
            .push_back(OrderId::new(20), quantity(7))
            .expect("second insertion");

        level
            .push_back(OrderId::new(30), quantity(9))
            .expect("third insertion");

        level
            .remove(OrderId::new(20), quantity(7))
            .expect("middle order exists");

        assert_eq!(
            level.order_ids().collect::<Vec<_>>(),
            vec![OrderId::new(10), OrderId::new(30)],
        );

        assert_eq!(level.aggregate_quantity(), quantity(14));
    }
}
