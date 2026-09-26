use std::collections::{
    BTreeMap, HashMap, btree_map::Entry as BTreeEntry, hash_map::Entry as HashEntry,
};

use crate::domain::{OrderId, PriceTicks, QuantityLots, Side};

use super::{error::OrderBookError, order::Order, price_level::PriceLevel};

/// In-memory limit order book for exactly one market.
///
/// Price levels are deterministically ordered with `BTreeMap`.
///
/// `HashMap` exists only as an order-ID lookup index. Its iteration order must never be used for matching priority or deterministic output.

#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<PriceTicks, PriceLevel>,
    asks: BTreeMap<PriceTicks, PriceLevel>,
    orders: HashMap<OrderId, Order>,
}

impl OrderBook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts one resting order.
    ///
    /// Orders joining an existing price level are appended to the back of that level's FIFO queue.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookError::DuplicateOrderId`] when the ID already exists.
    ///
    /// Returns [`OrderBookError::AggregateQuantityOutOfRange`] when adding the quantity would make the price-level aggregate exceed canonical `i64`.
    pub fn insert(&mut self, order: Order) -> Result<(), OrderBookError> {
        let order_id = order.id();
        let side = order.side();
        let price = order.price();
        let quantity = order.quantity();

        let order_entry = match self.orders.entry(order_id) {
            HashEntry::Occupied(_) => {
                return Err(OrderBookError::DuplicateOrderId(order_id));
            }

            HashEntry::Vacant(entry) => entry,
        };

        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        match levels.entry(price) {
            BTreeEntry::Occupied(mut entry) => {
                entry.get_mut().push_back(order_id, quantity)?;
            }

            BTreeEntry::Vacant(entry) => {
                let mut level = PriceLevel::new(price);
                level.push_back(order_id, quantity)?;
                entry.insert(level);
            }
        }

        order_entry.insert(order);

        Ok(())
    }

    /// Removes an order by ID.
    ///
    /// Returns `Ok(None)` when the order ID is not present.
    ///
    /// Empty price levels are removed automatically.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookError::InconsistentState`] if the ID index and price levels disagree.
    /// Such a result indicates an internal engine bug rather than an ordinary unknown-order condition.
    pub fn remove(&mut self, order_id: OrderId) -> Result<Option<Order>, OrderBookError> {
        let Some(order) = self.orders.get(&order_id) else {
            return Ok(None);
        };

        let side = order.side();
        let price = order.price();
        let quantity = order.quantity();

        let level_is_empty = {
            let levels = self.levels_mut(side);

            let level = levels
                .get_mut(&price)
                .ok_or(OrderBookError::InconsistentState(order_id))?;

            level.remove(order_id, quantity)?;

            level.is_empty()
        };

        if level_is_empty {
            let removed_level = self.levels_mut(side).remove(&price);

            if removed_level.is_none() {
                return Err(OrderBookError::InconsistentState(order_id));
            }
        }

        let removed_order = self
            .orders
            .remove(&order_id)
            .ok_or(OrderBookError::InconsistentState(order_id))?;

        Ok(Some(removed_order))
    }

    #[must_use]
    pub fn order(&self, order_id: OrderId) -> Option<&Order> {
        self.orders.get(&order_id)
    }

    #[must_use]
    pub fn contains_order(&self, order_id: OrderId) -> bool {
        self.orders.contains_key(&order_id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    #[must_use]
    pub fn bid_level_count(&self) -> usize {
        self.bids.len()
    }

    #[must_use]
    pub fn ask_level_count(&self) -> usize {
        self.asks.len()
    }

    /// Returns the highest bid price level.
    #[must_use]
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.last_key_value().map(|(_, level)| level)
    }

    /// Returns the lowest ask price level.
    #[must_use]
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first_key_value().map(|(_, level)| level)
    }

    #[must_use]
    pub fn level(&self, side: Side, price: PriceTicks) -> Option<&PriceLevel> {
        self.levels(side).get(&price)
    }

    #[must_use]
    pub fn aggregate_quantity(&self, side: Side, price: PriceTicks) -> QuantityLots {
        self.level(side, price)
            .map_or(QuantityLots::ZERO, PriceLevel::aggregate_quantity)
    }

    /// Bid levels from best price to worst price.
    pub fn bid_levels(&self) -> impl Iterator<Item = &PriceLevel> {
        self.bids.values().rev()
    }

    /// Ask levels from best price to worst price.
    pub fn ask_levels(&self) -> impl Iterator<Item = &PriceLevel> {
        self.asks.values()
    }

    fn levels(&self, side: Side) -> &BTreeMap<PriceTicks, PriceLevel> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<PriceTicks, PriceLevel> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(value: i64) -> PriceTicks {
        PriceTicks::new(value).expect("positive price")
    }

    fn quantity(value: i64) -> QuantityLots {
        QuantityLots::new(value).expect("non-negative quantity")
    }

    fn order(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Order {
        Order::new(
            OrderId::new(id),
            side,
            price(price_value),
            quantity(quantity_value),
        )
        .expect("valid test order")
    }

    #[test]
    fn order_can_be_looked_up_by_id() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Buy, 100, 5))
            .expect("insert succeeds");

        let resting = book.order(OrderId::new(1)).expect("order exists");

        assert_eq!(resting.price(), price(100));
        assert_eq!(resting.quantity(), quantity(5));
    }

    #[test]
    fn best_bid_is_highest_bid() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Buy, 100, 1)).expect("insert");

        book.insert(order(2, Side::Buy, 105, 1)).expect("insert");

        book.insert(order(3, Side::Buy, 102, 1)).expect("insert");

        assert_eq!(book.best_bid().map(PriceLevel::price), Some(price(105)),);
    }

    #[test]
    fn best_ask_is_lowest_ask() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Sell, 110, 1)).expect("insert");

        book.insert(order(2, Side::Sell, 105, 1)).expect("insert");

        book.insert(order(3, Side::Sell, 108, 1)).expect("insert");

        assert_eq!(book.best_ask().map(PriceLevel::price), Some(price(105)),);
    }

    #[test]
    fn orders_at_same_price_remain_fifo() {
        let mut book = OrderBook::new();

        book.insert(order(10, Side::Buy, 100, 3))
            .expect("first order");

        book.insert(order(20, Side::Buy, 100, 4))
            .expect("second order");

        book.insert(order(30, Side::Buy, 100, 5))
            .expect("third order");

        let level = book.level(Side::Buy, price(100)).expect("level exists");

        assert_eq!(
            level.order_ids().collect::<Vec<_>>(),
            vec![OrderId::new(10), OrderId::new(20), OrderId::new(30),],
        );

        assert_eq!(level.aggregate_quantity(), quantity(12));
    }

    #[test]
    fn removing_middle_order_preserves_fifo_of_survivors() {
        let mut book = OrderBook::new();

        book.insert(order(10, Side::Buy, 100, 3)).expect("first");

        book.insert(order(20, Side::Buy, 100, 4)).expect("second");

        book.insert(order(30, Side::Buy, 100, 5)).expect("third");

        let removed = book
            .remove(OrderId::new(20))
            .expect("book remains consistent")
            .expect("order exists");

        assert_eq!(removed.id(), OrderId::new(20));

        let level = book.level(Side::Buy, price(100)).expect("level remains");

        assert_eq!(
            level.order_ids().collect::<Vec<_>>(),
            vec![OrderId::new(10), OrderId::new(30)],
        );

        assert_eq!(level.aggregate_quantity(), quantity(8));
    }

    #[test]
    fn removing_last_order_removes_empty_price_level() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Sell, 100, 5)).expect("insert");

        book.remove(OrderId::new(1))
            .expect("book remains consistent")
            .expect("order exists");

        assert!(book.best_ask().is_none());
        assert_eq!(book.ask_level_count(), 0);
        assert!(book.is_empty());
    }

    #[test]
    fn unknown_order_removal_is_not_an_error() {
        let mut book = OrderBook::new();

        let removed = book
            .remove(OrderId::new(999))
            .expect("unknown id is a normal lookup miss");

        assert!(removed.is_none());
    }

    #[test]
    fn duplicate_order_id_is_rejected_without_mutation() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Buy, 100, 5))
            .expect("initial insert");

        let result = book.insert(order(1, Side::Sell, 200, 10));

        assert_eq!(
            result,
            Err(OrderBookError::DuplicateOrderId(OrderId::new(1))),
        );

        assert_eq!(book.len(), 1);
        assert_eq!(book.bid_level_count(), 1);
        assert_eq!(book.ask_level_count(), 0);

        let original = book.order(OrderId::new(1)).expect("original remains");

        assert_eq!(original.side(), Side::Buy);
        assert_eq!(original.price(), price(100));
    }

    #[test]
    fn bids_and_asks_at_same_numeric_price_are_independent() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Buy, 100, 5)).expect("bid");

        book.insert(order(2, Side::Sell, 100, 7)).expect("ask");

        assert_eq!(book.aggregate_quantity(Side::Buy, price(100)), quantity(5),);

        assert_eq!(book.aggregate_quantity(Side::Sell, price(100)), quantity(7),);
    }

    #[test]
    fn aggregate_overflow_rejects_second_order_atomically() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Buy, 100, i64::MAX))
            .expect("maximum canonical quantity fits");

        let result = book.insert(order(2, Side::Buy, 100, 1));

        assert_eq!(
            result,
            Err(OrderBookError::AggregateQuantityOutOfRange { price: price(100) }),
        );

        assert_eq!(book.len(), 1);
        assert!(!book.contains_order(OrderId::new(2)));

        let level = book
            .level(Side::Buy, price(100))
            .expect("original level remains");

        assert_eq!(level.aggregate_quantity(), quantity(i64::MAX));

        assert_eq!(level.order_ids().collect::<Vec<_>>(), vec![OrderId::new(1)],);
    }

    #[test]
    fn level_iterators_are_best_price_first() {
        let mut book = OrderBook::new();

        book.insert(order(1, Side::Buy, 100, 1)).expect("bid");
        book.insert(order(2, Side::Buy, 103, 1)).expect("bid");
        book.insert(order(3, Side::Buy, 101, 1)).expect("bid");

        book.insert(order(4, Side::Sell, 110, 5)).expect("ask");
        book.insert(order(5, Side::Sell, 105, 5)).expect("ask");
        book.insert(order(6, Side::Sell, 108, 5)).expect("ask");

        assert_eq!(
            book.bid_levels().map(PriceLevel::price).collect::<Vec<_>>(),
            vec![price(103), price(101), price(100)],
        );

        assert_eq!(
            book.ask_levels().map(PriceLevel::price).collect::<Vec<_>>(),
            vec![price(105), price(108), price(110)],
        );
    }
}
