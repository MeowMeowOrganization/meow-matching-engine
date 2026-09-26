use crate::domain::{OrderId, PriceTicks, QuantityLots, Side};

use super::error::OrderError;

/// One resting limit order.
///
/// Prices and  quantities  are already canonicalized before reaching this type.
/// Human decimals, tick-size conversion and lot-size conversion do not  occur here.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    id: OrderId,
    side: Side,
    price: PriceTicks,
    quantity: QuantityLots,
}

impl Order {
    /// Creates a resting limit order.
    ///
    /// # Errors
    ///
    /// Returns [`OrderError::ZeroPrice`] when `price` is zero.
    ///
    /// Returns [`OrderError:ZeroQuantity`] when `quantity` is zero.
    pub const fn new(
        id: OrderId,
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    ) -> Result<Self, OrderError> {
        if price.is_zero() {
            return Err(OrderError::ZeroPrice);
        }

        if quantity.is_zero() {
            return Err(OrderError::ZeroQuantity);
        }

        Ok(Self {
            id,
            side,
            price,
            quantity,
        })
    }

    #[must_use]
    pub const fn id(&self) -> OrderId {
        self.id
    }

    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    #[must_use]
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    #[must_use]
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_resting_order_can_be_created() {
        let order = Order::new(
            OrderId::new(1),
            Side::Buy,
            PriceTicks::new(100).expect("positive price"),
            QuantityLots::new(25).expect("positive quantity"),
        )
        .expect("valid order");

        assert_eq!(order.id(), OrderId::new(1));
        assert_eq!(order.side(), Side::Buy);
        assert_eq!(order.price().get(), 100);
        assert_eq!(order.quantity().get(), 25);
    }

    #[test]
    fn resting_order_rejects_zero_price() {
        let result = Order::new(
            OrderId::new(1),
            Side::Buy,
            PriceTicks::ZERO,
            QuantityLots::new(1).expect("positive quantity"),
        );

        assert_eq!(result, Err(OrderError::ZeroPrice));
    }

    #[test]
    fn resting_order_rejects_zero_quantity() {
        let result = Order::new(
            OrderId::new(1),
            Side::Buy,
            PriceTicks::new(1).expect("positive price"),
            QuantityLots::ZERO,
        );

        assert_eq!(result, Err(OrderError::ZeroQuantity));
    }
}
