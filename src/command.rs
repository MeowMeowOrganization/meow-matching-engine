use crate::{
    domain::{OrderId, PriceTicks, QuantityLots, Side},
    error::CommandError,
};

/// Places one canonicalized limit order into the matching engine.
///
/// Human decimal parsing, tick conversion, lot conversion, balance checks, reservation and client-order idempotency happen before this command reaches the matching core.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceLimitOrder {
    order_id: OrderId,
    side: Side,
    price: PriceTicks,
    quantity: QuantityLots,
}

impl PlaceLimitOrder {
    /// Creates a valid incoming limit-order command.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::ZeroPrice`] when `price` is zero.
    ///
    /// Returns [`CommandError::ZeroQuantity`] when `quantity` is zero.
    pub const fn new(
        order_id: OrderId,
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    ) -> Result<Self, CommandError> {
        if price.is_zero() {
            return Err(CommandError::ZeroPrice);
        }

        if quantity.is_zero() {
            return Err(CommandError::ZeroQuantity);
        }

        Ok(Self {
            order_id,
            side,
            price,
            quantity,
        })
    }

    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    #[must_use]
    pub const fn side(self) -> Side {
        self.side
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

/// Canonical synchronous commands accepted by one matching-engine instance.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    PlaceLimitOrder(PlaceLimitOrder),
}

impl From<PlaceLimitOrder> for Command {
    fn from(order: PlaceLimitOrder) -> Self {
        Self::PlaceLimitOrder(order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn positive_price() -> PriceTicks {
        PriceTicks::new(100).expect("positive price")
    }

    fn positive_quantity() -> QuantityLots {
        QuantityLots::new(10).expect("positive quantity")
    }

    #[test]
    fn limit_order_command_accepts_positive_price_and_quantity() {
        let command = PlaceLimitOrder::new(
            OrderId::new(1),
            Side::Buy,
            positive_price(),
            positive_quantity(),
        )
        .expect("valid command");

        assert_eq!(command.order_id(), OrderId::new(1));
        assert_eq!(command.side(), Side::Buy);
        assert_eq!(command.price(), positive_price());
        assert_eq!(command.quantity(), positive_quantity());
    }

    #[test]
    fn limit_order_command_rejects_zero_price() {
        let result = PlaceLimitOrder::new(
            OrderId::new(1),
            Side::Buy,
            PriceTicks::ZERO,
            positive_quantity(),
        );

        assert_eq!(result, Err(CommandError::ZeroPrice));
    }

    #[test]
    fn limit_order_command_rejects_zero_quantity() {
        let result = PlaceLimitOrder::new(
            OrderId::new(1),
            Side::Sell,
            positive_price(),
            QuantityLots::ZERO,
        );

        assert_eq!(result, Err(CommandError::ZeroQuantity));
    }
}
