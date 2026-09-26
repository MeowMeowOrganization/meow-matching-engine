use thiserror::Error;

use crate::domain::{OrderId, PriceTicks};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OrderError {
    #[error("limit-order price must be greater than zero")]
    ZeroPrice,

    #[error("limit-order quantity must be greater than zero")]
    ZeroQuantity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OrderBookError {
    #[error("order id is already present in the book: {0:?}")]
    DuplicateOrderId(OrderId),

    #[error("aggregate quantity at price {price:?} exceeds the canonical i64 range")]
    AggregateQuantityOutOfRange { price: PriceTicks },

    #[error("order-book indices are inconsistent for order id: {0:?}")]
    InconsistentState(OrderId),
}
