use thiserror::Error;

/// Errors encountered while constructing canonical matching-domain values.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("price ticks cannot be negative: {0}")]
    NegativePriceTicks(i64),

    #[error("quantity lots cannot be negative: {0}")]
    NegativeQuantityLots(i64),
}

/// Errors encountered while constructing a canonical engine command.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CommandError {
    #[error("limit-order price must be greater than zero")]
    ZeroPrice,

    #[error("limit-order quantity must be greater than zero")]
    ZeroQuantity,
}

/// Errors indicating that the matching engine cannot safely complete deterministic processing.
///
/// Expected business-level rejection is represented through domain events rather than through this error type.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EngineError {
    #[error("engine sequence number overflowed")]
    SequenceOverflow,

    #[error("checked financial arithmetic overflowed")]
    ArithmeticOverflow,

    #[error("financial arithmetic result is outside the canonical i64 range")]
    CanonicalIntegerOutRange,

    #[error("order-book invariant was violated")]
    OrderBookInvariantViolation,
}
