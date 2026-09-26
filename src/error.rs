use thiserror::Error;

/// Errors encountered while constructing canonical matching-domain values.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("price ticks cannot be negative: {0}")]
    NegativePriceTicks(i64),

    #[error("quantity lots cannot be negative: {0}")]
    NegativeQuantityLots(i64),
}

/// Errors indicating that the matching engine cannot safely complete deterministic processing.
///
/// Expected business-level order rejection should eventually be represented as domain events rather than as `EngineError`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EngineError {
    #[error("engine sequence number overflowed")]
    SequenceOverflow,

    #[error("checked financial arithmetic overflowed")]
    ArithmeticOverflow,

    #[error("financial arithmetic result is outside the canonical i64 range")]
    CanonicalIntegerOutRange,
}
