use crate::config::EngineConfig;
use crate::error::EngineError;

/// Internal deterministic arrival sequence.
///
/// This is deliberately not a wall-clock timestamp.
///
/// It is private because sequence representation is currently an engine implementation detail rather than part of the external contract.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SequenceNumber(u64);

impl SequenceNumber {
    const ZERO: Self = Self(0);

    fn advance(&mut self) -> Result<Self, EngineError> {
        let current = *self;

        self.0 = self.0.checked_add(1).ok_or(EngineError::SequenceOverflow)?;

        Ok(current)
    }
}

/// Deterministic matching state for exactly one market.

#[derive(Debug)]
pub struct MatchingEngine {
    config: EngineConfig,

    // This becomes actively used when the first order-processing command is introduced.
    // It exists now because deterministic price-time sequencing is part of the engine foundation.
    #[allow(dead_code)]
    next_sequence: SequenceNumber,
}

impl MatchingEngine {
    #[must_use]
    pub const fn new(config: EngineConfig) -> Self {
        Self {
            config,
            next_sequence: SequenceNumber::ZERO,
        }
    }

    #[must_use]
    pub const fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Reserves the next deterministic logical sequence number.
    ///
    /// This remains private until actual command processing is introduced.
    #[allow(dead_code)]
    fn reserve_sequence(&mut self) -> Result<SequenceNumber, EngineError> {
        self.next_sequence.advance()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::MarketId;

    fn test_config() -> EngineConfig {
        EngineConfig::new(MarketId::new(1))
    }

    #[test]
    fn engine_owns_exactly_one_market_configuration() {
        let engine = MatchingEngine::new(test_config());

        assert_eq!(engine.config().market_id(), MarketId::new(1));
    }

    #[test]
    fn sequence_starts_at_zero() {
        let mut engine = MatchingEngine::new(test_config());

        assert_eq!(
            engine.reserve_sequence().expect("sequence should exist"),
            SequenceNumber(0),
        );
    }

    #[test]
    fn sequence_is_monotonic() {
        let mut engine = MatchingEngine::new(test_config());

        assert_eq!(
            engine.reserve_sequence().expect("first sequence"),
            SequenceNumber(0),
        );

        assert_eq!(
            engine.reserve_sequence().expect("second sequence"),
            SequenceNumber(1),
        );

        assert_eq!(
            engine.reserve_sequence().expect("third sequence"),
            SequenceNumber(2),
        );
    }
}
