use crate::domain::MarketId;

/// Deterministic configuration required directly by one matching-engine instance.
///
/// Configuration loading is deliberately outside this crate.
/// The engine does not read environment variables, files, Kubernetes configuration, databases or remote services.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    market_id: MarketId,
}

impl EngineConfig {
    #[must_use]
    pub const fn new(market_id: MarketId) -> Self {
        Self { market_id }
    }

    #[must_use]
    pub const fn market_id(&self) -> MarketId {
        self.market_id
    }
}
