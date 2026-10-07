use std::collections::HashMap;

use meow_matching_engine::{
    EngineConfig, EngineError, MarketId, MatchingEngine, ProcessResult, StateHash,
};
use thiserror::Error;

use crate::codec::{CommandFingerprint, DecodedCommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupeDecision {
    New,
    Duplicate,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartitionStateError {
    #[error("command_id {command_id:?} was reused with different canonical contents")]
    ConflictingCommandId { command_id: String },

    #[error("matching engine failed: {0}")]
    Engine(#[from] EngineError),
}

#[derive(Debug)]
pub struct NewCommandResult {
    process_result: ProcessResult,
    state_hash: StateHash,
}

impl NewCommandResult {
    #[must_use]
    pub const fn process_result(&self) -> &ProcessResult {
        &self.process_result
    }

    #[must_use]
    pub const fn state_hash(&self) -> StateHash {
        self.state_hash
    }
}

#[derive(Debug)]
pub enum LiveProcessOutcome {
    New(NewCommandResult),
    Duplicate,
}

/// Reconstructable in-memory state owned by exactly one Kafka input partition.
///
/// A partition may contain many markets. Each market still owns an independent
/// deterministic `MatchingEngine`; the partition-level dedupe index exists only
/// because `command_id` is transport identity and must be checked before routing
/// into any engine.
#[derive(Debug, Default)]
pub struct PartitionState {
    engines: HashMap<MarketId, MatchingEngine>,
    command_ids: HashMap<String, CommandFingerprint>,
}

impl PartitionState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one historical record while reconstructing committed state.
    ///
    /// Replay never republishes matching results. Exact transport duplicates are
    /// skipped, while conflicting command-id reuse is fatal corruption.
    /// # Errors
    ///
    /// Returns [`PartitionStateError`] for conflicting command identity or any
    /// fail-stop deterministic engine error encountered during replay.
    pub fn replay(
        &mut self,
        decoded: &DecodedCommand,
    ) -> Result<DedupeDecision, PartitionStateError> {
        match self.observe_command_id(decoded)? {
            DedupeDecision::Duplicate => Ok(DedupeDecision::Duplicate),
            DedupeDecision::New => {
                self.engine_mut(decoded.market_id())
                    .process(decoded.command())?;
                Ok(DedupeDecision::New)
            }
        }
    }

    /// Applies one live command provisionally to Rust memory.
    ///
    /// The returned state is not Kafka-durable. The caller must discard this
    /// entire `PartitionState` and reconstruct from committed Kafka history if
    /// the surrounding Kafka transaction later aborts or becomes untrusted.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionStateError`] for conflicting command identity or a
    /// fail-stop deterministic engine error.
    pub fn process_live(
        &mut self,
        decoded: &DecodedCommand,
    ) -> Result<LiveProcessOutcome, PartitionStateError> {
        match self.observe_command_id(decoded)? {
            DedupeDecision::Duplicate => Ok(LiveProcessOutcome::Duplicate),
            DedupeDecision::New => {
                let engine = self.engine_mut(decoded.market_id());
                let process_result = engine.process(decoded.command())?;
                let state_hash = engine.state_hash()?;

                Ok(LiveProcessOutcome::New(NewCommandResult {
                    process_result,
                    state_hash,
                }))
            }
        }
    }

    #[must_use]
    pub fn engine(&self, market_id: MarketId) -> Option<&MatchingEngine> {
        self.engines.get(&market_id)
    }

    fn observe_command_id(
        &mut self,
        decoded: &DecodedCommand,
    ) -> Result<DedupeDecision, PartitionStateError> {
        match self.command_ids.get(decoded.command_id()) {
            Some(existing) if *existing == decoded.fingerprint() => Ok(DedupeDecision::Duplicate),
            Some(_) => Err(PartitionStateError::ConflictingCommandId {
                command_id: decoded.command_id().to_owned(),
            }),
            None => {
                self.command_ids
                    .insert(decoded.command_id().to_owned(), decoded.fingerprint());
                Ok(DedupeDecision::New)
            }
        }
    }

    fn engine_mut(&mut self, market_id: MarketId) -> &mut MatchingEngine {
        self.engines
            .entry(market_id)
            .or_insert_with(|| MatchingEngine::new(EngineConfig::new(market_id)))
    }
}
