//! Deterministic matching-engine core for the Meow spot exchange.
//!
//! The crate intentionally knows nothing about:
//!
//! - Kafka,
//! - PostgreSQL,
//! - Redis,
//! - HTTP,
//! - Kubernetes,
//! - environment variables,
//! - configuration files,
//! - wall-clock ordering,
//! - logging subscribers,
//! - human decimal price/quantity parsing.
//!
//! Commands entering the matching engine are expected to contain already canonicalized integer-domain values.

mod book;
mod command;
mod config;
mod domain;
mod engine;
mod error;
mod event;
mod lifecycle;
mod matching;
mod sequencing;

pub use book::{Order, OrderBook, OrderBookError, OrderError, PriceLevel};
pub use command::{CancelOrder, Command, PlaceLimitOrder};
pub use config::EngineConfig;
pub use domain::{MarketId, OrderId, PriceTicks, QuantityLots, Side};
pub use engine::MatchingEngine;
pub use error::{CommandError, DomainError, EngineError};
pub use event::{
    CancelRejected, CancelRejectionReason, Event, Execution, OrderCancelled, OrderRejected,
    OrderRejectionReason,
};
pub use lifecycle::{OrderLifecycle, OrderState};
pub use sequencing::{EventId, EventOrdinal, MarketSequence, ProcessResult, SequencedEvent};
