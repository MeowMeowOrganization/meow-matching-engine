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
mod matching;

pub use book::{Order, OrderBook, OrderBookError, OrderError, PriceLevel};
pub use command::{Command, PlaceLimitOrder};
pub use config::EngineConfig;
pub use domain::{MarketId, OrderId, PriceTicks, QuantityLots, Side};
pub use engine::MatchingEngine;
pub use error::{CommandError, DomainError, EngineError};
pub use event::{Event, Execution, OrderRejected, OrderRejectionReason};
