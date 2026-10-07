//! Kafka adapter/runtime for the pure Meow matching engine.
//!
//! The dependency direction is intentionally one-way:
//!
//! Kafka/Protobuf adapter -> `meow-matching-engine` core.
//!
//! The core crate has no dependency on this crate or on any Kafka type.

pub mod codec;
pub mod config;
pub mod contracts;
mod kafka;
pub mod recovery;
pub mod runtime;
pub mod state;

pub use runtime::{KafkaRuntime, RuntimeError};
