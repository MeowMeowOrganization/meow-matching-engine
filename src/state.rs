use std::{collections::HashSet, fmt};

use sha2::{Digest, Sha256};

use crate::{
    book::OrderBook,
    domain::{MarketId, OrderId, Side},
    error::EngineError,
    lifecycle::{OrderLifecycle, OrderState},
    sequencing::MarketSequence,
};

const STATE_DOMAIN_SEPARATOR: &[u8] = b"MEOW-MATCHING-STATE";

const STATE_ENCODING_VERSION: u16 = 1;

const SECTION_BIDS: u8 = 0x01;
const SECTION_ASKS: u8 = 0x02;
const SECTION_LIFECYCLES: u8 = 0x03;

const STATE_OPEN: u8 = 0x01;
const STATE_PARTIALLY_FILLED: u8 = 0x02;
const STATE_FILLED: u8 = 0x03;
const STATE_CANCELLED: u8 = 0x04;
const STATE_REJECTED: u8 = 0x05;

/// SHA-256 fingerprint of one canonical committed matching-engine state.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StateHash([u8; 32]);

impl StateHash {
    /// Version of the canonical semantic-state byte encoding hashed by this
    /// type.
    ///
    /// Transport adapters may publish this alongside the digest so
    /// replay/audit code can reject incompatible encodings.
    pub const ENCODING_VERSION: u16 = 1;

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for StateHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }

        Ok(())
    }
}

pub(crate) fn canonical_state_bytes(
    market_id: MarketId,
    last_committed_sequence: MarketSequence,
    book: &OrderBook,
    lifecycles: &[(OrderId, OrderLifecycle)],
) -> Result<Vec<u8>, EngineError> {
    validate_book(book)?;

    let mut bytes = Vec::new();

    bytes.extend_from_slice(STATE_DOMAIN_SEPARATOR);

    push_u16(&mut bytes, STATE_ENCODING_VERSION);

    push_u32(&mut bytes, market_id.get());

    push_u64(&mut bytes, last_committed_sequence.get());

    encode_book_side(
        &mut bytes,
        SECTION_BIDS,
        book.bid_level_count(),
        Side::Buy,
        book.bid_levels(),
        book,
    )?;

    encode_book_side(
        &mut bytes,
        SECTION_ASKS,
        book.ask_level_count(),
        Side::Sell,
        book.ask_levels(),
        book,
    )?;

    bytes.push(SECTION_LIFECYCLES);

    push_len(&mut bytes, lifecycles.len())?;

    for (order_id, lifecycle) in lifecycles {
        push_u64(&mut bytes, order_id.get());

        push_i64(&mut bytes, lifecycle.submitted_quantity().get());

        push_i64(&mut bytes, lifecycle.executed_quantity().get());

        bytes.push(lifecycle_state_tag(lifecycle.state())?);
    }

    Ok(bytes)
}

#[must_use]
pub(crate) fn state_hash(canonical_bytes: &[u8]) -> StateHash {
    let digest = Sha256::digest(canonical_bytes);

    let mut bytes = [0_u8; 32];

    bytes.copy_from_slice(&digest);

    StateHash(bytes)
}

fn encode_book_side<'a>(
    bytes: &mut Vec<u8>,
    section_tag: u8,
    level_count: usize,
    expected_side: Side,
    levels: impl Iterator<Item = &'a crate::PriceLevel>,
    book: &OrderBook,
) -> Result<(), EngineError> {
    bytes.push(section_tag);

    push_len(bytes, level_count)?;

    for level in levels {
        push_i64(bytes, level.price().get());

        push_len(bytes, level.order_count())?;

        for order_id in level.order_ids() {
            let order = book
                .order(order_id)
                .ok_or(EngineError::OrderBookInvariantViolation)?;

            if order.id() != order_id
                || order.side() != expected_side
                || order.price() != level.price()
                || order.quantity().is_zero()
            {
                return Err(EngineError::OrderBookInvariantViolation);
            }

            push_u64(bytes, order_id.get());

            push_i64(bytes, order.quantity().get());
        }
    }

    Ok(())
}

/// Performs an independent physical-consistency pass before canonical output.
///
/// The `HashSet` is used only for membership/count validation. Its iteration order is never observed.
fn validate_book(book: &OrderBook) -> Result<(), EngineError> {
    let mut seen_order_ids = HashSet::with_capacity(book.len());

    validate_book_side(book, Side::Buy, book.bid_levels(), &mut seen_order_ids)?;

    validate_book_side(book, Side::Sell, book.ask_levels(), &mut seen_order_ids)?;

    if seen_order_ids.len() != book.len() {
        return Err(EngineError::OrderBookInvariantViolation);
    }

    Ok(())
}

fn validate_book_side<'a>(
    book: &OrderBook,
    expected_side: Side,
    levels: impl Iterator<Item = &'a crate::PriceLevel>,
    seen_order_ids: &mut HashSet<OrderId>,
) -> Result<(), EngineError> {
    for level in levels {
        if level.is_empty() || level.price().is_zero() {
            return Err(EngineError::OrderBookInvariantViolation);
        }

        let mut aggregate = 0_i128;

        for order_id in level.order_ids() {
            if !seen_order_ids.insert(order_id) {
                return Err(EngineError::OrderBookInvariantViolation);
            }

            let order = book
                .order(order_id)
                .ok_or(EngineError::OrderBookInvariantViolation)?;

            if order.id() != order_id
                || order.side() != expected_side
                || order.price() != level.price()
                || order.quantity().is_zero()
            {
                return Err(EngineError::OrderBookInvariantViolation);
            }

            aggregate = aggregate
                .checked_add(i128::from(order.quantity().get()))
                .ok_or(EngineError::ArithmeticOverflow)?;
        }

        if aggregate != i128::from(level.aggregate_quantity().get()) {
            return Err(EngineError::OrderBookInvariantViolation);
        }
    }

    Ok(())
}

fn lifecycle_state_tag(state: OrderState) -> Result<u8, EngineError> {
    match state {
        OrderState::Open => Ok(STATE_OPEN),

        OrderState::PartiallyFilled => Ok(STATE_PARTIALLY_FILLED),

        OrderState::Filled => Ok(STATE_FILLED),

        OrderState::Cancelled => Ok(STATE_CANCELLED),

        OrderState::Rejected => Ok(STATE_REJECTED),

        OrderState::New => Err(EngineError::OrderLifecycleInvariantViolation),
    }
}

fn push_len(bytes: &mut Vec<u8>, value: usize) -> Result<(), EngineError> {
    let value = u64::try_from(value).map_err(|_| EngineError::CanonicalStateOutOfRange)?;

    push_u64(bytes, value);

    Ok(())
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
