use std::str;

use meow_matching_engine::{
    CancelOrder, CancelRejected as CoreCancelRejected,
    CancelRejectionReason as CoreCancelRejectionReason, Command, Event as CoreEvent,
    Execution as CoreExecution, MarketId, OrderCancelled as CoreOrderCancelled, OrderId,
    OrderRejected as CoreOrderRejected, OrderRejectionReason as CoreOrderRejectionReason,
    PlaceLimitOrder, PriceTicks, ProcessResult, QuantityLots, SequencedEvent, Side, StateHash,
};
use prost::Message;
use thiserror::Error;

use crate::contracts::{matching as pb, order as order_pb};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandFingerprint {
    PlaceLimitOrder {
        market_id: u32,
        order_id: u64,
        side: Side,
        price_ticks: i64,
        quantity_lots: i64,
    },
    CancelOrder {
        market_id: u32,
        order_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedCommand {
    command_id: String,
    market_id: MarketId,
    command: Command,
    fingerprint: CommandFingerprint,
    wire: pb::MatchingCommand,
}

impl DecodedCommand {
    #[must_use]
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    #[must_use]
    pub const fn market_id(&self) -> MarketId {
        self.market_id
    }

    #[must_use]
    pub const fn command(&self) -> Command {
        self.command
    }

    #[must_use]
    pub const fn fingerprint(&self) -> CommandFingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn wire(&self) -> &pb::MatchingCommand {
        &self.wire
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("Kafka matching command record has no key")]
    MissingKey,

    #[error("Kafka matching command record has no payload")]
    MissingPayload,

    #[error("Kafka market key is not valid UTF-8")]
    NonUtf8Key,

    #[error("Kafka market key is not canonical unsigned decimal: {0}")]
    NonCanonicalMarketKey(String),

    #[error("matching command protobuf is malformed: {0}")]
    MalformedProtobuf(String),

    #[error("matching command_id must be non-empty")]
    EmptyCommandId,

    #[error(
        "Kafka key market_id {key_market_id} does not match payload market_id {payload_market_id}"
    )]
    MarketKeyMismatch {
        key_market_id: u32,
        payload_market_id: u32,
    },

    #[error("matching command is missing its command oneof")]
    MissingCommand,

    #[error("unsupported/unspecified OrderSide enum value: {0}")]
    InvalidOrderSide(i32),

    #[error("invalid canonical price_ticks: {0}")]
    InvalidPriceTicks(i64),

    #[error("invalid canonical quantity_lots: {0}")]
    InvalidQuantityLots(i64),

    #[error("invalid limit-order command: {0}")]
    InvalidLimitOrder(String),
}

/// Decodes and validates one authoritative matching-command Kafka record.
///
/// # Errors
///
/// Returns [`DecodeError`] when the key/envelope is malformed, the key and
/// payload disagree about the market, or the canonical command cannot be built.
pub fn decode_record(
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
) -> Result<DecodedCommand, DecodeError> {
    let key = key.ok_or(DecodeError::MissingKey)?;
    let payload = payload.ok_or(DecodeError::MissingPayload)?;

    let key_market_id = parse_market_key(key)?;
    let wire = pb::MatchingCommand::decode(payload)
        .map_err(|error| DecodeError::MalformedProtobuf(error.to_string()))?;

    if wire.command_id.is_empty() {
        return Err(DecodeError::EmptyCommandId);
    }

    if key_market_id != wire.market_id {
        return Err(DecodeError::MarketKeyMismatch {
            key_market_id,
            payload_market_id: wire.market_id,
        });
    }

    let market_id = MarketId::new(wire.market_id);
    let (command, fingerprint) = decode_command_body(market_id, &wire)?;

    Ok(DecodedCommand {
        command_id: wire.command_id.clone(),
        market_id,
        command,
        fingerprint,
        wire,
    })
}

fn parse_market_key(key: &[u8]) -> Result<u32, DecodeError> {
    let text = str::from_utf8(key).map_err(|_| DecodeError::NonUtf8Key)?;
    let market_id = text
        .parse::<u32>()
        .map_err(|_| DecodeError::NonCanonicalMarketKey(text.to_owned()))?;

    if market_id.to_string().as_bytes() != key {
        return Err(DecodeError::NonCanonicalMarketKey(text.to_owned()));
    }

    Ok(market_id)
}

fn decode_command_body(
    market_id: MarketId,
    wire: &pb::MatchingCommand,
) -> Result<(Command, CommandFingerprint), DecodeError> {
    use pb::matching_command::Command as WireCommand;

    match wire.command.as_ref().ok_or(DecodeError::MissingCommand)? {
        WireCommand::PlaceLimitOrder(place) => {
            let side = decode_side(place.side)?;
            let price = PriceTicks::new(place.price_ticks)
                .map_err(|_| DecodeError::InvalidPriceTicks(place.price_ticks))?;
            let quantity = QuantityLots::new(place.quantity_lots)
                .map_err(|_| DecodeError::InvalidQuantityLots(place.quantity_lots))?;
            let command = PlaceLimitOrder::new(OrderId::new(place.order_id), side, price, quantity)
                .map_err(|error| DecodeError::InvalidLimitOrder(error.to_string()))?;

            Ok((
                command.into(),
                CommandFingerprint::PlaceLimitOrder {
                    market_id: market_id.get(),
                    order_id: place.order_id,
                    side,
                    price_ticks: place.price_ticks,
                    quantity_lots: place.quantity_lots,
                },
            ))
        }
        WireCommand::CancelOrder(cancel) => Ok((
            CancelOrder::new(OrderId::new(cancel.order_id)).into(),
            CommandFingerprint::CancelOrder {
                market_id: market_id.get(),
                order_id: cancel.order_id,
            },
        )),
    }
}

fn decode_side(value: i32) -> Result<Side, DecodeError> {
    match order_pb::OrderSide::try_from(value) {
        Ok(order_pb::OrderSide::Buy) => Ok(Side::Buy),
        Ok(order_pb::OrderSide::Sell) => Ok(Side::Sell),
        Ok(order_pb::OrderSide::Unspecified) | Err(_) => Err(DecodeError::InvalidOrderSide(value)),
    }
}

#[must_use]
pub fn market_key(market_id: MarketId) -> String {
    market_id.get().to_string()
}

/// Serializes the canonical result for one provisional semantic engine commit.
///
/// The caller must still commit the surrounding Kafka transaction before this
/// result or its input offset is considered durable.
#[must_use]
pub fn encode_matching_result(
    command: &pb::MatchingCommand,
    process_result: &ProcessResult,
    state_hash: StateHash,
) -> Vec<u8> {
    let result = pb::MatchingResult {
        command: Some(command.clone()),
        market_sequence: process_result.market_sequence().get(),
        events: process_result.events().iter().map(encode_event).collect(),
        state_hash_after: Some(pb::MatchingStateHash {
            encoding_version: u32::from(StateHash::ENCODING_VERSION),
            sha256: state_hash.as_bytes().to_vec(),
        }),
    };

    result.encode_to_vec()
}

fn encode_event(event: &SequencedEvent) -> pb::SequencedMatchingEvent {
    let id = event.id();

    pb::SequencedMatchingEvent {
        event_id: Some(pb::MatchingEventId {
            market_id: id.market_id().get(),
            market_sequence: id.market_sequence().get(),
            event_ordinal: id.event_ordinal().get(),
        }),
        event: Some(match event.event() {
            CoreEvent::Execution(execution) => {
                pb::sequenced_matching_event::Event::Execution(encode_execution(*execution))
            }
            CoreEvent::OrderRejected(rejection) => {
                pb::sequenced_matching_event::Event::OrderRejected(encode_order_rejected(
                    *rejection,
                ))
            }
            CoreEvent::OrderCancelled(cancelled) => {
                pb::sequenced_matching_event::Event::OrderCancelled(encode_order_cancelled(
                    *cancelled,
                ))
            }
            CoreEvent::CancelRejected(rejection) => {
                pb::sequenced_matching_event::Event::CancelRejected(encode_cancel_rejected(
                    *rejection,
                ))
            }
        }),
    }
}

fn encode_execution(execution: CoreExecution) -> pb::Execution {
    pb::Execution {
        maker_order_id: execution.maker_order_id().get(),
        taker_order_id: execution.taker_order_id().get(),
        taker_side: encode_side(execution.taker_side()),
        price_ticks: execution.price().get(),
        quantity_lots: execution.quantity().get(),
    }
}

fn encode_order_rejected(rejection: CoreOrderRejected) -> pb::OrderRejected {
    let reason = match rejection.reason() {
        CoreOrderRejectionReason::DuplicateOrderId => {
            pb::MatchingOrderRejectionReason::DuplicateOrderId
        }
        CoreOrderRejectionReason::RestingAggregateQuantityOutOfRange => {
            pb::MatchingOrderRejectionReason::RestingAggregateQuantityOutOfRange
        }
    };

    pb::OrderRejected {
        order_id: rejection.order_id().get(),
        reason: reason as i32,
    }
}

fn encode_order_cancelled(cancelled: CoreOrderCancelled) -> pb::OrderCancelled {
    pb::OrderCancelled {
        order_id: cancelled.order_id().get(),
        cancelled_quantity_lots: cancelled.cancelled_quantity().get(),
    }
}

fn encode_cancel_rejected(rejection: CoreCancelRejected) -> pb::CancelRejected {
    let reason = match rejection.reason() {
        CoreCancelRejectionReason::UnknownOrder => pb::MatchingCancelRejectionReason::UnknownOrder,
        CoreCancelRejectionReason::AlreadyFilled => {
            pb::MatchingCancelRejectionReason::AlreadyFilled
        }
        CoreCancelRejectionReason::AlreadyCancelled => {
            pb::MatchingCancelRejectionReason::AlreadyCancelled
        }
        CoreCancelRejectionReason::AlreadyRejected => {
            pb::MatchingCancelRejectionReason::AlreadyRejected
        }
    };

    pb::CancelRejected {
        order_id: rejection.order_id().get(),
        reason: reason as i32,
    }
}

const fn encode_side(side: Side) -> i32 {
    match side {
        Side::Buy => order_pb::OrderSide::Buy as i32,
        Side::Sell => order_pb::OrderSide::Sell as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn place_wire(command_id: &str, market_id: u32) -> pb::MatchingCommand {
        pb::MatchingCommand {
            command_id: command_id.to_owned(),
            market_id,
            command: Some(pb::matching_command::Command::PlaceLimitOrder(
                pb::PlaceLimitOrder {
                    order_id: 9,
                    side: order_pb::OrderSide::Buy as i32,
                    price_ticks: 100,
                    quantity_lots: 5,
                },
            )),
        }
    }

    #[test]
    fn rejects_non_canonical_market_key() {
        let payload = place_wire("cmd-1", 7).encode_to_vec();

        let result = decode_record(Some(b"007"), Some(&payload));

        assert_eq!(
            result,
            Err(DecodeError::NonCanonicalMarketKey("007".to_owned()))
        );
    }

    #[test]
    fn rejects_key_payload_market_mismatch() {
        let payload = place_wire("cmd-1", 7).encode_to_vec();

        let result = decode_record(Some(b"8"), Some(&payload));

        assert_eq!(
            result,
            Err(DecodeError::MarketKeyMismatch {
                key_market_id: 8,
                payload_market_id: 7,
            })
        );
    }

    #[test]
    fn decodes_canonical_limit_order() {
        let wire = place_wire("cmd-1", 7);
        let payload = wire.encode_to_vec();

        let decoded = decode_record(Some(b"7"), Some(&payload)).expect("valid record");

        assert_eq!(decoded.command_id(), "cmd-1");
        assert_eq!(decoded.market_id(), MarketId::new(7));
        assert_eq!(decoded.wire(), &wire);
        assert_eq!(
            decoded.fingerprint(),
            CommandFingerprint::PlaceLimitOrder {
                market_id: 7,
                order_id: 9,
                side: Side::Buy,
                price_ticks: 100,
                quantity_lots: 5,
            }
        );
    }
}
