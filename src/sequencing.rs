use crate::{domain::MarketId, error::EngineError, event::Event};

/// Per-market logical position of one committed canonical command.
///
/// `0` represent a pristine engine with no committed commands.
/// The first successfully processed command commits sequence `1`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MarketSequence(u64);

impl MarketSequence {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Zero-based deterministic position of one event inside the output produced by a single committed command.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventOrdinal(u64);

impl EventOrdinal {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Canonical deterministic identity of one matching-engine event.
///
/// Identity is derived entirely from deterministic engine state:
///
/// `(MarketId, MarketSequence, EventOrdinal)`.
///
/// It contains no UUID, wall-clock timestamp, Kafka offset, database ID or other infrastructure-generated value.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventId {
    market_id: MarketId,
    market_sequence: MarketSequence,
    event_ordinal: EventOrdinal,
}

impl EventId {
    #[must_use]
    pub const fn new(
        market_id: MarketId,
        market_sequence: MarketSequence,
        event_ordinal: EventOrdinal,
    ) -> Self {
        Self {
            market_id,
            market_sequence,
            event_ordinal,
        }
    }

    #[must_use]
    pub const fn market_id(self) -> MarketId {
        self.market_id
    }

    #[must_use]
    pub const fn market_sequence(self) -> MarketSequence {
        self.market_sequence
    }

    #[must_use]
    pub const fn event_ordinal(self) -> EventOrdinal {
        self.event_ordinal
    }
}

/// One domain event plus its deterministic state-machine identity.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequencedEvent {
    id: EventId,
    event: Event,
}

impl SequencedEvent {
    #[must_use]
    pub const fn new(id: EventId, event: Event) -> Self {
        Self { id, event }
    }

    #[must_use]
    pub const fn id(&self) -> EventId {
        self.id
    }

    #[must_use]
    pub const fn event(&self) -> &Event {
        &self.event
    }

    #[must_use]
    pub const fn into_event(self) -> Event {
        self.event
    }
}

/// Committed deterministic result of processing exactly one canonical command.
///
/// A successful command always returns one `ProcessResult`, even when it produces zero domain events.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessResult {
    market_id: MarketId,
    market_sequence: MarketSequence,
    events: Vec<SequencedEvent>,
}

impl ProcessResult {
    pub(crate) fn new(
        market_id: MarketId,
        market_sequence: MarketSequence,
        events: Vec<Event>,
    ) -> Result<Self, EngineError> {
        let mut sequenced_events = Vec::with_capacity(events.len());

        for (index, event) in events.into_iter().enumerate() {
            let ordinal_value =
                u64::try_from(index).map_err(|_| EngineError::EventOrdinalOverflow)?;

            let event_ordinal = EventOrdinal::new(ordinal_value);

            let event_id = EventId::new(market_id, market_sequence, event_ordinal);

            sequenced_events.push(SequencedEvent::new(event_id, event));
        }

        Ok(Self {
            market_id,
            market_sequence,
            events: sequenced_events,
        })
    }

    #[must_use]
    pub const fn market_id(&self) -> MarketId {
        self.market_id
    }

    #[must_use]
    pub const fn market_sequence(&self) -> MarketSequence {
        self.market_sequence
    }

    #[must_use]
    pub fn events(&self) -> &[SequencedEvent] {
        &self.events
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    #[must_use]
    pub fn into_sequenced_events(self) -> Vec<SequencedEvent> {
        self.events
    }

    /// Compatibility helper for tests or callers interested only in the underlying domain events.
    ///
    /// Production publication paths should normally preserve the `SequencedEvent` envelopes instead.
    #[must_use]
    pub fn into_domain_events(self) -> Vec<Event> {
        self.events
            .into_iter()
            .map(SequencedEvent::into_event)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        OrderId,
        event::{CancelRejected, CancelRejectionReason},
    };

    #[test]
    fn market_sequence_zero_represents_pristine_state() {
        assert_eq!(MarketSequence::ZERO.get(), 0);
    }

    #[test]
    fn market_sequence_advances_without_mutating_original() {
        let current = MarketSequence::new(41);

        let next = current.checked_next().expect("sequence can advance");

        assert_eq!(current.get(), 41);
        assert_eq!(next.get(), 42);
    }

    #[test]
    fn maximum_market_sequence_has_no_successor() {
        let maximum = MarketSequence::new(u64::MAX);

        assert_eq!(maximum.checked_next(), None);
    }

    #[test]
    fn process_result_assigns_zero_based_ordinals() {
        let market_id = MarketId::new(7);
        let sequence = MarketSequence::new(99);

        let first = Event::CancelRejected(CancelRejected::new(
            market_id,
            OrderId::new(1),
            CancelRejectionReason::UnknownOrder,
        ));

        let second = Event::CancelRejected(CancelRejected::new(
            market_id,
            OrderId::new(2),
            CancelRejectionReason::UnknownOrder,
        ));

        let result = ProcessResult::new(market_id, sequence, vec![first, second])
            .expect("two  events fit canonical ordinal range");

        assert_eq!(result.events().len(), 2);

        assert_eq!(
            result.events()[0].id(),
            EventId::new(market_id, sequence, EventOrdinal::new(0),),
        );

        assert_eq!(
            result.events()[1].id(),
            EventId::new(market_id, sequence, EventOrdinal::new(1),),
        );
    }

    #[test]
    fn empty_process_result_preserves_committed_transition() {
        let result = ProcessResult::new(MarketId::new(7), MarketSequence::new(3), Vec::new())
            .expect("empty event stream is valid");

        assert_eq!(result.market_id(), MarketId::new(7));
        assert_eq!(result.market_sequence(), MarketSequence::new(3),);
        assert!(result.is_empty());
    }
}
