use crate::{
    book::{Order, OrderBook},
    command::{Command, PlaceLimitOrder},
    config::EngineConfig,
    error::EngineError,
    event::{Event, Execution, OrderRejected, OrderRejectionReason},
    matching::{ExecutionPlan, plan_limit_order},
};

/// Internal deterministic command-arrival sequence.
///
/// This is deliberately not a wall-clock timestamp.

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

#[derive(Debug, PartialEq, Eq)]
pub struct MatchingEngine {
    config: EngineConfig,
    book: OrderBook,
    next_sequence: SequenceNumber,
}

impl MatchingEngine {
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            book: OrderBook::new(),
            next_sequence: SequenceNumber::ZERO,
        }
    }

    #[must_use]
    pub const fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Returns read-only access to current deterministic order-book state.
    #[must_use]
    pub const fn book(&self) -> &OrderBook {
        &self.book
    }

    /// Processes exactly one canonical command synchronously.
    ///
    /// Returned events are already in canonical deterministic order.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] only when the engine cannot safely complete deterministic processing.
    pub fn process(&mut self, command: Command) -> Result<Vec<Event>, EngineError> {
        self.reserve_sequence()?;

        match command {
            Command::PlaceLimitOrder(place_order) => self.process_limit_order(place_order),
        }
    }

    fn process_limit_order(&mut self, command: PlaceLimitOrder) -> Result<Vec<Event>, EngineError> {
        if self.book.contains_order(command.order_id()) {
            return Ok(vec![
                self.rejection_event(command, OrderRejectionReason::DuplicateOrderId),
            ]);
        }

        // PLAN
        //
        // No book mutation occurs while the execution plan is being built.
        let plan = plan_limit_order(&self.book, command)?;

        // VALIDATE
        //
        // Any positive taker remainder will become a new resting order.
        //
        // Validate its aggregate before mutating makers.
        if !plan.remaining_quantity().is_zero()
            && !self.book.can_rest_quantity(
                command.side(),
                command.price(),
                plan.remaining_quantity(),
            )
        {
            return Ok(vec![self.rejection_event(
                command,
                OrderRejectionReason::RestingAggregateQuantityOutOfRange,
            )]);
        }

        // Construct the possible resting remainder before applying executions.
        // Therefore no ordinary constructor failure can occur after book mutation begins.
        let resting_order = Self::prepare_remainder(command, &plan)?;

        // APPLY
        //
        // Every planned maker must still be the FIFO front because the engine is single-threaded and nothing mutated the book between planning and application.
        for planned_execution in plan.fills() {
            self.book
                .apply_execution(
                    planned_execution.maker_order_id(),
                    planned_execution.quantity(),
                )
                .map_err(|_| EngineError::OrderBookInvariantViolation)?;
        }

        if let Some(order) = resting_order {
            self.book
                .insert(order)
                .map_err(|_| EngineError::OrderBookInvariantViolation)?;
        }

        Ok(self.execution_events(command, plan))
    }

    fn prepare_remainder(
        command: PlaceLimitOrder,
        plan: &ExecutionPlan,
    ) -> Result<Option<Order>, EngineError> {
        let remaining_quantity = plan.remaining_quantity();

        if remaining_quantity.is_zero() {
            return Ok(None);
        }

        let order = Order::new(
            command.order_id(),
            command.side(),
            command.price(),
            remaining_quantity,
        )
        .map_err(|_| EngineError::OrderBookInvariantViolation)?;

        Ok(Some(order))
    }

    fn execution_events(&self, command: PlaceLimitOrder, plan: ExecutionPlan) -> Vec<Event> {
        plan.into_fills()
            .into_iter()
            .map(|planned_execution| {
                Event::Execution(Execution::new(
                    self.config.market_id(),
                    planned_execution.maker_order_id(),
                    command.order_id(),
                    command.side(),
                    planned_execution.price(),
                    planned_execution.quantity(),
                ))
            })
            .collect()
    }

    fn rejection_event(&self, command: PlaceLimitOrder, reason: OrderRejectionReason) -> Event {
        Event::OrderRejected(OrderRejected::new(
            self.config.market_id(),
            command.order_id(),
            reason,
        ))
    }

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
