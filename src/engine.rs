use crate::{
    book::{Order, OrderBook},
    command::{CancelOrder, Command, PlaceLimitOrder},
    config::EngineConfig,
    domain::OrderId,
    error::EngineError,
    event::{
        CancelRejected, CancelRejectionReason, Event, Execution, OrderCancelled, OrderRejected,
        OrderRejectionReason,
    },
    lifecycle::{LifecycleError, OrderLifecycle, OrderLifecycleIndex, OrderState},
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
    lifecycles: OrderLifecycleIndex,
    next_sequence: SequenceNumber,
}

impl MatchingEngine {
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            book: OrderBook::new(),
            lifecycles: OrderLifecycleIndex::new(),
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

    /// Returns deterministic lifecycle information for an order identity.
    ///
    /// Terminal lifecycle records remain available even after the order has left the active book.
    #[must_use]
    pub fn order_lifecycle(&self, order_id: OrderId) -> Option<&OrderLifecycle> {
        self.lifecycles.get(order_id)
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

            Command::CancelOrder(cancel_order) => self.process_cancel_order(cancel_order),
        }
    }

    fn process_limit_order(&mut self, command: PlaceLimitOrder) -> Result<Vec<Event>, EngineError> {
        // OrderId reuse is forbidden across the reconstructed engine history, not merely while the ID is actively resting.
        if self
            .validate_order_consistency(command.order_id())?
            .is_some()
        {
            return Ok(vec![self.order_rejection_event(
                command,
                OrderRejectionReason::DuplicateOrderId,
            )]);
        }

        // NEW is deliberately transient and local.
        //
        // It is not inserted into OrderLifecycleIndex.
        let mut incoming_lifecycle =
            OrderLifecycle::new(command.quantity()).map_err(Self::map_lifecycle_error)?;

        // PLAN
        //
        // No matching/book/lifecycle mutation occurs here.
        let plan = plan_limit_order(&self.book, command)?;

        // Validate every maker against both the active book and lifecycle index before mutation begins.
        self.validate_execution_plan_lifecycles(&plan)?;

        // VALIDATE
        //
        // Any positive taker remainder must be safe to rest before makers are modified.
        if !plan.remaining_quantity().is_zero()
            && !self.book.can_rest_quantity(
                command.side(),
                command.price(),
                plan.remaining_quantity(),
            )
        {
            incoming_lifecycle
                .reject()
                .map_err(Self::map_lifecycle_error)?;

            self.insert_lifecycle(command.order_id(), incoming_lifecycle)?;

            self.validate_order_consistency(command.order_id())?;

            return Ok(vec![self.order_rejection_event(
                command,
                OrderRejectionReason::RestingAggregateQuantityOutOfRange,
            )]);
        }

        // Construct all predictable post-plan state before mutation begins.
        let resting_order = Self::prepare_remainder(command, &plan)?;

        incoming_lifecycle
            .complete_placement(plan.remaining_quantity())
            .map_err(Self::map_lifecycle_error)?;

        // APPLY MAKER EXECUTIONS
        //
        // Book state and lifecycle state move together.
        for planned_execution in plan.fills() {
            let maker_order_id = planned_execution.maker_order_id();

            self.book
                .apply_execution(maker_order_id, planned_execution.quantity())
                .map_err(|_| EngineError::OrderBookInvariantViolation)?;

            self.lifecycle_mut(maker_order_id)?
                .apply_execution(planned_execution.quantity())
                .map_err(Self::map_lifecycle_error)?;

            // A partially-filled maker must still exist with exactly the derived remainder. A filled maker must have left the book.
            self.validate_order_consistency(maker_order_id)?;
        }

        // APPLY TAKER REMAINDER
        if let Some(order) = resting_order {
            self.book
                .insert(order)
                .map_err(|_| EngineError::OrderBookInvariantViolation)?;
        }

        // Persist the now-finalized incoming lifecycle only after NEW has transitioned into Open / PartiallyFilled / Filled.
        self.insert_lifecycle(command.order_id(), incoming_lifecycle)?;

        self.validate_order_consistency(command.order_id())?;

        Ok(self.execution_events(command, plan))
    }

    fn process_cancel_order(&mut self, command: CancelOrder) -> Result<Vec<Event>, EngineError> {
        let order_id = command.order_id();

        let Some(state) = self.validate_order_consistency(order_id)? else {
            return Ok(vec![self.cancel_rejection_event(
                order_id,
                CancelRejectionReason::UnknownOrder,
            )]);
        };

        match state {
            OrderState::Open | OrderState::PartiallyFilled => self.cancel_active_order(order_id),

            OrderState::Filled => {
                Ok(vec![self.cancel_rejection_event(
                    order_id,
                    CancelRejectionReason::AlreadyFilled,
                )])
            }

            OrderState::Cancelled => {
                Ok(vec![self.cancel_rejection_event(
                    order_id,
                    CancelRejectionReason::AlreadyCancelled,
                )])
            }

            OrderState::Rejected => {
                Ok(vec![self.cancel_rejection_event(
                    order_id,
                    CancelRejectionReason::AlreadyRejected,
                )])
            }

            // NEW is forbidden in the persisted lifecycle index.
            OrderState::New => Err(EngineError::OrderLifecycleInvariantViolation),
        }
    }

    fn cancel_active_order(&mut self, order_id: OrderId) -> Result<Vec<Event>, EngineError> {
        let expected_remaining = self
            .lifecycles
            .get(order_id)
            .ok_or(EngineError::OrderLifecycleInvariantViolation)?
            .active_remaining()
            .map_err(Self::map_lifecycle_error)?
            .ok_or(EngineError::OrderLifecycleInvariantViolation)?;

        let removed_order = self
            .book
            .remove(order_id)
            .map_err(|_| EngineError::OrderBookInvariantViolation)?
            .ok_or(EngineError::OrderLifecycleInvariantViolation)?;

        if removed_order.quantity() != expected_remaining {
            return Err(EngineError::OrderLifecycleInvariantViolation);
        }

        self.lifecycle_mut(order_id)?
            .cancel()
            .map_err(Self::map_lifecycle_error)?;

        self.validate_order_consistency(order_id)?;

        Ok(vec![Event::OrderCancelled(OrderCancelled::new(
            self.config.market_id(),
            order_id,
            expected_remaining,
        ))])
    }

    fn validate_execution_plan_lifecycles(&self, plan: &ExecutionPlan) -> Result<(), EngineError> {
        for planned_execution in plan.fills() {
            let order_id = planned_execution.maker_order_id();

            let Some(state) = self.validate_order_consistency(order_id)? else {
                return Err(EngineError::OrderLifecycleInvariantViolation);
            };

            if !state.is_active() || planned_execution.quantity().is_zero() {
                return Err(EngineError::OrderLifecycleInvariantViolation);
            }

            let remaining = self
                .lifecycles
                .get(order_id)
                .ok_or(EngineError::OrderLifecycleInvariantViolation)?
                .active_remaining()
                .map_err(Self::map_lifecycle_error)?
                .ok_or(EngineError::OrderLifecycleInvariantViolation)?;

            if planned_execution.quantity() > remaining {
                return Err(EngineError::OrderLifecycleInvariantViolation);
            }
        }

        Ok(())
    }

    /// Validates the lifecycle/book relationship for exactly one `OrderId`.
    ///
    /// This performs keyed lookups only. `HashMap` traversal order is irrelevant.
    fn validate_order_consistency(
        &self,
        order_id: OrderId,
    ) -> Result<Option<OrderState>, EngineError> {
        let lifecycle = self.lifecycles.get(order_id);
        let book_order = self.book.order(order_id);

        let Some(lifecycle) = lifecycle else {
            if book_order.is_some() {
                return Err(EngineError::OrderLifecycleInvariantViolation);
            }

            return Ok(None);
        };

        lifecycle.validate().map_err(Self::map_lifecycle_error)?;

        let state = lifecycle.state();

        match state {
            OrderState::New => {
                return Err(EngineError::OrderLifecycleInvariantViolation);
            }

            OrderState::Open | OrderState::PartiallyFilled => {
                let expected_remaining = lifecycle
                    .active_remaining()
                    .map_err(Self::map_lifecycle_error)?
                    .ok_or(EngineError::OrderLifecycleInvariantViolation)?;

                let actual_order =
                    book_order.ok_or(EngineError::OrderLifecycleInvariantViolation)?;

                if actual_order.quantity() != expected_remaining {
                    return Err(EngineError::OrderLifecycleInvariantViolation);
                }
            }

            OrderState::Filled | OrderState::Cancelled | OrderState::Rejected => {
                if book_order.is_some() {
                    return Err(EngineError::OrderLifecycleInvariantViolation);
                }
            }
        }

        Ok(Some(state))
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

    fn order_rejection_event(
        &self,
        command: PlaceLimitOrder,
        reason: OrderRejectionReason,
    ) -> Event {
        Event::OrderRejected(OrderRejected::new(
            self.config.market_id(),
            command.order_id(),
            reason,
        ))
    }

    fn cancel_rejection_event(&self, order_id: OrderId, reason: CancelRejectionReason) -> Event {
        Event::CancelRejected(CancelRejected::new(
            self.config.market_id(),
            order_id,
            reason,
        ))
    }

    fn insert_lifecycle(
        &mut self,
        order_id: OrderId,
        lifecycle: OrderLifecycle,
    ) -> Result<(), EngineError> {
        self.lifecycles
            .insert_finalized(order_id, lifecycle)
            .map_err(Self::map_lifecycle_error)
    }

    fn lifecycle_mut(&mut self, order_id: OrderId) -> Result<&mut OrderLifecycle, EngineError> {
        self.lifecycles
            .get_mut(order_id)
            .ok_or(EngineError::OrderLifecycleInvariantViolation)
    }

    fn map_lifecycle_error(_error: LifecycleError) -> EngineError {
        EngineError::OrderLifecycleInvariantViolation
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
