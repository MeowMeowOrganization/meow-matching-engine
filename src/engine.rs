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
    sequencing::{MarketSequence, ProcessResult},
    state::{self, StateHash},
};

/// Deterministic matching state for exactly one market.

#[derive(Debug, PartialEq, Eq)]
pub struct MatchingEngine {
    config: EngineConfig,
    book: OrderBook,
    lifecycles: OrderLifecycleIndex,

    /// Last successfully committed canonical command position.
    ///
    /// `0` means this engine has not yet committed any command.
    last_committed_sequence: MarketSequence,

    /// Fatal errors are fail-stop.
    ///
    /// This is operational engine-instance state rather than a business lifecycle state. A halted instance must be discarded and reconstructed from a known committed state before processing can resume.
    halted: bool,
}

impl MatchingEngine {
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            book: OrderBook::new(),
            lifecycles: OrderLifecycleIndex::new(),
            last_committed_sequence: MarketSequence::ZERO,
            halted: false,
        }
    }

    #[must_use]
    pub const fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Returns read-only access to current active book state.
    #[must_use]
    pub const fn book(&self) -> &OrderBook {
        &self.book
    }

    /// Last successfully committed per-market command sequence.
    #[must_use]
    pub const fn last_committed_sequence(&self) -> MarketSequence {
        self.last_committed_sequence
    }

    /// Returns whether this engine instance has encountered a fatal error and must no longer process commands.
    #[must_use]
    pub const fn is_halted(&self) -> bool {
        self.halted
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
    /// The command has already been ordered and deduplicated upstream.
    /// `MatchingEngine` performs no `CommandId` generation or deduplication.
    ///
    /// A successful call commits exactly one [`MarketSequence`], including:
    ///
    /// - zero-event commands,
    /// - placement rejection,
    /// - cancellation rejection,
    /// - single-event commands,
    /// - multi-event matching sweeps.
    ///
    /// Sequence commit occurs only after the complete deterministic command outcome and event envelopes have been constructed successfully.
    ///
    /// # Errors
    ///
    /// Any [`EngineError`] is fatal for this engine instance. The candidate market sequence is not committed and subsequent calls return [`EngineError:EngineHalted`].
    pub fn process(&mut self, command: Command) -> Result<ProcessResult, EngineError> {
        if self.halted {
            return Err(EngineError::EngineHalted);
        }

        let Some(candidate_sequence) = self.last_committed_sequence.checked_next() else {
            return Err(self.halt(EngineError::SequenceOverflow));
        };

        let events = match self.process_command(command) {
            Ok(events) => events,
            Err(error) => {
                return Err(self.halt(error));
            }
        };

        let process_result =
            match ProcessResult::new(self.config.market_id(), candidate_sequence, events) {
                Ok(result) => result,
                Err(error) => {
                    return Err(self.halt(error));
                }
            };

        // COMMIT
        //
        // Nothing after this point can fall.
        self.last_committed_sequence = candidate_sequence;

        Ok(process_result)
    }

    fn process_command(&mut self, command: Command) -> Result<Vec<Event>, EngineError> {
        match command {
            Command::PlaceLimitOrder(place_order) => self.process_limit_order(place_order),

            Command::CancelOrder(cancel_order) => self.process_cancel_order(cancel_order),
        }
    }

    fn process_limit_order(&mut self, command: PlaceLimitOrder) -> Result<Vec<Event>, EngineError> {
        // This is an order-lifecycle rule, not CommandId deduplication.
        if self
            .validate_order_consistency(command.order_id())?
            .is_some()
        {
            return Ok(vec![self.order_rejection_event(
                command,
                OrderRejectionReason::DuplicateOrderId,
            )]);
        }

        // NEW is transient and local.
        //
        // It is never inserted into OrderLifecycleIndex.
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

        // Persist the now-finalized incoming lifecycle only after NEW has transitioned int Open / PartiallyFilled / Filled.
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

    fn halt(&mut self, error: EngineError) -> EngineError {
        self.halted = true;
        error
    }

    /// Produces the exact canonical semantic-state representation for this committed matching-engine state.
    ///
    /// The returned bytes are independent of `HashMap` seeds, allocation layout, compiler object layout and host endianness.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine is halted or if physical state violates the invariants required to construct canonical semantic state.
    pub fn canonical_state_bytes(&self) -> Result<Vec<u8>, EngineError> {
        if self.halted {
            return Err(EngineError::EngineHalted);
        }

        let lifecycles = self
            .lifecycles
            .canonical_entries()
            .map_err(Self::map_lifecycle_error)?;

        self.validate_canonical_state(&lifecycles)?;

        state::canonical_state_bytes(
            self.config.market_id(),
            self.last_committed_sequence,
            &self.book,
            &lifecycles,
        )
    }

    /// Returns the SHA-256 fingerprint of this engine's exact canonical semantic state.
    ///
    /// # Errors
    ///
    /// Returns an error when canonical state cannot safely be produced.
    pub fn state_hash(&self) -> Result<StateHash, EngineError> {
        let bytes = self.canonical_state_bytes()?;

        Ok(state::state_hash(&bytes))
    }

    fn validate_canonical_state(
        &self,
        lifecycles: &[(OrderId, OrderLifecycle)],
    ) -> Result<(), EngineError> {
        // Every resting order must have an active lifecycle record.
        //
        // Price/FIFO traversal is deterministic and performs keyed lookup only.
        for level in self.book.bid_levels().chain(self.book.ask_levels()) {
            for order_id in level.order_ids() {
                let state = self
                    .validate_order_consistency(order_id)?
                    .ok_or(EngineError::OrderLifecycleInvariantViolation)?;

                if !state.is_active() {
                    return Err(EngineError::OrderLifecycleInvariantViolation);
                }
            }
        }

        // Every lifecycle record must agree with its corresponding book presence or absence.
        //
        // `lifecycles` has already been sorted by OrderId.
        for (order_id, _) in lifecycles {
            self.validate_order_consistency(*order_id)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{MarketId, PriceTicks, QuantityLots, Side},
        sequencing::EventOrdinal,
    };

    fn test_config() -> EngineConfig {
        EngineConfig::new(MarketId::new(1))
    }

    fn test_place_command(order_id: u64) -> Command {
        PlaceLimitOrder::new(
            OrderId::new(order_id),
            Side::Buy,
            PriceTicks::new(100).expect("positive price"),
            QuantityLots::new(1).expect("positive quantity"),
        )
        .expect("valid command")
        .into()
    }

    #[test]
    fn engine_owns_exactly_one_market_configuration() {
        let engine = MatchingEngine::new(test_config());

        assert_eq!(engine.config().market_id(), MarketId::new(1));

        assert!(engine.book().is_empty());
        assert_eq!(engine.last_committed_sequence(), MarketSequence::ZERO,);
        assert!(!engine.is_halted());
    }

    #[test]
    fn first_successful_command_commits_sequence_one() {
        let mut engine = MatchingEngine::new(test_config());

        let result = engine
            .process(test_place_command(1))
            .expect("engine remains healthy");

        assert_eq!(result.market_sequence(), MarketSequence::new(1),);

        assert_eq!(engine.last_committed_sequence(), MarketSequence::new(1),);

        // No opposite liquidity existed, but the command still committed.
        assert!(result.events().is_empty());
    }

    #[test]
    fn deterministic_business_rejection_commits_sequence() {
        let mut engine = MatchingEngine::new(test_config());

        engine
            .process(test_place_command(1))
            .expect("first command commits");

        let rejection = engine
            .process(test_place_command(1))
            .expect("business rejection is successful processing");

        assert_eq!(rejection.market_sequence(), MarketSequence::new(2),);

        assert_eq!(engine.last_committed_sequence(), MarketSequence::new(2),);

        assert_eq!(rejection.events().len(), 1);

        let sequenced_event = rejection.events()[0];

        assert_eq!(
            sequenced_event.id().market_sequence(),
            MarketSequence::new(2),
        );

        assert_eq!(sequenced_event.id().event_ordinal(), EventOrdinal::ZERO,);

        let Event::OrderRejected(order_rejected) = sequenced_event.into_event() else {
            panic!("expected duplicate-order rejection");
        };

        assert_eq!(
            order_rejected.reason(),
            OrderRejectionReason::DuplicateOrderId,
        );
    }

    #[test]
    fn sequence_overflow_does_not_commit_and_halts_engine() {
        let mut engine = MatchingEngine::new(test_config());

        engine.last_committed_sequence = MarketSequence::new(u64::MAX);

        let result = engine.process(test_place_command(1));

        assert_eq!(result, Err(EngineError::SequenceOverflow),);

        assert_eq!(
            engine.last_committed_sequence(),
            MarketSequence::new(u64::MAX),
        );

        assert!(engine.is_halted());

        assert_eq!(
            engine.process(test_place_command(2)),
            Err(EngineError::EngineHalted),
        );
    }
}
