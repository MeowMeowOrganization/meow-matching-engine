use std::collections::{HashMap, hash_map::Entry};

use crate::domain::{OrderId, QuantityLots};

/// Deterministic lifecycle state for one canonical order identity.
///
/// `New` is transient and must never be persisted in `OrderLifecycleIndex`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    New,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl OrderState {
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Open | Self::PartiallyFilled)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

/// Minimal deterministic lifecycle information required by the matching core.
///
/// The durable exchange-wide order lifecycle remains owned by the Order Service.
/// This structure exists only because the matching engine must distinguish active, filled, cancelled, rejected and unknown orders deterministically.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderLifecycle {
    submitted_quantity: QuantityLots,
    executed_quantity: QuantityLots,
    state: OrderState,
}

impl OrderLifecycle {
    pub(crate) fn new(submitted_quantity: QuantityLots) -> Result<Self, LifecycleError> {
        if submitted_quantity.is_zero() {
            return Err(LifecycleError::InvalidInvariant);
        }

        Ok(Self {
            submitted_quantity,
            executed_quantity: QuantityLots::ZERO,
            state: OrderState::New,
        })
    }

    #[must_use]
    pub const fn submitted_quantity(self) -> QuantityLots {
        self.submitted_quantity
    }

    #[must_use]
    pub const fn executed_quantity(self) -> QuantityLots {
        self.executed_quantity
    }

    #[must_use]
    pub const fn state(self) -> OrderState {
        self.state
    }

    /// Finalizes successful placement after the matching plan is known.
    ///
    /// `remaining_quantity` is the taker's quantity after all planned executions.
    pub(crate) fn complete_placement(
        &mut self,
        remaining_quantity: QuantityLots,
    ) -> Result<(), LifecycleError> {
        self.validate()?;

        if self.state != OrderState::New {
            return Err(LifecycleError::InvalidTransition);
        }

        if remaining_quantity > self.submitted_quantity {
            return Err(LifecycleError::InvalidInvariant);
        }

        let executed_quantity = checked_sub_quantity(self.submitted_quantity, remaining_quantity)?;

        self.executed_quantity = executed_quantity;

        self.state = if remaining_quantity.is_zero() {
            OrderState::Filled
        } else if executed_quantity.is_zero() {
            OrderState::Open
        } else {
            OrderState::PartiallyFilled
        };

        self.validate()
    }

    /// Converts one transient NEW order into a terminal REJECTED order.
    ///
    /// This is valid only before any execution has occured.
    pub(crate) fn reject(&mut self) -> Result<(), LifecycleError> {
        self.validate()?;

        if self.state != OrderState::New || !self.executed_quantity.is_zero() {
            return Err(LifecycleError::InvalidTransition);
        }

        self.state = OrderState::Rejected;

        self.validate()
    }

    /// Applies one execution against an already-resting maker.
    pub(crate) fn apply_execution(
        &mut self,
        execution_quantity: QuantityLots,
    ) -> Result<(), LifecycleError> {
        self.validate()?;

        if !self.state.is_active() || execution_quantity.is_zero() {
            return Err(LifecycleError::InvalidTransition);
        }

        let updated_executed = checked_add_quantity(self.executed_quantity, execution_quantity)?;

        if updated_executed > self.submitted_quantity {
            return Err(LifecycleError::InvalidInvariant);
        }

        self.executed_quantity = updated_executed;

        self.state = if updated_executed == self.submitted_quantity {
            OrderState::Filled
        } else {
            OrderState::PartiallyFilled
        };

        self.validate()
    }

    /// Cancels the complete currently-resting remainder.
    ///
    /// Executed quantity is deliberately retained unchanged.
    pub(crate) fn cancel(&mut self) -> Result<(), LifecycleError> {
        self.validate()?;

        if !self.state.is_active() {
            return Err(LifecycleError::InvalidTransition);
        }

        self.state = OrderState::Cancelled;

        self.validate()
    }

    /// Returns the expected book quantity for an active order.
    pub(crate) fn active_remaining(&self) -> Result<Option<QuantityLots>, LifecycleError> {
        self.validate()?;

        if !self.state.is_active() {
            return Ok(None);
        }

        let remaining = checked_sub_quantity(self.submitted_quantity, self.executed_quantity)?;

        if remaining.is_zero() {
            return Err(LifecycleError::InvalidInvariant);
        }

        Ok(Some(remaining))
    }

    pub(crate) fn validate(&self) -> Result<(), LifecycleError> {
        if self.submitted_quantity.is_zero() || self.executed_quantity > self.submitted_quantity {
            return Err(LifecycleError::InvalidInvariant);
        }

        match self.state {
            OrderState::New | OrderState::Open | OrderState::Rejected => {
                if !self.executed_quantity.is_zero() {
                    return Err(LifecycleError::InvalidInvariant);
                }
            }

            OrderState::PartiallyFilled => {
                if self.executed_quantity.is_zero()
                    || self.executed_quantity >= self.submitted_quantity
                {
                    return Err(LifecycleError::InvalidInvariant);
                }
            }

            OrderState::Filled => {
                if self.executed_quantity != self.submitted_quantity {
                    return Err(LifecycleError::InvalidInvariant);
                }
            }

            OrderState::Cancelled => {
                if self.executed_quantity >= self.submitted_quantity {
                    return Err(LifecycleError::InvalidInvariant);
                }
            }
        }

        Ok(())
    }
}

/// Historical lifecycle index.
///
/// No iteration over this `HashMap` may influence deterministic output.
/// It exists only for keyed `OrderId` lookup.

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct OrderLifecycleIndex {
    orders: HashMap<OrderId, OrderLifecycle>,
}

impl OrderLifecycleIndex {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub(crate) fn get(&self, order_id: OrderId) -> Option<&OrderLifecycle> {
        self.orders.get(&order_id)
    }

    pub(crate) fn get_mut(&mut self, order_id: OrderId) -> Option<&mut OrderLifecycle> {
        self.orders.get_mut(&order_id)
    }

    /// Inserts a completed command outcome.
    ///
    /// `NEW` is deliberately forbidden here so transient state can never survive command processing.
    pub(crate) fn insert_finalized(
        &mut self,
        order_id: OrderId,
        lifecycle: OrderLifecycle,
    ) -> Result<(), LifecycleError> {
        lifecycle.validate()?;

        if lifecycle.state() == OrderState::New {
            return Err(LifecycleError::TransientStatePersisted);
        }

        match self.orders.entry(order_id) {
            Entry::Occupied(_) => Err(LifecycleError::DuplicateOrderId),
            Entry::Vacant(entry) => {
                entry.insert(lifecycle);
                Ok(())
            }
        }
    }

    pub(crate) fn canonical_entries(
        &self,
    ) -> Result<Vec<(OrderId, OrderLifecycle)>, LifecycleError> {
        // Native HashMap order is intentionally discarded before any entry becomes observable canonical output.
        let mut order_ids = self.orders.keys().copied().collect::<Vec<_>>();

        order_ids.sort_unstable_by_key(|order_id| order_id.get());

        let mut entries = Vec::with_capacity(order_ids.len());

        for order_id in order_ids {
            let lifecycle = *self
                .orders
                .get(&order_id)
                .ok_or(LifecycleError::InvalidInvariant)?;

            lifecycle.validate()?;

            if lifecycle.state() == OrderState::New {
                return Err(LifecycleError::TransientStatePersisted);
            }

            entries.push((order_id, lifecycle));
        }

        Ok(entries)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleError {
    InvalidInvariant,
    InvalidTransition,
    ArithmeticOverflow,
    CanonicalIntegerOutOfRange,
    DuplicateOrderId,
    TransientStatePersisted,
}

fn checked_add_quantity(
    left: QuantityLots,
    right: QuantityLots,
) -> Result<QuantityLots, LifecycleError> {
    let result = i128::from(left.get())
        .checked_add(i128::from(right.get()))
        .ok_or(LifecycleError::ArithmeticOverflow)?;

    let result = i64::try_from(result).map_err(|_| LifecycleError::CanonicalIntegerOutOfRange)?;

    QuantityLots::new(result).map_err(|_| LifecycleError::InvalidInvariant)
}

fn checked_sub_quantity(
    left: QuantityLots,
    right: QuantityLots,
) -> Result<QuantityLots, LifecycleError> {
    let result = i128::from(left.get())
        .checked_sub(i128::from(right.get()))
        .ok_or(LifecycleError::ArithmeticOverflow)?;

    let result = i64::try_from(result).map_err(|_| LifecycleError::CanonicalIntegerOutOfRange)?;

    QuantityLots::new(result).map_err(|_| LifecycleError::InvalidInvariant)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quantity(value: i64) -> QuantityLots {
        QuantityLots::new(value).expect("valid quantity")
    }

    #[test]
    fn new_to_open() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle
            .complete_placement(quantity(10))
            .expect("no execution leaves order open");

        assert_eq!(lifecycle.state(), OrderState::Open);
        assert_eq!(lifecycle.executed_quantity(), QuantityLots::ZERO);
    }

    #[test]
    fn new_to_partially_filled() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle
            .complete_placement(quantity(6))
            .expect("partial remainder");

        assert_eq!(lifecycle.state(), OrderState::PartiallyFilled,);
        assert_eq!(lifecycle.executed_quantity(), quantity(4));
    }

    #[test]
    fn new_to_filled() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle
            .complete_placement(QuantityLots::ZERO)
            .expect("fully executed");

        assert_eq!(lifecycle.state(), OrderState::Filled);
        assert_eq!(lifecycle.executed_quantity(), quantity(10));
    }

    #[test]
    fn new_to_rejected() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle.reject().expect("new order can reject");

        assert_eq!(lifecycle.state(), OrderState::Rejected);
        assert_eq!(lifecycle.executed_quantity(), QuantityLots::ZERO);
    }

    #[test]
    fn open_to_partial_to_filled() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle
            .complete_placement(quantity(10))
            .expect("rests open");

        lifecycle
            .apply_execution(quantity(4))
            .expect("partial execution");

        assert_eq!(lifecycle.state(), OrderState::PartiallyFilled,);
        assert_eq!(lifecycle.executed_quantity(), quantity(4));

        lifecycle
            .apply_execution(quantity(6))
            .expect("final execution");

        assert_eq!(lifecycle.state(), OrderState::Filled);
        assert_eq!(lifecycle.executed_quantity(), quantity(10));
    }

    #[test]
    fn cancelling_partial_order_preserves_executed_quantity() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle
            .complete_placement(quantity(10))
            .expect("rests open");

        lifecycle
            .apply_execution(quantity(4))
            .expect("partial execution");

        lifecycle.cancel().expect("active order can cancel");

        assert_eq!(lifecycle.state(), OrderState::Cancelled);
        assert_eq!(lifecycle.executed_quantity(), quantity(4));
    }

    #[test]
    fn terminal_state_cannot_execute_again() {
        let mut lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        lifecycle
            .complete_placement(QuantityLots::ZERO)
            .expect("filled");

        assert_eq!(
            lifecycle.apply_execution(quantity(1)),
            Err(LifecycleError::InvalidTransition),
        );
    }

    #[test]
    fn index_refuses_to_persist_new_to_state() {
        let lifecycle = OrderLifecycle::new(quantity(10)).expect("valid lifecycle");

        let mut index = OrderLifecycleIndex::new();

        assert_eq!(
            index.insert_finalized(OrderId::new(1), lifecycle),
            Err(LifecycleError::TransientStatePersisted),
        );
    }

    #[test]
    fn canonical_entries_ignore_hashmap_insertion_order() {
        fn finalized(quantity_value: i64) -> OrderLifecycle {
            let quantity = QuantityLots::new(quantity_value).expect("valid quantity");

            let mut lifecycle = OrderLifecycle::new(quantity).expect("valid lifecycle");

            lifecycle
                .complete_placement(quantity)
                .expect("becomes open");

            lifecycle
        }

        let mut first = OrderLifecycleIndex::new();

        first
            .insert_finalized(OrderId::new(30), finalized(3))
            .expect("insert");

        first
            .insert_finalized(OrderId::new(10), finalized(1))
            .expect("insert");

        first
            .insert_finalized(OrderId::new(20), finalized(2))
            .expect("insert");

        let mut second = OrderLifecycleIndex::new();

        second
            .insert_finalized(OrderId::new(20), finalized(2))
            .expect("insert");

        second
            .insert_finalized(OrderId::new(30), finalized(3))
            .expect("insert");

        second
            .insert_finalized(OrderId::new(10), finalized(1))
            .expect("insert");

        let first_entries = first.canonical_entries().expect("valid state");

        let second_entries = second.canonical_entries().expect("valid state");

        assert_eq!(first_entries, second_entries,);

        assert_eq!(
            first_entries
                .iter()
                .map(|(order_id, _)| *order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(10), OrderId::new(20), OrderId::new(30),],
        );
    }
}
