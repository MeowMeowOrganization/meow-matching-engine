use crate::error::DomainError;

/// A canonical market price expressed as a count of configured price ticks.
/// 
/// This type never represents a human decimal price, quote-asset atoms or floating-point value.
/// 
/// The broad canonical invariant is `value >= 0`.
/// Individual commands may impose stricter invariants, such as requiring a limit-order price to be greater than zero.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PriceTicks(i64);

impl PriceTicks {
	pub const ZERO: Self = Self(0);

	/// Creates a canonical price-tick value.
	/// 
	/// # Errors
	/// 
	/// Returns [`DomainError::NegativePriceTicks`] when `value` is negative.
	pub const fn new(value: i64) -> Result<Self, DomainError> {
		if value < 0 {
			return Err(DomainError::NegativePriceTicks(value));
		}

		Ok(Self(value))
	}

	#[must_use]
	pub const fn get(self) -> i64 {
		self.0
	}

	#[must_use]
	pub const fn is_zero(self) -> bool {
		self.0 == 0
	}
}

/// A canonical order quantity expressed as a count of configured quantity lots.
/// 
/// This type deliberately permits zero because internal matching state must be able to represent a fully filled order whose remaining quantity is zero.
/// 
/// Order admission must separately require submitted quantities to be greater than zero.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuantityLots(i64);

impl QuantityLots {
	pub const ZERO: Self = Self(0);

	/// Creates a canonical quantity-lot value.
	/// 
	/// # Errors
	/// 
	/// Returns [`DomainError::NegativeQuantityLots`] when `value` is negative.
	pub const fn new(value: i64) -> Result<Self, DomainError> {
		if value < 0 {
			return Err(DomainError::NegativeQuantityLots(value));
		}

		Ok(Self(value))
	}

	#[must_use]
	pub const fn get(self) -> i64 {
		self.0
	}

	#[must_use]
	pub const fn is_zero(self) -> bool {
		self.0 == 0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn price_ticks_accept_zero() {
		let price = PriceTicks::new(0).expect("zero is canonical");

		assert_eq!(price, PriceTicks::ZERO);
		assert!(price.is_zero());
	}

	#[test]
	fn price_ticks_accept_positive_values() {
		let price = PriceTicks::new(6_743_218).expect("positive ticks are valid");

		assert_eq!(price.get(), 6_743_218);
	}

	#[test]
	fn price_ticks_reject_negative_values() {
		assert_eq!(
			PriceTicks::new(-1),
			Err(DomainError::NegativePriceTicks(-1)),
		);
	}

	#[test]
	fn quantity_lots_accept_zero_for_internal_state() {
		let quantity = QuantityLots::new(0).expect("zero is canonical");

		assert_eq!(quantity, QuantityLots::ZERO);
		assert!(quantity.is_zero());
	}

	#[test]
	fn quantity_lots_reject_negative_values() {
		assert_eq!(
			QuantityLots::new(-1),
			Err(DomainError::NegativeQuantityLots(-1)),
		);
	}

	#[test]
	fn canonical_types_accept_i64_max() {
		assert_eq!(
			PriceTicks::new(i64::MAX)
				.expect("i64::MAX is inside the canonical representation")
				.get(),
			i64::MAX,
		);

		assert_eq!(
			QuantityLots::new(i64::MAX)
				.expect("i64::MAX is inside the canonical representation")
				.get(),
			i64::MAX,
		);
	}
}