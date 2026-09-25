#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketId(u32);

impl MarketId {
	#[must_use]
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	#[must_use]
	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderId(u64);

impl OrderId {
	#[must_use]
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	#[must_use]
	pub const fn get(self) -> u64 {
		self.0
	}
}