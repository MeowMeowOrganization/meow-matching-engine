use meow_matching_engine::{EngineConfig, MarketId, MatchingEngine, PriceTicks, QuantityLots};

#[test]
fn engine_can_be_created_for_one_market() {
    let config = EngineConfig::new(MarketId::new(42));
    let engine = MatchingEngine::new(config);

    assert_eq!(engine.config().market_id(), MarketId::new(42));
}

#[test]
fn canonical_financial_values_reject_negatives() {
    assert!(PriceTicks::new(-1).is_err());
    assert!(QuantityLots::new(-1).is_err());
}

#[test]
fn zero_quantity_is_representable_as_internal_state() {
    let quantity = QuantityLots::new(0).expect("zero remaining quantity is representable");

    assert!(quantity.is_zero());
}
