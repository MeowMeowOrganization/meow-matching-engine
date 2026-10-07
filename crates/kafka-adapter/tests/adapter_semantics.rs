use meow_matching_engine::{MarketId, MarketSequence};
use meow_matching_kafka_adapter::{
    codec::{decode_record, encode_matching_result},
    contracts::{matching as pb, order as order_pb},
    state::{DedupeDecision, LiveProcessOutcome, PartitionState, PartitionStateError},
};
use prost::Message;

fn place(command_id: &str, order_id: u64, price_ticks: i64) -> (Vec<u8>, Vec<u8>) {
    let command = pb::MatchingCommand {
        command_id: command_id.to_owned(),
        market_id: 7,
        command: Some(pb::matching_command::Command::PlaceLimitOrder(
            pb::PlaceLimitOrder {
                order_id,
                side: order_pb::OrderSide::Buy as i32,
                price_ticks,
                quantity_lots: 5,
            },
        )),
    };

    (b"7".to_vec(), command.encode_to_vec())
}

#[test]
fn exact_transport_duplicate_does_not_advance_market_sequence() {
    let (key, payload) = place("cmd-1", 100, 10);
    let decoded = decode_record(Some(&key), Some(&payload)).expect("valid command");
    let mut state = PartitionState::new();

    let first = state
        .process_live(&decoded)
        .expect("first processing succeeds");
    let LiveProcessOutcome::New(first) = first else {
        panic!("first command must be new");
    };
    assert_eq!(
        first.process_result().market_sequence(),
        MarketSequence::new(1)
    );

    let second = state
        .process_live(&decoded)
        .expect("duplicate is valid transport input");
    assert!(matches!(second, LiveProcessOutcome::Duplicate));
    assert_eq!(
        state
            .engine(MarketId::new(7))
            .expect("market engine exists")
            .last_committed_sequence(),
        MarketSequence::new(1)
    );
}

#[test]
fn conflicting_command_id_reuse_is_fatal_corruption() {
    let (key, first_payload) = place("cmd-1", 100, 10);
    let (_, conflicting_payload) = place("cmd-1", 101, 10);
    let first = decode_record(Some(&key), Some(&first_payload)).expect("valid first command");
    let conflict =
        decode_record(Some(&key), Some(&conflicting_payload)).expect("valid second envelope");
    let mut state = PartitionState::new();

    state.process_live(&first).expect("first command succeeds");

    let error = state.process_live(&conflict).expect_err("reuse must fail");
    assert_eq!(
        error,
        PartitionStateError::ConflictingCommandId {
            command_id: "cmd-1".to_owned()
        }
    );
}

#[test]
fn reconstruct_on_abort_removes_provisional_semantic_commit() {
    let (key, first_payload) = place("cmd-1", 100, 10);
    let (_, second_payload) = place("cmd-2", 101, 11);
    let first = decode_record(Some(&key), Some(&first_payload)).expect("valid first command");
    let second = decode_record(Some(&key), Some(&second_payload)).expect("valid second command");

    let mut live = PartitionState::new();
    live.process_live(&first)
        .expect("first command commits in memory");
    let provisional = live
        .process_live(&second)
        .expect("second command mutates provisionally");
    let LiveProcessOutcome::New(provisional) = provisional else {
        panic!("second command must be new");
    };
    assert_eq!(
        provisional.process_result().market_sequence(),
        MarketSequence::new(2)
    );

    // Simulate Kafka abort: discard the mutated state and replay only the durable
    // command history, whose committed boundary still ends after cmd-1.
    let mut rebuilt = PartitionState::new();
    assert_eq!(
        rebuilt.replay(&first).expect("replay committed cmd-1"),
        DedupeDecision::New
    );
    assert_eq!(
        rebuilt
            .engine(MarketId::new(7))
            .expect("rebuilt market")
            .last_committed_sequence(),
        MarketSequence::new(1)
    );

    let retried = rebuilt
        .process_live(&second)
        .expect("cmd-2 can be reconsidered only against rebuilt state");
    let LiveProcessOutcome::New(retried) = retried else {
        panic!("reconsidered cmd-2 must be new");
    };
    assert_eq!(
        retried.process_result().market_sequence(),
        MarketSequence::new(2)
    );
}

#[test]
fn zero_event_command_still_produces_a_committed_process_result() {
    let (key, payload) = place("cmd-zero-events", 500, 100);
    let decoded = decode_record(Some(&key), Some(&payload)).expect("valid command");
    let mut state = PartitionState::new();

    let outcome = state.process_live(&decoded).expect("matching succeeds");
    let LiveProcessOutcome::New(result) = outcome else {
        panic!("command is new");
    };

    assert_eq!(
        result.process_result().market_sequence(),
        MarketSequence::new(1)
    );
    assert!(result.process_result().events().is_empty());
}

#[test]
fn result_serialization_preserves_zero_event_semantic_commit_and_state_hash() {
    let (key, payload) = place("cmd-result", 900, 123);
    let decoded = decode_record(Some(&key), Some(&payload)).expect("valid command");
    let mut state = PartitionState::new();

    let outcome = state.process_live(&decoded).expect("matching succeeds");
    let LiveProcessOutcome::New(result) = outcome else {
        panic!("command is new");
    };

    let encoded =
        encode_matching_result(decoded.wire(), result.process_result(), result.state_hash());
    let wire_result =
        pb::MatchingResult::decode(encoded.as_slice()).expect("valid result protobuf");

    assert_eq!(
        wire_result
            .command
            .as_ref()
            .expect("result echoes command")
            .command_id,
        "cmd-result"
    );
    assert_eq!(wire_result.market_sequence, 1);
    assert!(wire_result.events.is_empty());

    let state_hash = wire_result
        .state_hash_after
        .expect("result carries post-command state hash");
    assert_eq!(
        state_hash.encoding_version,
        u32::from(meow_matching_engine::StateHash::ENCODING_VERSION)
    );
    assert_eq!(state_hash.sha256.len(), 32);
}
