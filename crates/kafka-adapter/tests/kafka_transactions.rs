use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::executor::block_on;
use meow_matching_kafka_adapter::{
    KafkaRuntime,
    config::RuntimeConfig,
    contracts::{matching as pb, order as order_pb},
};
use prost::Message;
use rdkafka::{
    ClientConfig, Message as KafkaMessage,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, BaseRecord, Producer},
    topic_partition_list::{Offset, TopicPartitionList},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Exercises the real broker transaction boundary against the local Kafka cluster.
///
/// Run explicitly with:
///
/// `MEOW_KAFKA_TEST_BOOTSTRAP_SERVERS=localhost:29092,localhost:39092,localhost:49092 \
///  cargo test -p meow-matching-kafka-adapter --test kafka_transactions -- --ignored`
#[test]
#[ignore = "requires the local three-node Kafka cluster"]
fn committed_duplicate_advances_input_without_a_second_result() {
    let bootstrap = std::env::var("MEOW_KAFKA_TEST_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:29092,localhost:39092,localhost:49092".to_owned());
    let suffix = unique_suffix();
    let command_topic = format!("meow.matching.commands.test.{suffix}");
    let result_topic = format!("meow.matching.results.test.{suffix}");
    let group_id = format!("meow-matching-engine-test-{suffix}");
    let transactional_id_prefix = format!("meow-matching-engine-test-{suffix}");

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .expect("create Kafka admin client");
    create_test_topics(&admin, &command_topic, &result_topic);

    let shutdown = Arc::new(AtomicBool::new(false));
    let runtime_shutdown = Arc::clone(&shutdown);
    let runtime_config = RuntimeConfig {
        bootstrap_servers: bootstrap.clone(),
        command_topic: command_topic.clone(),
        result_topic: result_topic.clone(),
        group_id: group_id.clone(),
        transactional_id_prefix,
        poll_timeout: Duration::from_millis(50),
        kafka_operation_timeout: Duration::from_secs(10),
        retry_initial_delay: Duration::from_millis(20),
        retry_max_delay: Duration::from_millis(250),
    };

    let runtime_handle = thread::spawn(move || {
        let mut runtime = KafkaRuntime::new(runtime_config, runtime_shutdown)?;
        runtime.run()
    });

    let command_id = format!("cmd-{suffix}");
    let payload = place_command(&command_id).encode_to_vec();
    let input_producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("acks", "all")
        .create()
        .expect("create input producer");

    publish_command(&input_producer, &command_topic, &payload);
    publish_command(&input_producer, &command_topic, &payload);
    input_producer
        .flush(TEST_TIMEOUT)
        .expect("flush matching commands");

    wait_for_committed_offset(&bootstrap, &group_id, &command_topic, 2);

    let result_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", format!("result-observer-{suffix}"))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .create()
        .expect("create result observer");
    let mut result_assignment = TopicPartitionList::new();
    result_assignment
        .add_partition_offset(&result_topic, 0, Offset::Beginning)
        .expect("build result assignment");
    result_consumer
        .assign(&result_assignment)
        .expect("assign result observer");

    let matching_results = collect_results_for_command(&result_consumer, &command_id);

    shutdown.store(true, Ordering::Relaxed);
    let runtime_result = runtime_handle.join().expect("runtime thread panicked");
    runtime_result.expect("runtime stopped with an error");

    assert_eq!(
        matching_results, 1,
        "the transport duplicate must commit its own offset without publishing a second MatchingResult"
    );

    let _ = block_on(admin.delete_topics(
        &[command_topic.as_str(), result_topic.as_str()],
        &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
    ));
}

fn unique_suffix() -> String {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch");
    format!("{}-{}", std::process::id(), elapsed.as_nanos())
}

fn create_test_topics(
    admin: &AdminClient<DefaultClientContext>,
    command_topic: &str,
    result_topic: &str,
) {
    let topics = [
        NewTopic::new(command_topic, 1, TopicReplication::Fixed(3))
            .set("cleanup.policy", "delete")
            .set("retention.ms", "-1")
            .set("min.insync.replicas", "2"),
        NewTopic::new(result_topic, 1, TopicReplication::Fixed(3))
            .set("cleanup.policy", "delete")
            .set("retention.ms", "-1")
            .set("min.insync.replicas", "2"),
    ];

    let results = block_on(admin.create_topics(
        &topics,
        &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
    ))
    .expect("create isolated Kafka test topics");

    for result in results {
        result.expect("Kafka test topic creation failed");
    }
}

fn place_command(command_id: &str) -> pb::MatchingCommand {
    pb::MatchingCommand {
        command_id: command_id.to_owned(),
        market_id: 7,
        command: Some(pb::matching_command::Command::PlaceLimitOrder(
            pb::PlaceLimitOrder {
                order_id: 7001,
                side: order_pb::OrderSide::Buy as i32,
                price_ticks: 100,
                quantity_lots: 5,
            },
        )),
    }
}

fn publish_command(producer: &BaseProducer, topic: &str, payload: &[u8]) {
    producer
        .send(BaseRecord::to(topic).partition(0).key("7").payload(payload))
        .expect("enqueue matching command");
}

fn wait_for_committed_offset(bootstrap: &str, group_id: &str, topic: &str, expected: i64) {
    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .create()
        .expect("create committed-offset observer");
    let deadline = Instant::now() + TEST_TIMEOUT;

    loop {
        let mut requested = TopicPartitionList::new();
        requested.add_partition(topic, 0);
        let committed = observer
            .committed_offsets(requested, Duration::from_secs(2))
            .expect("read committed matching offset");
        let offset = committed
            .find_partition(topic, 0)
            .expect("partition must exist")
            .offset();

        if offset == Offset::Offset(expected) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for consumer-group offset {expected}; last value was {offset:?}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn collect_results_for_command(consumer: &BaseConsumer, command_id: &str) -> usize {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut count = 0;

    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                let Some(payload) = message.payload() else {
                    continue;
                };
                let result =
                    pb::MatchingResult::decode(payload).expect("valid MatchingResult protobuf");

                if result
                    .command
                    .as_ref()
                    .map(|command| command.command_id.as_str())
                    == Some(command_id)
                {
                    count += 1;
                }
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(0))) => return count,
            Some(Err(error)) => panic!("result observer failed: {error}"),
            None => {}
        }

        assert!(
            Instant::now() < deadline,
            "timed out while draining committed matching results"
        );
    }
}
