use std::{thread, time::Duration};

use rdkafka::{
    ClientConfig,
    consumer::ConsumerGroupMetadata,
    error::{KafkaError, RDKafkaError, RDKafkaErrorCode},
    producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer},
    topic_partition_list::TopicPartitionList,
};

use crate::config::RuntimeConfig;

pub(crate) type TxProducer = ThreadedProducer<DefaultProducerContext>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionErrorClass {
    Retriable,
    AbortRequired,
    Fatal,
}

pub(crate) fn create_transactional_producer(
    config: &RuntimeConfig,
    partition: i32,
) -> Result<TxProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("transactional.id", config.transactional_id(partition))
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("max.in.flight.requests.per.connection", "5")
        .set("message.send.max.retries", "2147483647")
        .set("retry.backoff.ms", "100")
        .set("transaction.timeout.ms", "60000")
        .set("message.timeout.ms", "30000")
        .set("allow.auto.create.topics", "false")
        .create()
}

pub(crate) fn classify_transaction_error(error: &KafkaError) -> TransactionErrorClass {
    let KafkaError::Transaction(transaction_error) = error else {
        return TransactionErrorClass::Fatal;
    };

    classify_rdkafka_transaction_error(transaction_error)
}

fn classify_rdkafka_transaction_error(error: &RDKafkaError) -> TransactionErrorClass {
    if error.is_fatal() {
        TransactionErrorClass::Fatal
    } else if error.txn_requires_abort() {
        TransactionErrorClass::AbortRequired
    } else if error.is_retriable() {
        TransactionErrorClass::Retriable
    } else {
        TransactionErrorClass::Fatal
    }
}

pub(crate) fn enqueue_result_with_backpressure(
    producer: &TxProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
    initial_delay: Duration,
    max_delay: Duration,
    should_stop: impl Fn() -> bool,
) -> Result<(), KafkaError> {
    let mut delay = initial_delay;

    loop {
        let record = BaseRecord::to(topic).key(key).payload(payload);

        match producer.send(record) {
            Ok(()) => return Ok(()),
            Err((error @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => {
                if should_stop() {
                    return Err(error);
                }

                thread::sleep(delay);
                delay = delay.saturating_mul(2).min(max_delay);
            }
            Err((error, _record)) => return Err(error),
        }
    }
}

pub(crate) fn send_offsets(
    producer: &TxProducer,
    offsets: &TopicPartitionList,
    group_metadata: &ConsumerGroupMetadata,
    timeout: Duration,
) -> Result<(), KafkaError> {
    producer.send_offsets_to_transaction(offsets, group_metadata, timeout)
}
