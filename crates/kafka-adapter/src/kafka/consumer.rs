use std::{collections::BTreeSet, time::Duration};

use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    topic_partition_list::{Offset, TopicPartitionList},
};

use crate::config::RuntimeConfig;

pub(crate) fn create_group_consumer(config: &RuntimeConfig) -> Result<BaseConsumer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("allow.auto.create.topics", "false")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("max.poll.interval.ms", "600000")
        .create()
}

pub(crate) fn assigned_partitions(
    consumer: &BaseConsumer,
    topic: &str,
) -> Result<BTreeSet<i32>, KafkaError> {
    let assignment = consumer.assignment()?;

    Ok(assignment
        .elements()
        .iter()
        .filter(|element| element.topic() == topic)
        .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
        .collect())
}

pub(crate) fn committed_next_offset(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<Option<i64>, KafkaError> {
    let mut requested = TopicPartitionList::new();
    requested.add_partition(topic, partition);

    let committed = consumer.committed_offsets(requested, timeout)?;
    let element = committed
        .find_partition(topic, partition)
        .expect("requested committed partition must be present");

    match element.offset() {
        Offset::Offset(offset) => Ok(Some(offset)),
        _ => Ok(None),
    }
}

pub(crate) fn seek_to(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    offset: i64,
    timeout: Duration,
) -> Result<(), KafkaError> {
    consumer.seek(topic, partition, Offset::Offset(offset), timeout)
}

pub(crate) fn pause_partition(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
) -> Result<(), KafkaError> {
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, partition);
    consumer.pause(&partitions)
}
