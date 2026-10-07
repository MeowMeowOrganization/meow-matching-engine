use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    topic_partition_list::{Offset, TopicPartitionList},
};
use thiserror::Error;

use crate::{
    codec::{DecodeError, decode_record},
    config::RuntimeConfig,
    state::{PartitionState, PartitionStateError},
};

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("failed to create recovery consumer: {0}")]
    ConsumerCreate(#[source] KafkaError),

    #[error("failed to read command-topic watermarks: {0}")]
    Watermarks(#[source] KafkaError),

    #[error(
        "authoritative command history has been truncated: earliest offset is {earliest}, expected 0"
    )]
    HistoryTruncated { earliest: i64 },

    #[error("committed offset {committed} is beyond command-topic high watermark {high}")]
    CommittedBeyondHighWatermark { committed: i64, high: i64 },

    #[error("failed to assign recovery consumer: {0}")]
    Assign(#[source] KafkaError),

    #[error(
        "recovery encountered malformed/corrupt command at partition {partition} offset {offset}: {source}"
    )]
    Decode {
        partition: i32,
        offset: i64,
        #[source]
        source: DecodeError,
    },

    #[error(
        "recovery encountered deterministic-state failure at partition {partition} offset {offset}: {source}"
    )]
    State {
        partition: i32,
        offset: i64,
        #[source]
        source: PartitionStateError,
    },

    #[error("fatal Kafka error while replaying command history: {0}")]
    ConsumerFatal(#[source] KafkaError),

    #[error("shutdown requested during state reconstruction")]
    Shutdown,
}

/// Rebuilds one input partition's engine and transport-dedupe state from the
/// committed authoritative command history without republishing results.
///
/// # Errors
///
/// Returns [`RecoveryError`] if full history is unavailable, a replayed command
/// is corrupt, deterministic replay fails, Kafka cannot provide the history, or
/// shutdown is requested before reconstruction completes.
pub fn rebuild_partition(
    config: &RuntimeConfig,
    partition: i32,
    committed_next_offset: i64,
    shutdown: &AtomicBool,
) -> Result<PartitionState, RecoveryError> {
    let replay: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("group.id", format!("{}-recovery", config.group_id))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .set("allow.auto.create.topics", "false")
        .create()
        .map_err(RecoveryError::ConsumerCreate)?;

    let (earliest, high) = replay
        .fetch_watermarks(
            &config.command_topic,
            partition,
            config.kafka_operation_timeout,
        )
        .map_err(RecoveryError::Watermarks)?;

    if earliest != 0 {
        return Err(RecoveryError::HistoryTruncated { earliest });
    }

    if committed_next_offset > high {
        return Err(RecoveryError::CommittedBeyondHighWatermark {
            committed: committed_next_offset,
            high,
        });
    }

    let mut state = PartitionState::new();

    if committed_next_offset == 0 {
        return Ok(state);
    }

    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&config.command_topic, partition, Offset::Beginning)
        .map_err(RecoveryError::Assign)?;
    replay.assign(&assignment).map_err(RecoveryError::Assign)?;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Err(RecoveryError::Shutdown);
        }

        match replay.poll(config.poll_timeout) {
            Some(Ok(message)) => {
                let offset = message.offset();

                if offset >= committed_next_offset {
                    break;
                }

                let decoded =
                    decode_record(message.key(), message.payload()).map_err(|source| {
                        RecoveryError::Decode {
                            partition,
                            offset,
                            source,
                        }
                    })?;

                state
                    .replay(&decoded)
                    .map_err(|source| RecoveryError::State {
                        partition,
                        offset,
                        source,
                    })?;
            }
            Some(Err(KafkaError::PartitionEOF(_))) => break,
            Some(Err(error @ KafkaError::MessageConsumptionFatal(_))) => {
                return Err(RecoveryError::ConsumerFatal(error));
            }
            Some(Err(_error)) => {
                // Kafka availability failures are backpressure, not permission to skip.
                // The command history remains authoritative, so retry the same replay position.
                thread::sleep(Duration::from_millis(100));
            }
            None => {}
        }
    }

    Ok(state)
}
