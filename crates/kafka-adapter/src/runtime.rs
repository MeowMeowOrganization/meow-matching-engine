use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use rdkafka::{
    Message,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    message::OwnedMessage,
    producer::Producer,
    topic_partition_list::{Offset, TopicPartitionList},
};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{
    codec::{decode_record, encode_matching_result, market_key},
    config::RuntimeConfig,
    kafka::{
        consumer::{
            assigned_partitions, committed_next_offset, create_group_consumer, pause_partition,
            seek_to,
        },
        producer::{
            TransactionErrorClass, TxProducer, classify_transaction_error,
            create_transactional_producer, enqueue_result_with_backpressure, send_offsets,
        },
    },
    recovery::{RecoveryError, rebuild_partition},
    state::{LiveProcessOutcome, PartitionState, PartitionStateError},
};

struct PartitionRuntime {
    producer: TxProducer,
    state: Option<PartitionState>,
    expected_next_offset: Option<i64>,
    blocked: bool,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to create Kafka group consumer: {0}")]
    ConsumerCreate(#[source] KafkaError),

    #[error("failed to subscribe to matching command topic: {0}")]
    Subscribe(#[source] KafkaError),

    #[error("failed to inspect current Kafka assignment: {0}")]
    Assignment(#[source] KafkaError),

    #[error(
        "failed to create transactional Kafka producer for input partition {partition}: {source}"
    )]
    ProducerCreate {
        partition: i32,
        #[source]
        source: KafkaError,
    },

    #[error("fatal transactional Kafka error for input partition {partition}: {source}")]
    FatalTransaction {
        partition: i32,
        #[source]
        source: KafkaError,
    },

    #[error("failed to fetch committed input offset for partition {partition}: {source}")]
    CommittedOffset {
        partition: i32,
        #[source]
        source: KafkaError,
    },

    #[error("failed to seek input partition {partition}: {source}")]
    Seek {
        partition: i32,
        #[source]
        source: KafkaError,
    },

    #[error("failed to pause corrupt/fatal input partition {partition}: {source}")]
    Pause {
        partition: i32,
        #[source]
        source: KafkaError,
    },

    #[error("failed to enqueue matching result for input partition {partition}: {source}")]
    ResultEnqueue {
        partition: i32,
        #[source]
        source: KafkaError,
    },

    #[error("input offset {offset} cannot be advanced to O + 1")]
    OffsetOverflow { offset: i64 },

    #[error("failed to build transactional offset list: {0}")]
    OffsetList(#[source] KafkaError),

    #[error("state reconstruction failed for partition {partition}: {source}")]
    Recovery {
        partition: i32,
        #[source]
        source: RecoveryError,
    },

    #[error("fatal Kafka consumer error: {0}")]
    ConsumerFatal(#[source] KafkaError),
}

/// Correctness-first consume -> process -> produce runtime.
///
/// The first implementation intentionally has one application-level in-flight
/// record per process. This is hard backpressure: no later record can outrun an
/// unresolved transaction. Horizontal parallelism still comes from Kafka group
/// members owning different input partitions.
pub struct KafkaRuntime {
    config: RuntimeConfig,
    consumer: BaseConsumer,
    partitions: HashMap<i32, PartitionRuntime>,
    shutdown: Arc<AtomicBool>,
}

impl KafkaRuntime {
    /// Creates the group consumer and subscribes to the authoritative command topic.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if the Kafka consumer cannot be created or the
    /// command-topic subscription cannot be installed.
    pub fn new(config: RuntimeConfig, shutdown: Arc<AtomicBool>) -> Result<Self, RuntimeError> {
        let consumer = create_group_consumer(&config).map_err(RuntimeError::ConsumerCreate)?;
        consumer
            .subscribe(&[&config.command_topic])
            .map_err(RuntimeError::Subscribe)?;

        Ok(Self {
            config,
            consumer,
            partitions: HashMap::new(),
            shutdown,
        })
    }

    /// Runs the synchronous correctness-first consume/process/produce loop.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when an operational failure prevents the runtime
    /// from preserving the transactional processing invariant. Corrupt command
    /// records and deterministic input failures instead block only their
    /// affected partition without committing past the offending offset.
    pub fn run(&mut self) -> Result<(), RuntimeError> {
        info!(
            command_topic = %self.config.command_topic,
            result_topic = %self.config.result_topic,
            group_id = %self.config.group_id,
            "matching Kafka runtime started"
        );

        while !self.shutdown.load(Ordering::Relaxed) {
            let polled = self
                .consumer
                .poll(self.config.poll_timeout)
                .map(|result| result.map(|message| message.detach()));

            self.sync_assignment()?;

            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            match polled {
                Some(Ok(message)) => self.process_message(&message)?,
                Some(Err(KafkaError::PartitionEOF(_))) | None => {}
                Some(Err(error @ KafkaError::MessageConsumptionFatal(_))) => {
                    return Err(RuntimeError::ConsumerFatal(error));
                }
                Some(Err(error)) => {
                    warn!(%error, "Kafka consumer poll failed; no offset is advanced");
                    thread::sleep(self.config.retry_initial_delay);
                }
            }
        }

        info!("shutdown requested; no new Kafka command will be started");
        self.consumer.unsubscribe();
        self.partitions.clear();

        Ok(())
    }

    fn sync_assignment(&mut self) -> Result<(), RuntimeError> {
        let assigned = assigned_partitions(&self.consumer, &self.config.command_topic)
            .map_err(RuntimeError::Assignment)?;
        let existing: BTreeSet<i32> = self.partitions.keys().copied().collect();

        for partition in existing.difference(&assigned).copied().collect::<Vec<_>>() {
            info!(
                partition,
                "input partition revoked; discarding reconstructable memory"
            );
            self.partitions.remove(&partition);
        }

        for partition in assigned.difference(&existing).copied().collect::<Vec<_>>() {
            info!(
                partition,
                "input partition assigned; fencing prior transactional owner"
            );
            let producer = create_transactional_producer(&self.config, partition)
                .map_err(|source| RuntimeError::ProducerCreate { partition, source })?;
            self.init_transactions(partition, &producer)?;

            self.partitions.insert(
                partition,
                PartitionRuntime {
                    producer,
                    state: None,
                    expected_next_offset: None,
                    blocked: false,
                },
            );
        }

        Ok(())
    }

    fn init_transactions(&self, partition: i32, producer: &TxProducer) -> Result<(), RuntimeError> {
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                return Ok(());
            }

            match producer.init_transactions(self.config.kafka_operation_timeout) {
                Ok(()) => return Ok(()),
                Err(error) => match classify_transaction_error(&error) {
                    TransactionErrorClass::Retriable => {
                        warn!(partition, %error, "init_transactions retriable; retrying");
                        thread::sleep(self.config.retry_initial_delay);
                    }
                    TransactionErrorClass::AbortRequired | TransactionErrorClass::Fatal => {
                        return Err(RuntimeError::FatalTransaction {
                            partition,
                            source: error,
                        });
                    }
                },
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn process_message(&mut self, message: &OwnedMessage) -> Result<(), RuntimeError> {
        let partition = message.partition();

        let Some(partition_runtime) = self.partitions.get(&partition) else {
            // A message can be detached from the poll that also completed a rebalance.
            // If ownership no longer exists, never process or commit it.
            warn!(
                partition,
                offset = message.offset(),
                "discarding message detached during partition revocation"
            );
            return Ok(());
        };

        if partition_runtime.blocked {
            return Ok(());
        }

        if !self.ensure_recovered_at(partition, message.offset())? {
            return Ok(());
        }

        let decoded = match decode_record(message.key(), message.payload()) {
            Ok(decoded) => decoded,
            Err(error) => {
                error!(partition, offset = message.offset(), %error, "corrupt matching input blocks this partition");
                self.block_partition(partition, message.offset())?;
                return Ok(());
            }
        };

        self.begin_transaction(partition)?;

        let outcome = {
            let state = self
                .partitions
                .get_mut(&partition)
                .and_then(|runtime| runtime.state.as_mut())
                .expect("partition must be recovered before processing");
            state.process_live(&decoded)
        };

        let (engine_mutated, result_payload) = match outcome {
            Ok(LiveProcessOutcome::Duplicate) => (false, None),
            Ok(LiveProcessOutcome::New(result)) => {
                let payload = encode_matching_result(
                    decoded.wire(),
                    result.process_result(),
                    result.state_hash(),
                );
                (true, Some(payload))
            }
            Err(error) => {
                self.abort_transaction(partition)?;

                // `process_live` may already have changed engine memory before a
                // fail-stop EngineError surfaced. The aborted Kafka transaction
                // therefore invalidates the whole provisional partition state.
                // Rebuild the last committed state before parking the bad input.
                self.recover_after_abort(partition)?;

                match error {
                    PartitionStateError::ConflictingCommandId { .. }
                    | PartitionStateError::Engine(_) => {
                        error!(partition, offset = message.offset(), %error, "fatal matching input/state error blocks this partition");
                        self.block_partition(partition, message.offset())?;
                        return Ok(());
                    }
                }
            }
        };

        if let Some(payload) = result_payload.as_deref() {
            let key = market_key(decoded.market_id());
            let producer = &self
                .partitions
                .get(&partition)
                .expect("assigned partition runtime")
                .producer;

            if let Err(error) = enqueue_result_with_backpressure(
                producer,
                &self.config.result_topic,
                &key,
                payload,
                self.config.retry_initial_delay,
                self.config.retry_max_delay,
                || self.shutdown.load(Ordering::Relaxed),
            ) {
                self.abort_transaction(partition)?;
                if engine_mutated {
                    self.recover_after_abort(partition)?;
                }
                return self.transaction_enqueue_failure(partition, error);
            }
        }

        let next_offset = message
            .offset()
            .checked_add(1)
            .ok_or(RuntimeError::OffsetOverflow {
                offset: message.offset(),
            })?;

        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(
                &self.config.command_topic,
                partition,
                Offset::Offset(next_offset),
            )
            .map_err(RuntimeError::OffsetList)?;

        if let Err(outcome) = self.send_offsets_with_retry(partition, &offsets) {
            return self.handle_post_mutation_transaction_failure(
                partition,
                message.offset(),
                engine_mutated,
                outcome,
            );
        }

        if let Err(outcome) = self.commit_with_retry(partition) {
            return self.handle_post_mutation_transaction_failure(
                partition,
                message.offset(),
                engine_mutated,
                outcome,
            );
        }

        let runtime = self
            .partitions
            .get_mut(&partition)
            .expect("assigned partition runtime");
        runtime.expected_next_offset = Some(next_offset);

        info!(
            partition,
            offset = message.offset(),
            next_offset,
            command_id = decoded.command_id(),
            duplicate = !engine_mutated,
            "Kafka transaction committed; input record is safely processed"
        );

        Ok(())
    }

    fn ensure_recovered_at(
        &mut self,
        partition: i32,
        message_offset: i64,
    ) -> Result<bool, RuntimeError> {
        let expected = self
            .partitions
            .get(&partition)
            .and_then(|runtime| runtime.expected_next_offset);

        if expected == Some(message_offset) {
            return Ok(true);
        }

        let committed = self.committed_boundary(partition)?;
        let state = match rebuild_partition(&self.config, partition, committed, &self.shutdown) {
            Ok(state) => state,
            Err(RecoveryError::Shutdown) if self.shutdown.load(Ordering::Relaxed) => {
                return Ok(false);
            }
            Err(source) => return Err(RuntimeError::Recovery { partition, source }),
        };

        let runtime = self
            .partitions
            .get_mut(&partition)
            .expect("assigned partition runtime");
        runtime.state = Some(state);
        runtime.expected_next_offset = Some(committed);

        if message_offset != committed {
            warn!(
                partition,
                message_offset,
                committed,
                "consumer position does not match durable group boundary; seeking to committed offset"
            );
            seek_to(
                &self.consumer,
                &self.config.command_topic,
                partition,
                committed,
                self.config.kafka_operation_timeout,
            )
            .map_err(|source| RuntimeError::Seek { partition, source })?;
            return Ok(false);
        }

        Ok(true)
    }

    fn committed_boundary(&self, partition: i32) -> Result<i64, RuntimeError> {
        committed_next_offset(
            &self.consumer,
            &self.config.command_topic,
            partition,
            self.config.kafka_operation_timeout,
        )
        .map_err(|source| RuntimeError::CommittedOffset { partition, source })
        .map(|offset| offset.unwrap_or(0))
    }

    fn begin_transaction(&mut self, partition: i32) -> Result<(), RuntimeError> {
        loop {
            let producer = &self
                .partitions
                .get(&partition)
                .expect("assigned partition runtime")
                .producer;

            match producer.begin_transaction() {
                Ok(()) => return Ok(()),
                Err(error) => match classify_transaction_error(&error) {
                    TransactionErrorClass::Retriable => {
                        warn!(partition, %error, "begin_transaction retriable; retrying same operation");
                        thread::sleep(self.config.retry_initial_delay);
                    }
                    TransactionErrorClass::AbortRequired => {
                        self.abort_transaction(partition)?;
                    }
                    TransactionErrorClass::Fatal => {
                        self.partitions
                            .get_mut(&partition)
                            .expect("assigned partition runtime")
                            .state = None;
                        return Err(RuntimeError::FatalTransaction {
                            partition,
                            source: error,
                        });
                    }
                },
            }
        }
    }

    fn send_offsets_with_retry(
        &self,
        partition: i32,
        offsets: &TopicPartitionList,
    ) -> Result<(), TransactionFailure> {
        let producer = &self
            .partitions
            .get(&partition)
            .expect("assigned partition runtime")
            .producer;
        let group_metadata = self
            .consumer
            .group_metadata()
            .ok_or(TransactionFailure::MissingGroupMetadata)?;

        loop {
            match send_offsets(
                producer,
                offsets,
                &group_metadata,
                self.config.kafka_operation_timeout,
            ) {
                Ok(()) => return Ok(()),
                Err(error) => match classify_transaction_error(&error) {
                    TransactionErrorClass::Retriable => {
                        warn!(partition, %error, "send_offsets_to_transaction retriable; retrying same operation");
                        thread::sleep(self.config.retry_initial_delay);
                    }
                    TransactionErrorClass::AbortRequired => {
                        return Err(TransactionFailure::AbortRequired(error));
                    }
                    TransactionErrorClass::Fatal => return Err(TransactionFailure::Fatal(error)),
                },
            }
        }
    }

    fn commit_with_retry(&self, partition: i32) -> Result<(), TransactionFailure> {
        let producer = &self
            .partitions
            .get(&partition)
            .expect("assigned partition runtime")
            .producer;

        loop {
            match producer.commit_transaction(self.config.kafka_operation_timeout) {
                Ok(()) => return Ok(()),
                Err(error) => match classify_transaction_error(&error) {
                    TransactionErrorClass::Retriable => {
                        // CEX12-D09: retry COMMIT itself. Never call the engine again here.
                        warn!(partition, %error, "commit_transaction retriable; retrying commit without reprocessing");
                        thread::sleep(self.config.retry_initial_delay);
                    }
                    TransactionErrorClass::AbortRequired => {
                        return Err(TransactionFailure::AbortRequired(error));
                    }
                    TransactionErrorClass::Fatal => return Err(TransactionFailure::Fatal(error)),
                },
            }
        }
    }

    fn handle_post_mutation_transaction_failure(
        &mut self,
        partition: i32,
        input_offset: i64,
        engine_mutated: bool,
        failure: TransactionFailure,
    ) -> Result<(), RuntimeError> {
        match failure {
            TransactionFailure::AbortRequired(error) => {
                warn!(partition, input_offset, %error, "Kafka requires transaction abort");
                self.abort_transaction(partition)?;

                if engine_mutated {
                    self.recover_after_abort(partition)?;
                } else {
                    seek_to(
                        &self.consumer,
                        &self.config.command_topic,
                        partition,
                        input_offset,
                        self.config.kafka_operation_timeout,
                    )
                    .map_err(|source| RuntimeError::Seek { partition, source })?;
                }

                Ok(())
            }
            TransactionFailure::Fatal(error) => {
                self.partitions
                    .get_mut(&partition)
                    .expect("assigned partition runtime")
                    .state = None;
                Err(RuntimeError::FatalTransaction {
                    partition,
                    source: error,
                })
            }
            TransactionFailure::MissingGroupMetadata => {
                warn!(
                    partition,
                    input_offset, "consumer group metadata unavailable; aborting without progress"
                );
                self.abort_transaction(partition)?;
                if engine_mutated {
                    self.recover_after_abort(partition)?;
                } else {
                    seek_to(
                        &self.consumer,
                        &self.config.command_topic,
                        partition,
                        input_offset,
                        self.config.kafka_operation_timeout,
                    )
                    .map_err(|source| RuntimeError::Seek { partition, source })?;
                }
                Ok(())
            }
        }
    }

    fn abort_transaction(&self, partition: i32) -> Result<(), RuntimeError> {
        let producer = &self
            .partitions
            .get(&partition)
            .expect("assigned partition runtime")
            .producer;

        loop {
            match producer.abort_transaction(self.config.kafka_operation_timeout) {
                Ok(()) => return Ok(()),
                Err(error) => match classify_transaction_error(&error) {
                    TransactionErrorClass::Retriable | TransactionErrorClass::AbortRequired => {
                        warn!(partition, %error, "abort_transaction not yet complete; retrying abort");
                        thread::sleep(self.config.retry_initial_delay);
                    }
                    TransactionErrorClass::Fatal => {
                        return Err(RuntimeError::FatalTransaction {
                            partition,
                            source: error,
                        });
                    }
                },
            }
        }
    }

    fn recover_after_abort(&mut self, partition: i32) -> Result<(), RuntimeError> {
        // Discard provisional memory before consulting the durable Kafka boundary.
        {
            let runtime = self
                .partitions
                .get_mut(&partition)
                .expect("assigned partition runtime");
            runtime.state = None;
            runtime.expected_next_offset = None;
        }

        let committed = self.committed_boundary(partition)?;
        let state = match rebuild_partition(&self.config, partition, committed, &self.shutdown) {
            Ok(state) => state,
            Err(RecoveryError::Shutdown) if self.shutdown.load(Ordering::Relaxed) => return Ok(()),
            Err(source) => return Err(RuntimeError::Recovery { partition, source }),
        };

        {
            let runtime = self
                .partitions
                .get_mut(&partition)
                .expect("assigned partition runtime");
            runtime.state = Some(state);
            runtime.expected_next_offset = Some(committed);
        }

        seek_to(
            &self.consumer,
            &self.config.command_topic,
            partition,
            committed,
            self.config.kafka_operation_timeout,
        )
        .map_err(|source| RuntimeError::Seek { partition, source })?;

        info!(
            partition,
            committed, "partition state reconstructed from committed Kafka history after abort"
        );
        Ok(())
    }

    fn block_partition(&mut self, partition: i32, input_offset: i64) -> Result<(), RuntimeError> {
        // Keep the local consumer position parked on the same offending record.
        // Durable progress was not committed, and a restart/operator repair must
        // resume from this exact authoritative boundary.
        seek_to(
            &self.consumer,
            &self.config.command_topic,
            partition,
            input_offset,
            self.config.kafka_operation_timeout,
        )
        .map_err(|source| RuntimeError::Seek { partition, source })?;

        let runtime = self
            .partitions
            .get_mut(&partition)
            .expect("assigned partition runtime");
        runtime.blocked = true;

        pause_partition(&self.consumer, &self.config.command_topic, partition)
            .map_err(|source| RuntimeError::Pause { partition, source })
    }

    fn transaction_enqueue_failure(
        &mut self,
        partition: i32,
        error: KafkaError,
    ) -> Result<(), RuntimeError> {
        // QueueFull is retried inside the producer wrapper. Any other local
        // enqueue failure is treated as an operational/configuration failure;
        // the transaction has already been aborted and the input is uncommitted.
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        Err(RuntimeError::ResultEnqueue {
            partition,
            source: error,
        })
    }
}

#[derive(Debug)]
enum TransactionFailure {
    AbortRequired(KafkaError),
    Fatal(KafkaError),
    MissingGroupMetadata,
}
