use std::{env, time::Duration};

use thiserror::Error;

pub const DEFAULT_COMMAND_TOPIC: &str = "meow.matching.commands.v1";
pub const DEFAULT_RESULT_TOPIC: &str = "meow.matching.results.v1";
pub const DEFAULT_GROUP_ID: &str = "meow-matching-engine-v1";
pub const DEFAULT_TRANSACTIONAL_ID_PREFIX: &str = "meow-matching-engine-v1";

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub bootstrap_servers: String,
    pub command_topic: String,
    pub result_topic: String,
    pub group_id: String,
    pub transactional_id_prefix: String,
    pub poll_timeout: Duration,
    pub kafka_operation_timeout: Duration,
    pub retry_initial_delay: Duration,
    pub retry_max_delay: Duration,
}

impl RuntimeConfig {
    /// Loads only adapter/runtime configuration. No deterministic engine
    /// configuration is read from the environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Missing`] when the Kafka bootstrap-server setting
    /// is absent or empty.
    pub fn from_env() -> Result<Self, ConfigError> {
        let bootstrap_servers = required("MEOW_KAFKA_BOOTSTRAP_SERVERS")?;

        Ok(Self {
            bootstrap_servers,
            command_topic: optional("MEOW_KAFKA_COMMANDS_TOPIC", DEFAULT_COMMAND_TOPIC),
            result_topic: optional("MEOW_KAFKA_RESULTS_TOPIC", DEFAULT_RESULT_TOPIC),
            group_id: optional("MEOW_KAFKA_GROUP_ID", DEFAULT_GROUP_ID),
            transactional_id_prefix: optional(
                "MEOW_KAFKA_TRANSACTIONAL_ID_PREFIX",
                DEFAULT_TRANSACTIONAL_ID_PREFIX,
            ),
            poll_timeout: Duration::from_millis(100),
            kafka_operation_timeout: Duration::from_secs(10),
            retry_initial_delay: Duration::from_millis(50),
            retry_max_delay: Duration::from_secs(2),
        })
    }

    #[must_use]
    pub fn transactional_id(&self, partition: i32) -> String {
        format!("{}.p{partition}", self.transactional_id_prefix)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("required environment variable {0} is missing or empty")]
    Missing(&'static str),
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) | Err(_) => Err(ConfigError::Missing(name)),
    }
}

fn optional(name: &'static str, default: &'static str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}
