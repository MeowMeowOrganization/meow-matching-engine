use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use meow_matching_kafka_adapter::{KafkaRuntime, config::RuntimeConfig};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_flag = Arc::clone(&shutdown);

    ctrlc::set_handler(move || {
        signal_flag.store(true, Ordering::Relaxed);
    })?;

    let config = RuntimeConfig::from_env()?;
    let mut runtime = KafkaRuntime::new(config, shutdown)?;
    runtime.run()?;

    Ok(())
}
