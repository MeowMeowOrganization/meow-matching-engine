# meow-matching-kafka-adapter

Kafka/Protobuf runtime around the pure `meow-matching-engine` crate.

## Run locally

From the repository root:

```bash
docker compose -f docker-compose.kafka.yml up -d
./scripts/create-local-kafka-topics.sh

export MEOW_KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:39092,localhost:49092
cargo run -p meow-matching-kafka-adapter
```

Optional environment variables:

- `MEOW_KAFKA_COMMANDS_TOPIC` (default `meow.matching.commands.v1`)
- `MEOW_KAFKA_RESULTS_TOPIC` (default `meow.matching.results.v1`)
- `MEOW_KAFKA_GROUP_ID` (default `meow-matching-engine-v1`)
- `MEOW_KAFKA_TRANSACTIONAL_ID_PREFIX` (default `meow-matching-engine-v1`)
- `RUST_LOG`

See `docs/CEX12-kafka-adapter.md` for failure and recovery semantics.

## Verification

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings

MEOW_KAFKA_TEST_BOOTSTRAP_SERVERS=localhost:29092,localhost:39092,localhost:49092 \
  cargo test -p meow-matching-kafka-adapter --test kafka_transactions -- --ignored
```

The Protobuf directory is a local contract snapshot. Read `proto/README.md` before replacing it with the canonical shared contract package.
