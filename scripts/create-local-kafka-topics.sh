#!/usr/bin/env bash
set -euo pipefail

PARTITIONS="${MATCHING_PARTITIONS:-12}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.kafka.yml}"
BOOTSTRAP="kafka-1:19092"

wait_for_kafka() {
  local attempts=60

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if docker compose -f "$COMPOSE_FILE" exec -T kafka-1 \
      /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done

  echo "Kafka did not become ready after ${attempts} attempts" >&2
  return 1
}

create_topic() {
  local topic="$1"

  docker compose -f "$COMPOSE_FILE" exec -T kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=-1 \
    --config min.insync.replicas=2 \
    --config unclean.leader.election.enable=false
}

wait_for_kafka

create_topic "meow.matching.commands.v1"
create_topic "meow.matching.results.v1"

echo
echo "Authoritative matching topics:"
docker compose -f "$COMPOSE_FILE" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --describe \
  --topic "meow.matching.commands.v1"
docker compose -f "$COMPOSE_FILE" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --describe \
  --topic "meow.matching.results.v1"

echo
echo "Do not increase partition counts in place. Create a versioned topic for topology changes."
