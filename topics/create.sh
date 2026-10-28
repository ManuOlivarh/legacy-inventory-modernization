#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-kafka-1:29092}
TOPICS=/opt/kafka/bin/kafka-topics.sh

create() {
  local name=$1 partitions=$2
  shift 2
  "$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 3 \
    --config min.insync.replicas=2 "$@"
  echo "ready: $name"
}

create inventory.stock-movements.v1 3 --config cleanup.policy=delete --config retention.ms=2592000000

create inventory.stock-levels.v1 3 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.1

create inventory.translator.dlq 3 --config cleanup.policy=delete --config retention.ms=1209600000
create inventory.stock-levels.v1.availability-api.dlq 3 --config cleanup.policy=delete --config retention.ms=1209600000
