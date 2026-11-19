#!/bin/bash
# Idempotent CDC topics (ADR-0017 §3): create if missing, then (re)apply configs.
set -euo pipefail
BS="${KAFKA_BOOTSTRAP:-kafka:29092}"
T=/opt/kafka/bin/kafka-topics.sh
C=/opt/kafka/bin/kafka-configs.sh
WEEK=604800000
ensure() {
    local topic=$1 partitions=$2
    "$T" --bootstrap-server "$BS" --create --if-not-exists --topic "$topic" \
        --partitions "$partitions" --replication-factor 1 >/dev/null
    "$C" --bootstrap-server "$BS" --alter --entity-type topics --entity-name "$topic" \
        --add-config "retention.ms=$WEEK,cleanup.policy=delete" >/dev/null
    printf '{"event": "kafka_topic_ready", "topic": "%s", "partitions": %s}\n' "$topic" "$partitions"
}
ensure platform_cdc.walmart.orders 3
ensure __debezium-heartbeat.platform_cdc 1
