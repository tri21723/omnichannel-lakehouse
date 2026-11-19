#!/bin/bash
# Idempotent topic setup (ADR-0016 §3): create if missing, then (re)apply the desired configs.
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
    local actual
    actual=$("$T" --bootstrap-server "$BS" --describe --topic "$topic" | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
    [ "$actual" = "$partitions" ] || { echo "topic $topic has $actual partitions, expected $partitions" >&2; exit 1; }
    printf '{"event": "kafka_topic_ready", "topic": "%s", "partitions": %s}\n' "$topic" "$partitions"
}
for topic in amazon.order-notifications tiktok_shop.order-webhooks; do
    ensure "$topic" 3
    ensure "$topic.dlq" 1
done
ensure _smoke.stream 1
