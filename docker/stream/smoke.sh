#!/bin/bash
# Stream profile smoke (ADR-0016 §9). Prints one JSON line per check; exits 1 on any failure.
# Never prints credentials: the Flink secret is only piped into grep from the container env.
set -uo pipefail
C=${STREAM_COMPOSE:?set STREAM_COMPOSE to the compose command}
failures=0
total=0
report() { # name ok [detail]
    total=$((total + 1))
    [ "$2" = true ] || failures=$((failures + 1))
    printf '{"event": "stream_smoke_check", "check": "%s", "ok": %s%s}\n' "$1" "$2" "${3:+, $3}"
}
ok_if() { if "$@" >/dev/null 2>&1; then echo true; else echo false; fi; }
fail_if() { if "$@" >/dev/null 2>&1; then echo false; else echo true; fi; }

# Kafka: topics, partitions, produce/consume on the internal listener.
topics=$($C exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list 2>/dev/null)
for t in amazon.order-notifications tiktok_shop.order-webhooks amazon.order-notifications.dlq tiktok_shop.order-webhooks.dlq; do
    report "kafka_topic" "$(grep -qx "$t" <<<"$topics" && echo true || echo false)" "\"topic\": \"$t\""
done
parts=$($C exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe \
    --topic amazon.order-notifications 2>/dev/null | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
report "kafka_partitions" "$([ "$parts" = 3 ] && echo true || echo false)" "\"partitions\": ${parts:-0}"
token="smoke-$(date +%s%N)"
$C exec -T kafka bash -c "echo '$token' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic _smoke.stream" >/dev/null 2>&1
got=$($C exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
    --topic _smoke.stream --from-beginning --timeout-ms 15000 2>/dev/null | grep -c "$token")
report "kafka_produce_consume" "$([ "${got:-0}" -ge 1 ] && echo true || echo false)"
report "kafka_host_port" "$(ok_if timeout 5 bash -c 'exec 3<>/dev/tcp/127.0.0.1/9092')"
report "kafka_ui" "$(ok_if curl -fsS http://127.0.0.1:8085/actuator/health)"

# Flink: cluster has the TaskManager slots, and a real job writes to S3 through the plugin.
overview=$(curl -fsS http://127.0.0.1:8082/overview 2>/dev/null || echo '{}')
slots=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("slots-total", 0))' <<<"$overview")
report "flink_slots" "$([ "$slots" -ge 2 ] && echo true || echo false)" "\"slots\": $slots"
out="s3://raw/stream/_smoke/wordcount-$(date +%s)"
$C exec -T flink-jobmanager flink run /opt/flink/examples/streaming/WordCount.jar \
    --output "$out" --execution-mode BATCH >/dev/null 2>&1
wc_rc=$?
report "flink_job_s3_write" "$([ $wc_rc -eq 0 ] && echo true || echo false)" "\"exit_code\": $wc_rc"

# MinIO user flink: allowed only under raw/stream/* and checkpoints/flink/*.
probe=$($C run --rm --no-deps --entrypoint sh stream-minio-init -c '
    mc alias set f "$S3_ENDPOINT" "$FLINK_S3_ACCESS_KEY" "$FLINK_S3_SECRET_KEY" >/dev/null || exit 9
    r() { if "$@" >/dev/null 2>&1; then printf "1"; else printf "0"; fi; }
    printf "stream_put=%s " "$(echo x | r mc pipe f/raw/stream/_smoke/probe)"
    printf "ckpt_put=%s " "$(echo x | r mc pipe f/checkpoints/flink/_smoke/probe)"
    printf "raw_other_put=%s " "$(echo x | r mc pipe f/raw/_smoke/flink_probe)"
    printf "lakehouse_put=%s " "$(echo x | r mc pipe f/lakehouse/_smoke/flink_probe)"
    # Reading outside raw/stream/* must be denied. Root writes its own probe object (a fresh
    # environment has no manifests yet), user flink tries to read it, root removes it.
    mc alias set root "$S3_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null || exit 9
    echo probe | mc pipe root/raw/_smoke/flink_read_probe >/dev/null 2>&1
    printf "manifest_found=%s " "$(r mc stat root/raw/_smoke/flink_read_probe)"
    printf "manifest_get=%s " "$(r mc cat f/raw/_smoke/flink_read_probe)"
    mc rm --force root/raw/_smoke/flink_read_probe >/dev/null 2>&1
    printf "wordcount_files=%s " "$(mc ls --recursive f/'"${out#s3://}"'/ 2>/dev/null | grep -c .)"
    mc rm --recursive --force f/raw/stream/_smoke/ >/dev/null 2>&1
    mc rm --recursive --force f/checkpoints/flink/_smoke/ >/dev/null 2>&1
    printf "cleanup_left=%s" "$(mc ls --recursive f/raw/stream/_smoke/ 2>/dev/null | grep -c .)"
' 2>/dev/null)
val() { sed -n "s/.*$1=\([0-9]*\).*/\1/p" <<<"$probe"; }
report "flink_user_stream_write" "$([ "$(val stream_put)" = 1 ] && echo true || echo false)"
report "flink_user_checkpoint_write" "$([ "$(val ckpt_put)" = 1 ] && echo true || echo false)"
report "flink_user_denied_raw_outside_stream" "$([ "$(val raw_other_put)" = 0 ] && echo true || echo false)"
report "flink_user_denied_lakehouse" "$([ "$(val lakehouse_put)" = 0 ] && echo true || echo false)"
report "flink_user_denied_manifest_read" "$([ "$(val manifest_found)" = 1 ] && [ "$(val manifest_get)" = 0 ] && echo true || echo false)"
files=$(val wordcount_files)
files=${files:-0}
report "flink_output_files" "$([ "$files" -ge 1 ] && echo true || echo false)" "\"files\": $files"
report "smoke_cleanup" "$([ "$(val cleanup_left)" = 0 ] && echo true || echo false)"

# The S3 secret must not appear in Flink logs.
leaks=$($C logs flink-jobmanager flink-taskmanager 2>/dev/null \
    | grep -c -F -f <($C exec -T flink-jobmanager printenv AWS_SECRET_ACCESS_KEY 2>/dev/null))
report "no_secret_in_flink_logs" "$([ "${leaks:-0}" -eq 0 ] && echo true || echo false)"

ok=$([ $failures -eq 0 ] && echo true || echo false)
printf '{"event": "stream_smoke_finished", "checks": %s, "ok": %s}\n' "$total" "$ok"
[ $failures -eq 0 ]
