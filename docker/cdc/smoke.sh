#!/bin/bash
# CDC profile smoke (ADR-0017). Prints one JSON line per check; exits 1 on any failure.
# The DML probe uses its own `cdcsmoke-<epoch>` ids (never a batch dataset prefix) and the
# `generator` role; secrets are only read inside containers and never printed.
set -uo pipefail
C=${CDC_COMPOSE:?set CDC_COMPOSE}
failures=0
total=0
report() { # name ok [detail]
    total=$((total + 1))
    [ "$2" = true ] || failures=$((failures + 1))
    printf '{"event": "cdc_smoke_check", "check": "%s", "ok": %s%s}\n' "$1" "$2" "${3:+, $3}"
}
b() { if [ "$1" = 1 ] || [ "$1" = true ]; then echo true; else echo false; fi; }
psql_admin() { $C exec -T postgres sh -c 'psql -X -U "$POSTGRES_USER" -d sources -At -F " " -c "$0"' "$1"; }
topic_total() { # sum of end offsets
    $C exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic "$1" 2>/dev/null \
        | awk -F: '{s += $3} END {print s + 0}'
}

# Connector, slot, publication scope.
bash docker/cdc/connect.sh wait >/dev/null 2>&1
report "connector_running" "$([ $? -eq 0 ] && echo true || echo false)"
slot=$(psql_admin "SELECT plugin || ',' || active FROM pg_replication_slots WHERE slot_name = 'dbz_walmart'")
report "slot_active_pgoutput" "$([ "$slot" = "pgoutput,true" ] && echo true || echo false)"
pub=$(psql_admin "SELECT string_agg(schemaname || '.' || tablename, ',') FROM pg_publication_tables WHERE pubname = 'dbz_pub'")
report "publication_orders_only" "$([ "$pub" = "walmart.orders" ] && echo true || echo false)"
topics=$($C exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list 2>/dev/null)
report "no_customers_topic" "$(grep -q customers <<<"$topics" && echo false || echo true)"

# Snapshot: every existing order was read at least once.
orders=$(psql_admin "SELECT count(*) FROM walmart.orders")
messages=$(topic_total platform_cdc.walmart.orders)
report "snapshot_complete" "$([ "${messages:-0}" -ge "${orders:-1}" ] && echo true || echo false)" "\"orders\": ${orders:-0}, \"messages\": ${messages:-0}"

# DML probe: insert, two updates, delete on a smoke id → c, u, u, d (+ tombstone), in order.
id="cdcsmoke-$(date +%s)"
$C exec -T postgres sh -c 'PGPASSWORD="$GENERATOR_DB_PASSWORD" psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -U generator -d sources' >/dev/null 2>&1 <<SQL
INSERT INTO walmart.customers VALUES ('$id-c', 'Smoke Probe', 'smoke@example.invalid', 'VN', now());
INSERT INTO walmart.orders VALUES ('$id', '$id-c', now(), now(), 'paid', 'USD', 1.00, 'VN');
UPDATE walmart.orders SET status = 'shipped', updated_ts = now() WHERE order_id = '$id';
UPDATE walmart.orders SET total_amount = 2.50, updated_ts = now() WHERE order_id = '$id';
DELETE FROM walmart.orders WHERE order_id = '$id';
DELETE FROM walmart.customers WHERE customer_id = '$id-c';
SQL
report "probe_dml_applied" "$([ $? -eq 0 ] && echo true || echo false)"
ops=""
for _ in $(seq 1 30); do
    ops=$($C exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
            --topic platform_cdc.walmart.orders --from-beginning --timeout-ms 8000 \
            --property print.key=true --property key.separator='|' 2>/dev/null \
        | python3 -c '
import json, sys
probe = sys.argv[1]
ops = []
for line in sys.stdin:
    key, _, value = line.rstrip("\n").partition("|")
    if f"\"{probe}\"" not in key:
        continue
    ops.append("tombstone" if value in ("", "null") else json.loads(value)["op"])
print(",".join(ops))' "$id")
    [ "$ops" = "c,u,u,d,tombstone" ] && break
    sleep 2
done
report "probe_events_in_order" "$([ "$ops" = "c,u,u,d,tombstone" ] && echo true || echo false)" "\"ops\": \"$ops\""
amount=$($C exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
        --topic platform_cdc.walmart.orders --from-beginning --timeout-ms 8000 2>/dev/null \
    | python3 -c '
import json, sys
probe = sys.argv[1]
for line in sys.stdin:
    if line.strip() in ("", "null"):
        continue
    event = json.loads(line)
    after = event.get("after") or {}
    if after.get("order_id") == probe and after.get("total_amount") == "2.50":
        print("decimal-string"); break' "$id")
report "decimal_as_string" "$([ "$amount" = "decimal-string" ] && echo true || echo false)"

# Heartbeat and slot lag.
report "heartbeat_flowing" "$([ "$(topic_total __debezium-heartbeat.platform_cdc)" -gt 0 ] && echo true || echo false)"
lag=$(psql_admin "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name = 'dbz_walmart'")
report "slot_lag_bytes" "$([ -n "$lag" ] && [ "$lag" -lt 104857600 ] && echo true || echo false)" "\"lag_bytes\": ${lag:-0}"

# The replication password must not appear in Connect logs or in its stored configs.
leaks=$($C logs kafka-connect 2>/dev/null \
    | grep -c -F -f <($C exec -T kafka-connect printenv DEBEZIUM_REPL_DB_PASSWORD 2>/dev/null))
report "no_secret_in_connect_logs" "$([ "${leaks:-0}" -eq 0 ] && echo true || echo false)"
stored=$($C exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
        --topic _connect.configs --from-beginning --timeout-ms 5000 2>/dev/null | grep -c '\${env:DEBEZIUM_REPL_DB_PASSWORD}')
report "config_stores_placeholder" "$([ "${stored:-0}" -ge 1 ] && echo true || echo false)"

ok=$([ $failures -eq 0 ] && echo true || echo false)
printf '{"event": "cdc_smoke_finished", "checks": %s, "ok": %s}\n' "$total" "$ok"
[ $failures -eq 0 ]
