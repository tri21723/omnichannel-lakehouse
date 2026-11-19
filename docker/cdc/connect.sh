#!/bin/bash
# Kafka Connect REST without a host port (ADR-0017 §2). JSON events only.
set -euo pipefail
C=${CDC_COMPOSE:?set CDC_COMPOSE}
NAME=walmart-orders
rest() { $C exec -T kafka-connect curl -fsS -H 'Content-Type: application/json' "$@"; }
case "${1:-status}" in
register)
    # PUT is create-or-update, so re-registering the same file is idempotent.
    $C exec -T kafka-connect curl -fsS -X PUT -H 'Content-Type: application/json' \
        --data-binary @- "http://localhost:8083/connectors/$NAME/config" \
        < docker/cdc/connectors/walmart-orders.json >/dev/null
    printf '{"event": "cdc_connector_registered", "connector": "%s"}\n' "$NAME"
    ;;
status)
    rest "http://localhost:8083/connectors/$NAME/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
tasks = [t["state"] for t in s.get("tasks", [])]
print(json.dumps({"event": "cdc_connector_status", "connector": s["name"], "state": s["connector"]["state"], "tasks": tasks}))
sys.exit(0 if s["connector"]["state"] == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks) else 1)'
    ;;
wait)
    # A freshly registered connector is UNASSIGNED/without tasks for a few seconds.
    deadline=$((SECONDS + ${CDC_WAIT_SECONDS:-120}))
    until bash "$0" status >/dev/null 2>&1; do
        [ $SECONDS -lt $deadline ] || { bash "$0" status; exit 1; }
        sleep 3
    done
    bash "$0" status
    ;;
pause | resume)
    $C exec -T kafka-connect curl -fsS -X PUT "http://localhost:8083/connectors/$NAME/$1" >/dev/null
    printf '{"event": "cdc_connector_%sd", "connector": "%s"}\n' "$1" "$NAME"
    ;;
resnapshot)
    # Blocking snapshot through the Kafka signal channel: no slot drop, no signal table in the DB.
    printf '%s|{"type":"execute-snapshot","data":{"data-collections":["walmart.orders"],"type":"BLOCKING"}}\n' platform_cdc \
      | $C exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 \
          --topic platform_cdc.signals --property parse.key=true --property key.separator='|' >/dev/null
    printf '{"event": "cdc_resnapshot_requested", "connector": "%s"}\n' "$NAME"
    ;;
delete)
    rest -X DELETE "http://localhost:8083/connectors/$NAME" >/dev/null
    printf '{"event": "cdc_connector_deleted", "connector": "%s"}\n' "$NAME"
    ;;
*)
    echo "usage: connect.sh register | status | wait | pause | resume | resnapshot | delete" >&2
    exit 2
    ;;
esac
