#!/bin/bash
# Kafka Connect REST without a host port (ADR-0017 §2): register | status | delete. JSON events only.
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
delete)
    rest -X DELETE "http://localhost:8083/connectors/$NAME" >/dev/null
    printf '{"event": "cdc_connector_deleted", "connector": "%s"}\n' "$NAME"
    ;;
*)
    echo "usage: connect.sh register | status | delete" >&2
    exit 2
    ;;
esac
