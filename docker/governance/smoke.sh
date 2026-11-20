#!/bin/bash
# Governance profile smoke (ADR-0018). Prints one JSON line per check; exits 1 on any failure.
# The write probe uses its own dataset URN (platform `omnichannel_smoke`) and deletes it again.
# Passwords are read from git-ignored local files/containers and never printed.
set -uo pipefail
C=${GOV_COMPOSE:?set GOV_COMPOSE}
UI=${DATAHUB_UI:-http://127.0.0.1:9002}
failures=0
total=0
report() { # name ok [detail]
    total=$((total + 1))
    [ "$2" = true ] || failures=$((failures + 1))
    printf '{"event": "gov_smoke_check", "check": "%s", "ok": %s%s}\n' "$1" "$2" "${3:+, $3}"
}
tf() { if "$@"; then echo true; else echo false; fi; }
gms() { $C exec -T datahub-gms curl -fsS "$@"; }
jar=$(mktemp)
trap 'rm -f "$jar"' EXIT

# Health and exposure.
report "gms_health" "$(tf gms -o /dev/null http://localhost:8080/health)"
report "frontend_health" "$(tf curl -fsS -o /dev/null "$UI/admin")"
bindings=$(docker inspect --format '{{json .HostConfig.PortBindings}}' "$($C ps -q datahub-gms)")
report "gms_no_host_port" "$(tf test "$bindings" = "{}")" "\"bindings\": $bindings"
binding=$($C port datahub-frontend 9002 2>/dev/null)
report "frontend_loopback_only" "$(tf test "$binding" = "127.0.0.1:9002")" "\"binding\": \"$binding\""

# Login: generated password works, the image default does not.
password=$(cut -d: -f2- .local-data/governance/user.props)
status=$(curl -s -o /dev/null -w '%{http_code}' -c "$jar" -H 'Content-Type: application/json' \
    --data-binary @- "$UI/logIn" <<<"{\"username\": \"datahub\", \"password\": \"$password\"}")
report "login_generated_password" "$(tf test "$status" = 200)" "\"status\": $status"
rejected=$(curl -s -w ' %{http_code}' -H 'Content-Type: application/json' \
    -d '{"username": "datahub", "password": "datahub"}' "$UI/logIn")
report "default_login_rejected" "$(tf grep -q 'Invalid Credentials.* 400$' <<<"$rejected")" "\"status\": ${rejected##* }"
me=$(curl -s -b "$jar" -H 'Content-Type: application/json' \
    -d '{"query": "{ appConfig { appVersion } me { corpUser { username } } }"}' "$UI/api/v2/graphql")
version=$(python3 -c 'import json, sys; print(json.load(sys.stdin)["data"]["appConfig"]["appVersion"])' <<<"$me" 2>/dev/null)
report "graphql_as_admin" "$(tf grep -q '"username":"datahub"' <<<"$me")"
report "graphql_version" "$(tf test -n "$version")" "\"version\": \"$version\""

# Write, read, delete a probe entity through GMS (network-internal only).
urn="urn:li:dataset:(urn:li:dataPlatform:omnichannel_smoke,probe_$(date +%s),PROD)"
enc=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$urn")
proposal=$(python3 -c '
import json, sys
aspect = json.dumps({"description": "governance smoke probe", "customProperties": {}})
print(json.dumps({"proposal": {"entityType": "dataset", "entityUrn": sys.argv[1], "changeType": "UPSERT",
    "aspectName": "datasetProperties", "aspect": {"value": aspect, "contentType": "application/json"}}}))' "$urn")
gms -o /dev/null -X POST -H 'Content-Type: application/json' -H 'X-RestLi-Protocol-Version: 2.0.0' \
    -d "$proposal" 'http://localhost:8080/aspects?action=ingestProposal'
report "probe_write" "$([ $? -eq 0 ] && echo true || echo false)"
read_back=$(gms -H 'X-RestLi-Protocol-Version: 2.0.0' "http://localhost:8080/entitiesV2/$enc?aspects=List(datasetProperties)")
report "probe_read" "$(tf grep -q 'governance smoke probe' <<<"$read_back")"
gms -o /dev/null -X POST -H 'Content-Type: application/json' -H 'X-RestLi-Protocol-Version: 2.0.0' \
    -d "{\"urn\": \"$urn\"}" 'http://localhost:8080/entities?action=delete'
gone=$(gms -H 'X-RestLi-Protocol-Version: 2.0.0' "http://localhost:8080/entitiesV2/$enc?aspects=List(datasetProperties)" 2>/dev/null)
report "probe_deleted" "$(tf bash -c '! grep -q "governance smoke probe" <<<"$1"' _ "$gone")"

# Kafka: DataHub topics pre-created, broker auto-creation still off.
topics=$($C exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list 2>/dev/null)
missing=0
for topic in MetadataChangeProposal_v1 MetadataChangeLog_Versioned_v1 MetadataChangeLog_Timeseries_v1 \
    PlatformEvent_v1 DataHubUpgradeHistory_v1; do
    grep -qx "$topic" <<<"$topics" || missing=$((missing + 1))
done
report "datahub_topics_present" "$(tf test $missing -eq 0)" "\"missing\": $missing"
auto=$($C exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:29092 --entity-type brokers \
    --entity-name 1 --describe --all 2>/dev/null | grep -o 'auto.create.topics.enable=[a-z]*' | head -1)
report "broker_auto_create_off" "$(tf test "$auto" = auto.create.topics.enable=false)"

# Ingestion role cannot read source rows.
ro=$($C exec -T postgres sh -c 'psql -X -U "$POSTGRES_USER" -d sources -At -c "$0"' \
    "SELECT has_table_privilege('datahub_ro', 'walmart.customers', 'SELECT')::text || ',' || has_schema_privilege('datahub_ro', 'walmart', 'USAGE')::text")
report "datahub_ro_no_row_access" "$(tf test "$ro" = false,true)"

# No generated secret appears in DataHub logs.
leaks=0
for name in DATAHUB_DB_PASSWORD DATAHUB_SYSTEM_CLIENT_SECRET DATAHUB_TOKEN_SERVICE_SIGNING_KEY DATAHUB_SECRET; do
    value=$(grep -m1 "^$name=" .env | cut -d= -f2-)
    [ -n "$value" ] || continue
    count=$($C logs datahub-gms datahub-frontend datahub-system-update 2>/dev/null | grep -c -F -- "$value")
    leaks=$((leaks + count))
done
report "no_secret_in_logs" "$(tf test $leaks -eq 0)"

ok=$([ $failures -eq 0 ] && echo true || echo false)
printf '{"event": "gov_smoke_finished", "checks": %s, "ok": %s}\n' "$total" "$ok"
[ $failures -eq 0 ]
