#!/bin/bash
# Live acceptance (ADR-0016 §6, §8): jobs first, paced publish, drain, strict verify, close.
# Used locally and by .github/workflows/stream.yml. Prints JSON events only.
set -euo pipefail
: "${STREAM_COMPOSE:?set STREAM_COMPOSE}"
CFG=${GEN_STREAM_CONFIG:-generators/fixtures/stream-small.yaml}
ARGS=${GEN_STREAM_ARGS:-}
M=(make --no-print-directory GEN_STREAM_CONFIG="$CFG" GEN_STREAM_ARGS="$ARGS")

wait_checkpoints() {
    python3 - <<'PY'
import json, time, urllib.request
deadline = time.time() + 180
while time.time() < deadline:
    try:
        get = lambda p: json.load(urllib.request.urlopen("http://127.0.0.1:8082" + p, timeout=10))
        jobs = [j for j in get("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        done = [get(f"/jobs/{j['jid']}/checkpoints")["counts"]["completed"] for j in jobs]
        if len(done) == 2 and min(done) >= 1:
            raise SystemExit(0)
    except (OSError, KeyError, ValueError):
        pass
    time.sleep(3)
raise SystemExit("jobs did not complete a first checkpoint")
PY
}
lag() {
    $STREAM_COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:29092 --describe --all-groups 2>/dev/null \
      | awk '/^flink-/ && $6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}'
}

"${M[@]}" stream-start
wait_checkpoints
"${M[@]}" gen-stream
deadline=$((SECONDS + 600))
until [ "$(lag)" = 0 ]; do
    [ $SECONDS -lt $deadline ] || { echo '{"event": "stream_acceptance_finished", "ok": false}'; exit 1; }
    sleep 5
done
"${M[@]}" stream-stop
"${M[@]}" verify-stream
"${M[@]}" stream-close
echo '{"event": "stream_acceptance_finished", "ok": true}'
