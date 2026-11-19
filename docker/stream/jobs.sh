#!/bin/bash
# Flink job lifecycle for the session cluster (ADR-0016 §8). Prints JSON events.
set -euo pipefail
C=${STREAM_COMPOSE:?set STREAM_COMPOSE}
JAR=/opt/flink/usrlib/stream-jobs.jar
declare -A CLASSES=(
    [amazon_late]=omnichannel.stream.amazonlate.AmazonLateJob
    [tiktok_dedup]=omnichannel.stream.tiktokdedup.TiktokDedupJob
)
flink() { $C exec -T flink-jobmanager flink "$@"; }
running() { # name -> job id of the RUNNING job, if any
    flink list -r 2>/dev/null | sed -n "s/.* : \([0-9a-f]\{32\}\) : $1 (RUNNING).*/\1/p"
}
case "${1:-list}" in
start)
    start=${2:-latest}
    for name in amazon_late tiktok_dedup; do
        if [ -n "$(running "$name")" ]; then
            printf '{"event": "stream_job_running", "job": "%s"}\n' "$name"
            continue
        fi
        flink run -d -c "${CLASSES[$name]}" "$JAR" --start "$start" >/dev/null
        printf '{"event": "stream_job_started", "job": "%s", "start": "%s"}\n' "$name" "$start"
    done
    ;;
stop)
    for name in amazon_late tiktok_dedup; do
        id=$(running "$name")
        if [ -z "$id" ]; then
            printf '{"event": "stream_job_not_running", "job": "%s"}\n' "$name"
            continue
        fi
        flink stop --drain -p s3://checkpoints/flink/savepoints "$id" >/dev/null
        printf '{"event": "stream_job_stopped", "job": "%s", "drain": true}\n' "$name"
    done
    ;;
list)
    flink list -a 2>/dev/null | grep -E ' : [0-9a-f]{32} : ' || true
    ;;
*)
    echo "usage: jobs.sh start [latest|earliest|<ms>] | stop | list" >&2
    exit 2
    ;;
esac
