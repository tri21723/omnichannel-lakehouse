#!/bin/bash
# Flink job lifecycle for the session cluster (ADR-0016 §7-8). Prints JSON events.
# Job ids are kept in .local-data/stream/jobs.env so `resume` can find their retained checkpoints
# after the JobManager lost them (session cluster without HA).
set -euo pipefail
C=${STREAM_COMPOSE:?set STREAM_COMPOSE}
JAR=/opt/flink/usrlib/stream-jobs.jar
STATE=.local-data/stream/jobs.env
declare -A CLASSES=(
    [amazon_late]=omnichannel.stream.amazonlate.AmazonLateJob
    [tiktok_dedup]=omnichannel.stream.tiktokdedup.TiktokDedupJob
)
flink() { $C exec -T flink-jobmanager flink "$@"; }
running() { # name -> job id of the RUNNING job, if any
    flink list -r 2>/dev/null | sed -n "s/.* : \([0-9a-f]\{32\}\) : $1 (RUNNING).*/\1/p"
}
remember() { # name id
    mkdir -p "$(dirname "$STATE")"
    touch "$STATE"
    grep -v "^$1=" "$STATE" > "$STATE.tmp" || true
    echo "$1=$2" >> "$STATE.tmp"
    mv "$STATE.tmp" "$STATE"
}
submit() { # name start [savepoint]
    local args=(run -d)
    [ -n "${3:-}" ] && args+=(-s "$3")
    local out
    out=$(flink "${args[@]}" -c "${CLASSES[$1]}" "$JAR" --start "$2" 2>&1)
    local id
    id=$(sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' <<<"$out" | tail -1)
    [ -n "$id" ] || { echo "$out" | tail -5 >&2; exit 1; }
    remember "$1" "$id"
    echo "$id"
}
latest_checkpoint() { # job id -> s3 path of the newest completed checkpoint
    $C run --rm --no-deps --entrypoint sh stream-minio-init -c '
        mc alias set f "$S3_ENDPOINT" "$FLINK_S3_ACCESS_KEY" "$FLINK_S3_SECRET_KEY" >/dev/null
        mc ls --recursive f/checkpoints/flink/checkpoints/'"$1"'/ 2>/dev/null \
          | awk "{print \$NF}" | grep "/_metadata$" | sort -t- -k2 -n | tail -1' 2>/dev/null \
      | sed -n 's#^\(chk-[0-9]*\)/_metadata$#s3://checkpoints/flink/checkpoints/'"$1"'/\1#p'
}
case "${1:-list}" in
start)
    start=${2:-latest}
    for name in amazon_late tiktok_dedup; do
        if [ -n "$(running "$name")" ]; then
            printf '{"event": "stream_job_running", "job": "%s"}\n' "$name"
            continue
        fi
        id=$(submit "$name" "$start")
        printf '{"event": "stream_job_started", "job": "%s", "start": "%s", "job_id": "%s"}\n' "$name" "$start" "$id"
    done
    ;;
resume)
    for name in amazon_late tiktok_dedup; do
        if [ -n "$(running "$name")" ]; then
            printf '{"event": "stream_job_running", "job": "%s"}\n' "$name"
            continue
        fi
        old=$(sed -n "s/^$name=//p" "$STATE" 2>/dev/null)
        [ -n "$old" ] || { echo "no recorded job id for $name" >&2; exit 1; }
        path=$(latest_checkpoint "$old")
        [ -n "$path" ] || { echo "no retained checkpoint for $name ($old)" >&2; exit 1; }
        id=$(submit "$name" latest "$path")
        printf '{"event": "stream_job_resumed", "job": "%s", "from": "%s", "job_id": "%s"}\n' "$name" "${path##*/}" "$id"
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
    echo "usage: jobs.sh start [latest|earliest|<ms>] | resume | stop | list" >&2
    exit 2
    ;;
esac
