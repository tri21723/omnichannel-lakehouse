#!/bin/sh
# The raw log stays on the disposable runner; only allowlisted events reach console.
set -u
mkdir -p .local-data/ci
log_path=$(mktemp .local-data/ci/bronze.XXXXXX)
command_status=0
"$@" > "$log_path" 2>&1 || command_status=$?
python3 docker/batch/ci_events.py "$log_path"
if [ "$command_status" -ne 0 ]; then
    printf '{"event":"ci_command_failed","exit_code":%s}\n' "$command_status"
fi
exit "$command_status"
