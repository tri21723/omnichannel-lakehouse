#!/bin/sh
# Idempotent: create/refresh the MinIO user `flink` limited to raw/stream/* and checkpoints/flink/*.
set -eu
mc alias set local "$S3_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc admin user add local "$FLINK_S3_ACCESS_KEY" "$FLINK_S3_SECRET_KEY" >/dev/null
mc admin policy create local flink /bootstrap/policies/flink.json >/dev/null
mc admin policy attach local flink --user "$FLINK_S3_ACCESS_KEY" >/dev/null
printf 'MinIO stream bootstrap complete.\n'
