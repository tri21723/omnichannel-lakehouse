#!/bin/sh
set -eu
mc alias set generator "$S3_ENDPOINT" "$GENERATOR_S3_ACCESS_KEY" "$GENERATOR_S3_SECRET_KEY" >/dev/null
mc alias set spark "$S3_ENDPOINT" "$SPARK_S3_ACCESS_KEY" "$SPARK_S3_SECRET_KEY" >/dev/null
mc alias set trino "$S3_ENDPOINT" "$TRINO_S3_ACCESS_KEY" "$TRINO_S3_SECRET_KEY" >/dev/null
mc alias set lakekeeper "$S3_ENDPOINT" "$LAKEKEEPER_S3_ACCESS_KEY" "$LAKEKEEPER_S3_SECRET_KEY" >/dev/null
# Dedicated deterministic smoke objects also serve as restart persistence markers.
if [ "${1:-}" = verify ]; then
    test "$(mc cat spark/raw/_smoke/core.txt)" = 'core-smoke'
    test "$(mc cat trino/lakehouse/_smoke/core.txt)" = 'core-smoke'
    echo 'MinIO persistence passed.'
    exit 0
fi
printf 'core-smoke' >/tmp/core-smoke.txt
mc cp /tmp/core-smoke.txt generator/raw/_smoke/core.txt >/dev/null
test "$(mc cat spark/raw/_smoke/core.txt)" = 'core-smoke'
mc cp /tmp/core-smoke.txt spark/lakehouse/_smoke/core.txt >/dev/null
mc cp /tmp/core-smoke.txt spark/checkpoints/_smoke/core.txt >/dev/null
test "$(mc cat trino/lakehouse/_smoke/core.txt)" = 'core-smoke'
test "$(mc cat lakekeeper/lakehouse/_smoke/core.txt)" = 'core-smoke'
mc cp /tmp/core-smoke.txt lakekeeper/lakehouse/_smoke/catalog.txt >/dev/null
if mc cp /tmp/core-smoke.txt trino/lakehouse/_smoke/denied.txt >/dev/null 2>&1; then
    echo 'ERROR: Trino S3 role can write'; exit 1
fi
if mc cat generator/lakehouse/_smoke/core.txt >/dev/null 2>&1; then
    echo 'ERROR: generator can access lakehouse'; exit 1
fi
printf 'MinIO smoke passed.\n'
