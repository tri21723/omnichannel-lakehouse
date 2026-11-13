#!/bin/sh
set -eu
mc alias set spark "$S3_ENDPOINT" "$SPARK_S3_ACCESS_KEY" "$SPARK_S3_SECRET_KEY" >/dev/null
metadata=$(mc find spark/lakehouse/warehouse --name '*.metadata.json' | wc -l)
parquet=$(mc find spark/lakehouse/warehouse --name '*.parquet' | wc -l)
test "$metadata" -gt 0
test "$parquet" -gt 0
printf 'Iceberg objects verified: metadata=%s parquet=%s\n' "$metadata" "$parquet"
printf 'denied-test' >/tmp/lakehouse-denied.txt
if mc cp /tmp/lakehouse-denied.txt spark/raw/_smoke/lakehouse-denied.txt >/dev/null 2>&1; then
    echo 'ERROR: Spark can write raw'; exit 1
fi
echo 'Spark raw write correctly denied.'
