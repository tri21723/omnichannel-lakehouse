#!/bin/sh
set -eu
mc alias set local "$S3_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
for bucket in raw lakehouse checkpoints; do
    mc mb --ignore-existing "local/$bucket"
done
mc admin user add local "$GENERATOR_S3_ACCESS_KEY" "$GENERATOR_S3_SECRET_KEY" >/dev/null
mc admin policy create local generator /bootstrap/policies/generator.json >/dev/null
mc admin policy attach local generator --user "$GENERATOR_S3_ACCESS_KEY" >/dev/null
mc admin user add local "$SPARK_S3_ACCESS_KEY" "$SPARK_S3_SECRET_KEY" >/dev/null
mc admin policy create local spark /bootstrap/policies/spark.json >/dev/null
mc admin policy attach local spark --user "$SPARK_S3_ACCESS_KEY" >/dev/null
mc admin user add local "$TRINO_S3_ACCESS_KEY" "$TRINO_S3_SECRET_KEY" >/dev/null
mc admin policy create local trino /bootstrap/policies/trino.json >/dev/null
mc admin policy attach local trino --user "$TRINO_S3_ACCESS_KEY" >/dev/null
mc admin user add local "$LAKEKEEPER_S3_ACCESS_KEY" "$LAKEKEEPER_S3_SECRET_KEY" >/dev/null
mc admin policy create local lakekeeper /bootstrap/policies/lakekeeper.json >/dev/null
mc admin policy attach local lakekeeper --user "$LAKEKEEPER_S3_ACCESS_KEY" >/dev/null
printf 'MinIO bootstrap complete.\n'
