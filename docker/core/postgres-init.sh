#!/bin/sh
set -eu
export PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD"
psql -X -v ON_ERROR_STOP=1 -f /bootstrap/roles.psql
psql -X -v ON_ERROR_STOP=1 -d sources -f /sql/001_walmart.sql
psql -X -v ON_ERROR_STOP=1 -d sources -f /bootstrap/grants.psql
for db in airflow lakekeeper mlflow; do
    psql -X -v ON_ERROR_STOP=1 -d "$db" -c 'REVOKE CREATE ON SCHEMA public FROM PUBLIC'
done
printf 'PostgreSQL bootstrap complete.\n'
