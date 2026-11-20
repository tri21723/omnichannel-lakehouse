#!/bin/sh
# Idempotent PostgreSQL bootstrap for DataHub (ADR-0018 §2): metadata store database/role `datahub`
# and the ingestion role `datahub_ro` with schema USAGE only (no table SELECT, so it cannot read
# walmart.customers rows). Passwords come from the environment and are re-applied on every run.
set -eu
export PGHOST=postgres PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD"
psql -X -q -v ON_ERROR_STOP=1 -d postgres \
     -v dh_password="$DATAHUB_DB_PASSWORD" -v ro_password="$DATAHUB_RO_DB_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE datahub LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub') \gexec
ALTER ROLE datahub WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'dh_password';
SELECT 'CREATE DATABASE datahub OWNER datahub' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datahub') \gexec
SELECT 'CREATE ROLE datahub_ro LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_ro') \gexec
ALTER ROLE datahub_ro WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'ro_password';
REVOKE ALL ON DATABASE datahub FROM PUBLIC;
GRANT CONNECT ON DATABASE sources TO datahub_ro;
SQL
psql -X -q -v ON_ERROR_STOP=1 -d sources <<'SQL'
GRANT USAGE ON SCHEMA walmart, warehouse TO datahub_ro;
SQL
psql -X -q -v ON_ERROR_STOP=1 -d sources -At <<'SQL'
SELECT 'datahub_ro select on walmart.customers: ' || has_table_privilege('datahub_ro', 'walmart.customers', 'SELECT');
SQL
printf 'PostgreSQL governance bootstrap complete.\n'
