#!/bin/sh
set -eu
export PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD"
if [ "${1:-}" = verify ]; then
    test "$(psql -X -At -d postgres -c "SELECT marker FROM public.core_smoke_persistence WHERE id = 1")" = 'core-smoke'
    echo 'PostgreSQL persistence passed.'
    exit 0
fi
psql -X -v ON_ERROR_STOP=1 -d postgres <<'SQL'
CREATE TABLE IF NOT EXISTS public.core_smoke_persistence (id integer PRIMARY KEY, marker text NOT NULL);
INSERT INTO public.core_smoke_persistence VALUES (1, 'core-smoke') ON CONFLICT (id) DO NOTHING;
SQL
psql -X -v ON_ERROR_STOP=1 -d sources -f /sql/002_core_smoke.sql
# Use TCP/password authentication, including service database isolation.
PGUSER=airflow PGPASSWORD="$AIRFLOW_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d airflow -c 'SELECT current_user' >/dev/null
PGUSER=lakekeeper PGPASSWORD="$LAKEKEEPER_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d lakekeeper -c 'SELECT current_user' >/dev/null
PGUSER=mlflow PGPASSWORD="$MLFLOW_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d mlflow -c 'SELECT current_user' >/dev/null
PGUSER=spark_reader PGPASSWORD="$SPARK_READER_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d sources -c 'SELECT count(*) FROM walmart.orders' >/dev/null
PGUSER=trino_ro PGPASSWORD="$TRINO_RO_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d sources -c 'SELECT count(*) FROM walmart.orders' >/dev/null
PGUSER=debezium_repl PGPASSWORD="$DEBEZIUM_REPL_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d sources -c 'SELECT count(*) FROM walmart.orders' >/dev/null
if PGUSER=trino_ro PGPASSWORD="$TRINO_RO_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d sources -c 'DELETE FROM walmart.orders WHERE false' >/dev/null 2>&1; then
    echo 'ERROR: read-only SQL role can write'; exit 1
fi
if PGUSER=generator PGPASSWORD="$GENERATOR_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d airflow -c 'SELECT 1' >/dev/null 2>&1; then
    echo 'ERROR: generator can access Airflow database'; exit 1
fi
PGUSER=generator PGPASSWORD="$GENERATOR_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d sources <<'SQL'
BEGIN;
INSERT INTO walmart.customers VALUES ('__smoke__', 'Synthetic', 'smoke@example.invalid', 'VN', now());
INSERT INTO walmart.orders VALUES ('__smoke__', '__smoke__', now(), now(), 'created', 'USD', 1.00, 'VN');
UPDATE walmart.orders SET status = 'paid' WHERE order_id = '__smoke__';
DELETE FROM walmart.orders WHERE order_id = '__smoke__';
ROLLBACK;
SQL
PGUSER=spark_writer PGPASSWORD="$SPARK_WRITER_DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -d sources <<'SQL'
BEGIN;
CREATE TABLE warehouse.__smoke__ (id integer);
DO $$ BEGIN
    IF NOT has_table_privilege('trino_ro', 'warehouse.__smoke__', 'SELECT') THEN
        RAISE EXCEPTION 'Missing default warehouse SELECT grant';
    END IF;
END $$;
ROLLBACK;
SQL
printf 'PostgreSQL smoke passed.\n'
