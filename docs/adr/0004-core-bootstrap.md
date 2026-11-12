# ADR-0004: Re-runnable local core bootstrap

- Status: Accepted.
- PostgreSQL and MinIO use named volumes, healthchecks and bounded memory.
  Host ports bind to loopback; names and ports follow AGENTS.md.
- Separate one-shot init services reconcile state after healthchecks pass.
  PostgreSQL entrypoint init scripts run only on empty data directories, so relying
  on them alone would prevent safe re-runs after partial bootstrap failure.
- Bootstrap is sequential, fail-fast and idempotent for an unchanged schema.
  CREATE TABLE IF NOT EXISTS is not a schema migration mechanism. Future schema
  changes require reviewed migrations. Do not run bootstrap concurrently.
- Roles have separate generated passwords. Only the bootstrap superuser provisions
  objects. Generator has DML on Walmart; spark_reader reads Walmart; spark_writer
  owns warehouse; trino_ro reads both; debezium_repl has replication and source read.
  Airflow, Lakekeeper and MLflow each own an isolated database.
- Default grants cover future Walmart tables created by the bootstrap owner and
  warehouse tables created by spark_writer. Other object owners require new grants.
- CDC publishes walmart.orders only, with REPLICA IDENTITY FULL. No replication
  slot is created until Phase 8. WAL retention is capped at 5GB.
- S3 generator writes raw; Spark reads raw and writes lakehouse/checkpoints;
  Trino reads lakehouse; Lakekeeper writes lakehouse. These are non-root users.
  Credential vending and prefix-scoped access are deferred to Lakekeeper deployment.
- Database/bucket names and ports are fixed contracts in this phase. Keep matching
  values in .env.example; only credentials and image references are interpolated.
- `make env` generates random credentials once and preserves existing files.
  Bootstrap can reconcile service passwords but changing the PostgreSQL superuser
  password in .env alone does not rotate an existing database password.

Sources:
- [Postgres image initialization](https://hub.docker.com/_/postgres)
- [Default privileges](https://www.postgresql.org/docs/16/sql-alterdefaultprivileges.html)
