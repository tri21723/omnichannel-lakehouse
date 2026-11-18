# Local development runbook

1. Install Python 3.11 with venv support and GNU Make.
2. Run `make setup`, `make env`, then `make check`.
3. Optionally run `.venv/bin/pre-commit install` to enable local checks.

`make env` generates credentials for a new .env and preserves an existing file.
Never commit credentials. Environment variables describe container-network hosts,
not host-side client endpoints.

If Docker is missing in WSL, enable Docker Desktop integration for the distro or
install Docker Engine with the Compose plugin. Runtime validation requires
`docker compose version` to succeed. No containers are needed for scaffold tests.

If make lint/test reports a missing executable, run make setup first.
Do not delete volumes to resolve configuration errors without reviewing data impact.

## Core storage

```sh
make env                 # generate local random credentials; existing file is preserved
make config-core         # validate silently (never print interpolated Compose with secrets)
make pull-core
make up-core             # healthy services, then bootstrap
make smoke-core          # SQL + S3 positive and negative permission checks
make init-core           # repeat bootstrap to check idempotency
make restart-core
make verify-persistence-core
make status-core
make down                # preserves named volumes
```

Credentials are in the ignored `.env`; do not paste them into logs/issues.
If upgrading from the empty scaffold environment, add the new required variables
from `.env.example`. Never replace an existing environment containing live credentials.

Host clients: PostgreSQL `localhost:5432`, MinIO API `http://localhost:9000`,
console `http://localhost:9001`. DBeaver uses database `sources`, user `trino_ro`
and `TRINO_RO_DB_PASSWORD`. Container clients use service DNS `postgres` / `minio`.

Smoke tests roll back Walmart/warehouse SQL fixtures. A small persistent marker
is stored in `postgres.public.core_smoke_persistence`, outside business databases. S3 tests keep tiny `_smoke/` marker objects in
the three buckets to verify restart persistence. Raw ingestion must select its
channel/date paths rather than scanning every object in the raw bucket.

If bootstrap fails, inspect the error, correct configuration and run `make init-core`.
Do not remove volumes. Init containers are ephemeral; output is emitted to the terminal.
`make logs SVC=postgres` and `make logs SVC=minio` inspect long-running services.

If WSL reports `docker-credential-desktop.exe: exec format error`, repair Docker
Desktop WSL integration. For public image pulls only, an empty temporary Docker
client configuration can bypass a broken credential helper without editing personal
Docker settings: create an empty JSON config in a temporary directory and pass
`DOCKER_CONFIG=<that-directory> make pull-core`. This does not authenticate private registries.

MinIO is built locally from pinned upstream source releases because the tested
public registries return unauthorized for their old binary images. `make up-core`
runs `make build-core` automatically; the first build needs internet, several GB
of free disk and additional time. Later builds reuse Docker cache. `make pull-core`
only pulls PostgreSQL. The source build includes the upstream AGPL license.

## Source generation

After core storage is healthy, run `make build-generator`, then `make gen-sources`
and `make verify-sources`. On a core installation created before the generator,
run `make init-core` to grant TEMPORARY to the generator role. See
[generator usage and contracts](generators.md) for config overrides and retry behavior.

## Lakehouse Phase 3

```sh
make env-batch          # append missing settings; existing credentials remain
make build-batch
make up-batch
make smoke-lakehouse
make smoke-lakehouse   # read/verify existing fixture on replay
make restart-lakehouse
make verify-lakehouse  # read only; does not regenerate missing fixture
make down              # merged core+batch; no volume deletion
```

Use DOCKER_CONFIG temporary config described above if the Windows credential helper
fails. Run it for the entire make command, including recursive make/build calls.
Lakekeeper health/UI: localhost:8181; Spark UI: localhost:8080.
Two workers advertise 2GiB each; container overhead has a separate allowance.
The driver is a one-shot container named omnichannel-spark-driver, so smoke commands
must run sequentially. This image currently targets Linux amd64 (Java home layout).

REST v0.9.3 resolves the configured warehouse **name**, then supplies the catalog
prefix. Management uses UUID. A UUID in the REST warehouse setting causes NotFound.
Storage profile endpoints are normalized with trailing slash by the API; bootstrap
compares the normalized value and refuses bucket/prefix changes. Authentication is
disabled only for this loopback-bound local deployment, not a production setup.

Migration failure: inspect lakekeeper-migrate logs; fix configuration, rerun migration;
never remove the database to retry. Preserve LAKEKEEPER_ENCRYPTION_KEY in .env.
The PostgreSQL DSN uses the generated hex password; URL-encode a manually changed
password before using reserved URI characters in this configuration.

If classpath fails: compare runtime suffix 3.5_2.12 and both Iceberg 1.9.2 JARs.
If S3 fails: check endpoint inside the Docker network, path-style and service policy;
S3FileIO is not Spark's raw filesystem. If executors cannot reach driver: inspect
service DNS/driver host; host localhost does not identify the driver container.
If a container is killed: inspect memory usage/limits before increasing executor heap.

Partial smoke fixture: the job refuses to reset an existing table without evidence.
Inspect its snapshots and rows, then recover the evidence explicitly; do not drop/purge
business tables. Fixture evidence is /runtime/fixture.json in lakehouse-config volume.

Maintenance is deferred: define retention > required replay/time-travel horizon before
expire_snapshots; only remove orphan files after a safe age exceeding running writes.
Do not run either operation on project data as part of a smoke test. Compaction can
be trialed on the dedicated fixture after taking snapshot evidence.


## GitHub Actions: billing lock and runner notices

If GitHub says the job was not started because the account is locked due to billing,
no workflow step has run. The account or organization billing owner must review
Settings → Billing and licensing, resolve the reported payment issue, then re-run
the job. If no balance, subscription or payment problem is visible, ask GitHub Support
to review the billing lock. Changing runner labels does not unlock the account.

Workflows pin ubuntu-24.04 to avoid automatic OS migrations via ubuntu-latest.
This fixes the runner-selection notice independently of the billing lock; it does
not pin every preinstalled tool version. Local checks remain available via make check.

Reference: https://docs.github.com/en/billing/how-tos/troubleshooting/locked-account
Runner policy: https://github.com/actions/runner-images

## Bronze Phase 4A — initial slice

Requires the source run already published/loaded, then `make build-batch` and
`make up-batch` (workers and driver must share the staging volume).

```sh
make smoke-bronze  # Walmart 2026-01-01, both JSONL and all 5 JDBC tables, replay/verify
make spark-submit JOB=dp1_bronze BRONZE_ARGS='--run-id b1e64afaff0176de180a --run-date 2026-01-01 --channel amazon --source jsonl --mode full'
make verify-bronze BRONZE_ARGS='--run-id b1e64afaff0176de180a --run-date 2026-01-01 --channel walmart --source jdbc --mode full'
```

`make integration-bronze` runs the extended 5-channel/3-day matrix against the
existing default source dataset; it writes Bronze slices and takes longer. The
small config in generators/fixtures is used by the manual Bronze CI workflow,
not automatically published by this command. Do not point the fixed-date matrix at arbitrary date ranges.
Run jobs sequentially: the fixed driver name rejects simultaneous submits and Bronze
also holds a POSIX lock for the full pipeline. A crash releases the lock; a remaining
state JSON with status running is incomplete and must be replayed, not manually
marked completed. Both workers mount /staging read-only; driver alone downloads.

Raw input is limited to BRONZE_STAGE_MAX_BYTES (default 64MiB/slice). Increase only
with measured disk/memory headroom. Raw LF/CRLF terminators are omitted in _raw;
blank or malformed JSON lines remain available with _parse_status. Invalid UTF-8,
checksum or manifest inventory fails before target writing.

Iceberg overwrite-by-expression failed replay because truncated column statistics
could not prove full-file deletion for run IDs. The writer uses an atomic MERGE
with one delete marker plus unmatched insert rows, retaining duplicate rows.
No table reset or data purge is needed for recovery.

JDBC is materialized eagerly through JVM JDBC under an exported PostgreSQL snapshot;
all 5 read-only repeatable-read connections import it before querying. Spark reads
staged raw rows after DB connections close. JDBC incremental refreshes rather than
skipping from the JSON manifest alone. Concurrent mutation and broken-connection tests use dedicated fixture IDs; see
bronze-validation for results and the sequential JDBC reader limitation.

To prepare the small fixture locally:

```sh
make build-generator
make gen-sources GEN_ARGS='--config generators/fixtures/bronze.yaml'
make smoke-bronze BRONZE_RUN_ID=186ae107d0793d2ee237
make test-bronze-recovery BRONZE_RUN_ID=186ae107d0793d2ee237
```

The current make check test count is recorded in bronze-validation.md. The manual .github/workflows/bronze.yml mirrors
small-fixture setup and recovery. The user confirmed billing recovery and hosted CI
success on 2026-11-13; the workflow/run URL was not supplied.

Additional Phase 4A checks (run sequentially after build-batch/up-batch):

```sh
make test-bronze-snapshot
make test-bronze-input
make test-bronze-contract  # read-only, requires the default full reference dataset
```

The first two use compose.bronze-test.yml: only the test driver receives generator
credentials. Snapshot tests clean only snapshot-probe-v1 fixture rows. Input tests
leave small immutable raw fixtures (including intentional malformed/missing-file
inventories); these run IDs are excluded from business ingestion unless selected.
No dataset or bucket reset is needed. The contract test validates Amazon drift,
Shopee timezone strings, Lazada optional brand/currency and default duplicate counts.

Accepted attempts persist failed status after staging/write failure; re-run full to
recover. CLI failure reason codes distinguish missing source, fingerprint mismatch,
staging limit and invalid encoding, without outputting payload/credentials.
CI uses ci_run.sh/ci_events.py to print allowlisted structured summaries only; raw
logs remain on the disposable runner and are not uploaded. Local raw logs must stay
out of Git. Hosted CI success is user-confirmed; validate new workflow changes
against their own runs.

## Silver input checkpoint

After Docker Desktop/WSL integration is enabled, start the stack with `make up-batch`;
starting Docker alone does not start PostgreSQL, MinIO or spark-master. UnknownHost
spark-master / endpoint connection errors indicate the stack must be checked first.

```sh
make build-batch
make up-batch
make smoke-silver-input
```

The smoke uses the existing small run 186ae107d0793d2ee237, pins all five JDBC table
snapshots and replays that Bronze slice. It preserves prior input manifests and
checks missing snapshot rejection. For a fresh environment prepare the small source
and Bronze fixture with the commands in the Bronze section first.
Input pins live in /runtime/silver/inputs/<sha256>.json. They contain references and
summaries only, not raw customer data or credentials. See ADR-0008 and silver-validation.md.

## Silver Walmart normalization (P4B-04)

```sh
make env-silver            # random local HMAC key, preserve existing key/version
make build-batch
make up-batch              # workers need the new Python normalization modules
make test-silver-normalize
make smoke-silver-normalize
make spark-submit JOB=dp2_silver SILVER_ARGS='--run-id 186ae107d0793d2ee237 --run-date 2026-01-01 --channel walmart --mode full --validate-only'
```

Without --validate-only, dp2_silver publishes all five entities and quarantine.
--input-id selects an immutable pin; otherwise the selected current Bronze completion
is validated and pinned. Missing/malformed SILVER_PII_KEY (hex, >=32 decoded bytes)
or missing version fails before Spark starts. Keep the key/version stable; rotation
requires a complete reviewed backfill. Never print or commit the key.

## Silver publication and recovery

```sh
make env-silver
make build-generator build-batch
make up-batch
make prepare-silver-fixture
make test-silver-dq
make integration-silver
make test-silver-recovery
make reconcile-silver
make restart-lakehouse
make verify-silver
```

Run Spark targets sequentially: the fixed driver hostname allows one active submit.
prepare-silver-fixture creates a separate 25-orders/day/channel run and only missing
Bronze completions. It does not reset existing tables or regenerate expired snapshots.
For the default dataset ingest all authoritative Bronze slices before
make integration-silver-reference, then run
make reconcile-silver SILVER_RUN_ID=b1e64afaff0176de180a.

Full/incremental both verify and skip a completed identical pin; a new input replaces
only its contribution slice. The job does not infer CDC deletes. Corrections must
first be re-ingested into Bronze, then explicitly selected. Previously superseded or
unrelated/older snapshots fail; --correction explicitly permits an intentional old
revision, while key/policy migrations remain forbidden.

Completion is /runtime/silver/publication/ledger.json on the shared config volume.
Input pins and archived publication manifests are retained. attempt.json is progress,
not a consumer completion. After failure read last completed ledger snapshots; latest
table heads may contain partial work. Retry the same immutable input under the same
key. Consumers must use the ledger's snapshot-id for every table, including after
restart; verify-silver performs no writes. Do not expire referenced snapshots.

Recovery tests own prefixed SQL rows and a separate run: corrections, missing/invalid
rows, profile sharing, privacy, empty slices, writer conflict, source ancestry, NULL
scope, key ownership and three partial-commit failure points. Cleanup removes only
owned fixture rows and publishes their empty slices; volumes/buckets/DBs survive.

Schema mismatch: stop and review DDL/migration; never DROP/recreate the table to pass.
Changing column order alone is harmless: comparison is by name/type. Initial
unpartitioned bootstrap is evolved by ADD PARTITION FIELD, preserving old snapshots.

Spark 3.5 optimizer can drop grouping attributes when comparing complex aggregate
plans with exceptAll. The monetary verifier uses eager localCheckpoint on both
aggregate sides before comparison, bounding lineage and preserving currency columns.
Local checkpoints are ephemeral; retry the entire job from immutable pins after
executor loss. Full-table reduction/MERGE is intended for bounded local datasets;
file counts/bytes and duration in the ledger are diagnostics, not a large benchmark.

CI: .github/workflows/silver.yml is manual and uses the small fixture, timeout and
make down without volume removal. ci_run.sh emits only allowlisted events/counts;
raw diagnostics remain in ignored .local-data on the disposable runner. No new hosted
Silver run is claimed until an actual run result is available.

## Gold star schema (Phase 4C) and Feature Store / Return Labels (Phase 4D)

```sh
make integration-gold
make verify-gold
make reconcile-gold GOLD_RUN_ID=186ae107d0793d2ee237
make reconcile-gold GOLD_RUN_ID=b1e64afaff0176de180a
make test-gold-dq GOLD_RUN_ID=186ae107d0793d2ee237
make test-gold-recovery

make smoke-features
make verify-features
make smoke-labels
make verify-labels
make integration-dp3
make test-dp3-dq DP3_RUN_ID=186ae107d0793d2ee237
make test-dp3-dq DP3_RUN_ID=b1e64afaff0176de180a
make test-dp3-recovery
```

`dp2_gold` owns the 8 star-schema tables (`lh.gold.dim_*`, `lh.gold.fact_*`) and writes
`/runtime/gold/publication/ledger.json`. `dp3_features` and `dp3_return_labels` consume
pinned snapshot IDs from `/runtime/gold/publication/ledger.json` read-only and record
their own state in `/runtime/dp3/publication/ledger.json` without mutating Gold's ledger.
Both small fixture (`186ae107d0793d2ee237`) and default reference (`b1e64afaff0176de180a`)
coexist in `lh.gold.{feat_customer_daily,feat_product_daily,lbl_order_return}`.

Point-in-time feature cutoffs enforce `order_ts < as_of_ts` and `return_ts < as_of_ts`.
`dp3_return_labels` enforces observation horizon right-censoring (`is_mature = false`
sets `is_returned` and all return label metrics to `NULL`, never `0`).
See [ADR-0010](adr/0010-features-and-return-labels.md), [DP3 validation](dp3-validation.md),
and `.github/workflows/gold-dp3.yml`.

## Downstream Data Warehouse Export (Phase 4E)

```sh
make smoke-warehouse
make verify-warehouse
make integration-warehouse
make reconcile-warehouse
make test-warehouse-dq
make test-warehouse-recovery
```

`dp4_warehouse` reads completed `/runtime/gold/publication/ledger.json` snapshots strictly
read-only (`snapshot-id` pinned), slices facts to `[run_date - 29 days, run_date + 1 day)` UTC,
and replaces all 8 `warehouse.*` tables for `_dataset_run_id` inside one PostgreSQL transaction
via JVM JDBC on the driver using role `spark_writer`. Spark workers never hold DB write passwords.
Export state is stored separately at `/runtime/warehouse/export/ledger.json`. Both small fixture
(`186ae107d0793d2ee237`) and default reference (`b1e64afaff0176de180a`) coexist in PostgreSQL
`warehouse.*` with full PK/FK enforcement and `SELECT` access for `trino_ro`.
See [ADR-0011](adr/0011-warehouse-export.md), [Warehouse validation](warehouse-validation.md),
and `.github/workflows/warehouse.yml`.

## Review fixes (2026-11-16)

Gold publishing rejects NULL/negative monetary and quantity aggregates before commit,
including silent DECIMAL overflow when Spark ANSI is disabled. DP3's closed return
horizon becomes mature only when horizon_end < as_of_ts; equality stays censored.
Label policy v2 is tracked per evaluation date; old dates are recomputed on the next
label job, including incremental runs, rather than skipped as identical inputs.

Warehouse completion now lives in warehouse.export_state, committed in the same JDBC
transaction as the eight tables. File export manifests are recoverable mirrors. CLI
verification reads PostgreSQL metadata first, including if file ledger is missing or
stale. Retrying an identical export verifies all row fingerprints and repairs the
file mirror without rewriting rows. --fail-after db_commit tests this boundary.
Legacy file-only exports are reconciled against pinned Gold until the next rewrite
seeds database metadata. Existing tables and data are preserved; no reset is needed.
Full-row fingerprints include every exported field with UTC microsecond timestamps,
normalized decimals, NULLs and duplicate multiplicity. Counts/totals alone are not
sufficient for identical-export skip. Use make test-review-regressions and
make test-warehouse-review for the new real Spark/JDBC regression cases.

### Follow-up review: DP3 provenance and feature currency

DP3 ledgers now track freshness per table/date. The first refresh of a legacy scope
recomputes it; later identical refreshes can skip. Updating features does not refresh
labels, and changing a label horizon requires recomputing the requested dates.

`Unsupported or mixed feature currency` means eligible Gold facts do not match the
native channel currency in ADR-0010. Inspect the pinned source and correct/re-publish
upstream data; do not relabel monetary values or bypass the guard. Other currencies
need an explicit feature-grain/key migration.

Warehouse retry reads `warehouse.export_state` before the file mirror, including when
`ledger.json` is invalid JSON. Rerun the same export to verify DB evidence and repair
the mirror. If database lookup fails, resolve that error first; stale files are not
used as a fallback for database errors.

### Feature aggregate overflow (policy v3)

`Feature numeric overflow or invalid aggregate` aborts feature computation before
publication. Check sums against the DECIMAL(18,2) limit; a valid individual order
can still overflow a customer/product total. Correct the input or design an explicit
precision migration; do not replace the failed aggregate with zero. Empty activity
continues to use zero amounts and NULL AOV. Requested v2 scopes recompute under v3.

A full DP3 refresh also checks the retained `table_dates` inventory. After partial
legacy migration, dates without provenance still prevent an incorrect full skip.

### Count/quantity exceeds nonnegative INT range

The count/quantity aggregate exceeds the published INT contract (or is NULL).
The job checks the wide value before casting and aborts, including when ANSI is
false. Check source quantities/counts; supporting larger metrics requires an explicit
BIGINT schema migration. Do not disable the guard. Gold v2, feature v4 and label v3
scopes recompute on request; other dates/runs retain their historical snapshots.

## Skew benchmark (Phase 4F)

Run sequentially on core + batch only; no other Spark job may run at the same time.
After code changes run `make build-batch` (job code is copied into the Spark image).

```sh
make test-benchmark                                   # real-Spark variant correctness
make benchmark-dataset BENCH_DATASETS=l1m_control,l1m_skew80   # empty = all datasets
make verify-benchmark-dataset BENCH_DATASETS=l1m_control,l1m_skew80
make benchmark-preflight BENCH_CONFIG=matrix-1m
make benchmark-skew BENCH_CONFIG=matrix-1m            # BENCH_ARGS=--resume to continue
make verify-benchmark BENCH_CONFIG=matrix-1m
make report-benchmark BENCH_CONFIG=matrix-1m          # writes docs/benchmarks/skew-*.{json,csv,md}
```

Configs live in `spark/jobs/benchmark_skew/configs/` (`smoke`, `matrix-100k`, `matrix-1m`,
`ci`); datasets in `configs/datasets.json`. Datasets read the completed Gold publication by
pinned snapshot and are recorded in `/runtime/benchmark/datasets/<name>.json`; rebuilding with
an unchanged spec and Gold publication is skipped. Trials write
`/runtime/benchmark/experiments/<id>/{trials,eventlogs,plans,logs}`; a re-run keeps earlier
artifacts as `*.attemptN.json`. Results go to `lh.benchmark.res_*` / `ref_*`, never to Gold.

- `dataset manifest missing` in preflight: run `make benchmark-dataset` for that config.
- `ground truth check failed`: the amplified rows, one-line-per-order or hot share differ from
  the spec (e.g. a channel without `p00`); fix the spec or the pinned Gold input, do not relax
  the tolerance.
- `no B0 reference`: the first B0 trial of a dataset/workload failed; inspect its log under
  `logs/` and rerun with `BENCH_ARGS=--resume`.
- `correctness_failed`: the variant output differs from B0; it is excluded from speedup but
  kept. Compare `res_*` with `ref_*` before changing any tuning.
- `timeout` or `benchmark_budget_exceeded`: lower the scale or raise `timeout_seconds` within
  the ADR-0012 budget; record the failed cell in the report instead of deleting it.
- Disk retention: event logs and result tables are local evidence. Remove an experiment
  directory only after its report is committed, and never expire Gold snapshots pinned by
  `/runtime/benchmark/datasets/*.json` or the Gold/DP3/warehouse ledgers.

## Airflow orchestration (Phase 5)

Run only `core + batch + orch`. Airflow is also the Spark driver (ADR-0013).

```sh
make env-orch            # adds Airflow secrets/settings to .env once, never rotates them
make build-orch          # builds the Spark image, then the Airflow image
make up-orch             # up-batch, airflow-init (db migrate + pool), Airflow on 127.0.0.1:8081
make smoke-orch          # health, DAG import errors, pool, Spark driver run from Airflow
make test-dags           # DagBag/contract tests inside the Airflow image
make e2e                 # orchestration fixture, backfill 2026-01-01..03 twice via Airflow
make airflow-cli ARGS='dags list'
make restart-orch
```

Login: user `admin`, password `AIRFLOW_ADMIN_PASSWORD` in `.env` (simple auth manager,
local only). DAG/plugin code is bind-mounted read-only and reparsed automatically; changes
under `spark/` still need `make build-orch` because job code is baked into the image.

Daily chain: trigger or backfill `batch_daily` with conf `{"dataset_run_id": "<20 hex>"}`.
It runs `dp1_bronze → dp2_silver → dp2_gold → dp3_features → dp3_return_labels →
dp4_warehouse` for the same logical date. Per-job DAGs can be triggered alone.

- **Backfill stays `queued`:** `batch_daily` is paused at creation. Unpause it
  (`make airflow-cli ARGS='dags unpause batch_daily'`); its `end_date` is in the past and
  `catchup=False`, so no automatic run is created. `make e2e` does this via the API.
- **`Dag run with run_id 'batch_daily__…' not found` in a controller task:** a per-job DAG
  already has a run for that logical date (for example a manual trigger); Airflow allows one
  run per DAG and logical date. Delete that metadata-only run in the UI/API or reuse the
  controller run ID, then clear the failed controller task.
- **Later dates did not start after a failure:** intended. Controller tasks use
  `depends_on_past=True` so Gold/DP3 never run on a dataset missing an earlier date. Fix the
  failed date and rerun it (`airflow backfill create … --reprocess-behavior failed`).
- **`spark_driver_busy` / exit 75:** another Spark driver (a `make` target or another task)
  holds `/runtime/locks/spark-driver.lock`. Airflow tasks wait up to 5 minutes for the lock,
  then fail and retry once after 2 minutes; for `make`, wait and rerun. Avoid long manual Spark
  jobs while a backfill runs. Never delete the lock file while a driver is running.
- **`airflow command error: invalid choice: 'sh'`:** the image entrypoint only treats `bash`
  or `python` as shell commands; compose uses `bash`.
- **`No module named 'airflow'` under `docker exec`:** the image needs
  `PYTHONUSERBASE=/home/airflow/.local` when running as root (set in the Dockerfile).
- **Deadlock with `parallelism=1`:** controller tasks are deferrable; if a controller task
  sits in `running` while the child stays `queued`, check that the triggerer is healthy
  (`/api/v2/monitor/health`).
- **Memory:** Airflow `mem_limit` is 3 GiB (standalone ~1 GiB + one driver JVM). Keep other
  profiles off; check with `docker stats`.
- **Per-date DP3 must be incremental:** `dp3_* --as-of-date D --mode full` treats D as the whole
  run and removes every other date of that run. The DAGs use `--mode incremental`; for a full
  recompute use `--all-dates --mode full`.
- **Warehouse window after a backfill:** the DAG passes `--no-regress`, so replaying older dates
  skips instead of moving `warehouse.*` back in time; the newest date's run owns the window.
- **Iceberg maintenance (dry run):** `make iceberg-report` or unpause `maintenance_iceberg`
  writes `/runtime/maintenance/iceberg-report-<date>.json`. It lists candidates only; pinned
  snapshots are excluded and nothing is expired or deleted (ADR-0014). If
  `unreadable_pin_sources` is not empty, fix that JSON before any future real expiry.
- **Assets tab:** layer assets update when the last task of a per-job DAG succeeds (published or
  verified-identical skip); they do not trigger other DAGs.

## Trino query layer (Phase 6)

Run `make up-query` (PostgreSQL, MinIO, Lakekeeper, Trino on 127.0.0.1:8083; no Spark/Airflow),
then `make smoke-query`. CLI: `make trino-cli ARGS='--execute "SHOW SCHEMAS FROM iceberg"'`.
Config is in `docker/query/trino/etc` (read-only mount); after editing rules or catalogs run
`make restart-query` and `make smoke-query`.

- **`PERMISSION_DENIED` on a read:** the schema is hidden on purpose (`iceberg.bronze` holds raw
  PII, `iceberg.benchmark` is not a contract) or the catalog is not `iceberg`/`postgresql`/`system`.
- **`[redacted]` in `walmart.customers`:** column masks for `full_name`/`email`; use Silver/Gold
  hashes for analysis.
- **Never relax `iceberg.security=read_only`:** file-based rules do not cover
  `ALTER TABLE … EXECUTE`, and Lakekeeper would commit metadata changes (for example
  `expire_snapshots`) on Trino's behalf. Changing access rules requires updating the negative
  tests in `docker/query/smoke.py`.
- **Trino cannot reach MinIO/Lakekeeper:** check `TRINO_S3_*`, `S3_ENDPOINT`, `AWS_DEFAULT_REGION`,
  `LAKEKEEPER_URI`, `LAKEHOUSE_WAREHOUSE` in `.env`; `make smoke-core` checks the S3 user.
- **Memory:** Trino is capped at 2.5 GiB (heap 1.5 GiB). Avoid running `batch` + `orch` + `query`
  together on a ~12 GiB host; stop Airflow/Spark with `docker compose stop` when querying.
- **Samples and parameters:** `make query-samples` runs every `sql/trino` file for every dataset;
  `make query-sql SQL=… PARAMS='dataset_run_id=…'` runs one file with validated parameters.
  `ParamError` means a value failed validation (run IDs are 20 lowercase hex, dates ISO).
- **`make verify-query`** needs `up-batch` (Spark) and `up-query`; it runs a read-only Spark job,
  copies `/runtime/query/parity.json` to `.local-data/query/` and compares with Trino. A
  `spark_parity` mismatch with equal ledger rows usually means a query uses an engine-specific
  function; a `warehouse_three_way` mismatch means the warehouse export and Gold differ (rerun
  `reconcile-warehouse`).
- **`query_checks` DAG** is asset-scheduled on `sources.warehouse` and paused at creation; unpause
  it only while the `query` profile runs, otherwise each warehouse export triggers a failing check.
- **DBeaver:** see [dbeaver.md](dbeaver.md); `make query-jdbc-check` tests the same JDBC path.
