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
summaries only, not raw customer data or credentials. Silver transforms/publishing
are not implemented by this checkpoint. See ADR-0008 and silver-validation.md.
