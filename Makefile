SHELL := /bin/sh
.DEFAULT_GOAL := help
PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
COMPOSE := docker compose --env-file .env -f docker/compose.core.yml --profile core

.PHONY: help setup env lint format test check up-core up-batch up-orch up-query up-stream up-cdc up-gov down gen-batch gen-load gen-stream spark-submit e2e logs
help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Create virtualenv and install pinned development tools
	$(PYTHON) -m venv $(VENV)
	$(BIN)/python -m pip install -r requirements-dev.txt

env: ## Generate local credentials; preserve an existing .env
	$(PYTHON) docker/core/env.py

lint: ## Check Python, YAML and SQL
	$(BIN)/ruff check .
	$(BIN)/black --check .
	$(BIN)/yamllint .github .pre-commit-config.yaml .yamllint.yml generators/config.yaml docker/compose.core.yml docker/compose.batch.yml docker/compose.bronze-test.yml
	for script in docker/core/*.sh docker/batch/*.sh; do sh -n "$$script" || exit; done
	$(BIN)/sqlfluff lint sql

format: ## Format Python source
	$(BIN)/black .
	$(BIN)/ruff check --fix .

test: ## Run unit tests; use smoke-core for runtime integration
	$(BIN)/python -m pytest

check: lint test ## Run lint and unit tests

up-orch up-query up-stream up-cdc up-gov gen-stream e2e:
	@echo "Target '$@' is not implemented yet. See docs/implementation-status.md."
	@exit 2

.PHONY: config-core pull-core init-core smoke-core restart-core status-core
config-core: ## Validate Compose without printing secrets
	$(COMPOSE) config --quiet

pull-core: config-core ## Pull pinned PostgreSQL image
	$(COMPOSE) pull postgres postgres-init

up-core: config-core build-core ## Start core services and bootstrap roles/buckets
	$(COMPOSE) up -d --wait postgres minio
	$(MAKE) init-core

init-core: ## Reconcile databases, roles, buckets and policies (services must be healthy)
	$(COMPOSE) run --rm --no-deps postgres-init
	$(COMPOSE) run --rm --no-deps minio-init

smoke-core: ## Check SQL/S3 access, denied operations and bootstrap state
	$(COMPOSE) run --rm --no-deps --entrypoint /bin/sh postgres-init /bootstrap/postgres-smoke.sh
	$(COMPOSE) run --rm --no-deps --entrypoint /bin/sh minio-init /bootstrap/minio-smoke.sh

restart-core: ## Restart core services while preserving named volumes
	$(COMPOSE) restart postgres minio
	$(COMPOSE) up -d --wait postgres minio

status-core: ## Show core service status
	$(COMPOSE) ps -a

down: ## Stop core services without deleting volumes
	$(BATCH) down

logs: ## Follow core logs; optionally SVC=postgres or minio
	$(BATCH) logs --tail=100 -f $(SVC)

.PHONY: format-sql
format-sql: ## Format PostgreSQL DDL and smoke assertions
	$(BIN)/sqlfluff fix sql

.PHONY: verify-persistence-core
verify-persistence-core: ## Verify smoke markers after restart; does not rewrite them
	$(COMPOSE) run --rm --no-deps --entrypoint /bin/sh minio-init /bootstrap/minio-smoke.sh verify
	$(COMPOSE) run --rm --no-deps --entrypoint /bin/sh postgres-init /bootstrap/postgres-smoke.sh verify

.PHONY: build-core
build-core: ## Build MinIO server/client from pinned upstream source releases
	$(COMPOSE) build minio minio-init

GEN_ARGS ?=
GEN := $(COMPOSE) --profile batch run --rm --no-deps generator
.PHONY: build-generator gen-batch gen-load gen-sources verify-sources gen-local
build-generator: ## Build the pinned Python 3.11 generator image
	$(COMPOSE) --profile batch build generator

gen-local: ## Generate local files only (GEN_ARGS supports CLI overrides)
	$(BIN)/python -m generators batch $(GEN_ARGS)

gen-batch: ## Generate/upload five-channel batch JSONL and manifest
	$(GEN) batch --output /data --upload $(GEN_ARGS)

gen-load: ## Load Walmart from the same exported dataset
	$(GEN) load --output /data $(GEN_ARGS)

gen-sources: ## One command for S3 batch + Walmart relational load
	$(GEN) sources --output /data $(GEN_ARGS)

verify-sources: ## Reconcile local/S3 bytes and SQL rows without rewriting data
	$(GEN) verify --output /data --remote --database $(GEN_ARGS)

BATCH := docker compose --env-file .env -f docker/compose.core.yml -f docker/compose.batch.yml --profile core --profile batch
.PHONY: env-batch config-batch build-batch init-lakehouse smoke-lakehouse verify-lakehouse restart-lakehouse
env-batch: env ## Add missing Phase 3 settings without rotating credentials
	$(PYTHON) docker/batch/env.py
config-batch: ## Validate merged core+batch configuration silently
	$(BATCH) config --quiet
build-batch: ## Build Spark with pinned Iceberg dependencies
	$(BATCH) build spark-master
up-batch: env-batch ## Start core, migrate catalog, bootstrap and start Spark
	$(MAKE) up-core
	$(BATCH) up -d --wait lakekeeper
	$(MAKE) init-lakehouse
	$(BATCH) up -d --wait spark-master spark-worker1 spark-worker2
init-lakehouse: ## Reconcile warehouse and save its UUID
	$(BATCH) run --rm --no-deps lakehouse-init
smoke-lakehouse: ## Exercise Iceberg writes and snapshots
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/smoke.py
verify-lakehouse: ## Read existing fixture without rewriting it
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/smoke.py --verify
restart-lakehouse: ## Restart catalog and Spark; preserve data
	$(BATCH) restart lakekeeper spark-master spark-worker1 spark-worker2
	$(BATCH) up -d --wait lakekeeper spark-master spark-worker1 spark-worker2

.PHONY: spark-submit
BRONZE_ARGS ?=
SILVER_ARGS ?=
GOLD_ARGS ?=
FEATURES_ARGS ?=
LABELS_ARGS ?=
WAREHOUSE_ARGS ?=
spark-submit: ## Submit smoke, dp1_bronze, dp2_silver, dp2_gold, dp3_features, dp3_return_labels or dp4_warehouse
	@case "$(JOB)" in smoke) $(MAKE) smoke-lakehouse ;; dp4_warehouse) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp4_warehouse/main.py $(WAREHOUSE_ARGS) ;; dp3_return_labels) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp3_return_labels/main.py $(LABELS_ARGS) ;; dp3_features) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp3_features/main.py $(FEATURES_ARGS) ;; dp2_gold) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/gold_main.py $(GOLD_ARGS) ;; dp2_silver) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/main.py $(SILVER_ARGS) ;; dp1_bronze) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/main.py $(BRONZE_ARGS) ;; *) echo 'Supported JOB=smoke|dp1_bronze|dp2_silver|dp2_gold|dp3_features|dp3_return_labels|dp4_warehouse'; exit 2 ;; esac

.PHONY: verify-objects-lakehouse
verify-objects-lakehouse: ## Inspect Iceberg objects and Spark read-only raw permissions
	$(BATCH) run --rm --no-deps --entrypoint /bin/sh minio-init /bootstrap/lakehouse-smoke.sh

.PHONY: verify-runtime
verify-runtime: ## Inspect JVM classpath in the built Spark image
	$(BATCH) run --rm --no-deps spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/runtime.py

.PHONY: test-catalog-failure
test-catalog-failure: ## Wrong catalog path must fail, then existing fixture must still read
	@if $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/smoke.py --verify --bad-catalog > /tmp/omnichannel-catalog-failure.log 2>&1; then echo 'Expected catalog failure'; exit 1; fi
	@rg -q '404|NotFound' /tmp/omnichannel-catalog-failure.log
	$(MAKE) verify-lakehouse

.PHONY: smoke-bronze verify-bronze integration-bronze
BRONZE_RUN_ID ?= b1e64afaff0176de180a
smoke-bronze: ## Ingest/replay/verify one day Walmart JSONL and JDBC (existing source run)
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/validate.py --run-id $(BRONZE_RUN_ID)
integration-bronze: ## Extended matrix: default dataset 5 JSONL channels/3 dates plus JDBC
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/validate.py --run-id $(BRONZE_RUN_ID) --all-slices
verify-bronze: ## Verify a completed slice; pass selection in BRONZE_ARGS
	$(MAKE) spark-submit JOB=dp1_bronze BRONZE_ARGS='$(BRONZE_ARGS) --verify'

.PHONY: test-bronze-recovery
test-bronze-recovery: ## Test both-worker staging, empty slice, isolation and partial recovery
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/recovery.py --run-id $(BRONZE_RUN_ID)

BRONZE_TEST := docker compose --env-file .env -f docker/compose.core.yml -f docker/compose.batch.yml -f docker/compose.bronze-test.yml --profile core --profile batch
.PHONY: test-bronze-snapshot
test-bronze-snapshot: ## Test concurrent source mutations and broken JDBC connections on owned fixtures
	$(BRONZE_TEST) config --quiet
	$(BRONZE_TEST) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/snapshot_test.py

.PHONY: test-bronze-input
test-bronze-input: ## Test malformed MinIO fixtures and failure paths without changing original sources
	$(BRONZE_TEST) config --quiet
	$(BRONZE_TEST) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/input_test.py

.PHONY: test-bronze-contract
test-bronze-contract: ## Read-only raw drift/duplicate assertions on the default reference dataset
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/contract_test.py --run-id $(BRONZE_RUN_ID)

.PHONY: smoke-silver-input
smoke-silver-input: ## Pin/reconcile five JDBC inputs, replay Bronze and verify old snapshots (small fixture)
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/input_smoke.py --run-id 186ae107d0793d2ee237

.PHONY: env-silver
env-silver: env-batch ## Initialize a persistent random PII HMAC key for local Silver
	$(PYTHON) docker/batch/silver_env.py

.PHONY: smoke-silver-normalize
SILVER_ARGS ?=
smoke-silver-normalize: ## Validate typed Walmart candidates twice without publishing Silver tables
	$(MAKE) spark-submit JOB=dp2_silver SILVER_ARGS='--run-id 186ae107d0793d2ee237 --run-date 2026-01-01 --channel walmart --mode full --validate-only --expect-orders 25 --fail-on-reject'
	$(MAKE) spark-submit JOB=dp2_silver SILVER_ARGS='--run-id 186ae107d0793d2ee237 --run-date 2026-01-01 --channel walmart --mode full --validate-only --expect-orders 25 --fail-on-reject'

.PHONY: test-silver-normalize
test-silver-normalize: ## Verify typed values, total mismatch, parent rejection and accounting in Spark
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/normalization_cases.py

.PHONY: smoke-silver verify-silver
SILVER_RUN_ID ?= 186ae107d0793d2ee237
smoke-silver: ## Publish Walmart first day and replay the same pinned input
	$(MAKE) spark-submit JOB=dp2_silver SILVER_ARGS='--run-id $(SILVER_RUN_ID) --run-date 2026-01-01 --channel walmart --mode full --fail-on-reject'
	$(MAKE) spark-submit JOB=dp2_silver SILVER_ARGS='--run-id $(SILVER_RUN_ID) --run-date 2026-01-01 --channel walmart --mode incremental --fail-on-reject'
verify-silver: ## Verify committed Silver snapshots without publishing
	$(MAKE) spark-submit JOB=dp2_silver SILVER_ARGS='--run-id $(SILVER_RUN_ID) --run-date 2026-01-01 --channel walmart --mode full --verify'

.PHONY: integration-silver integration-silver-reference
integration-silver: ## Five channels x three dates using the small generator fixture
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/integration.py --run-id $(SILVER_RUN_ID) --orders-per-day 25
integration-silver-reference: ## Full default reference dataset: 15000 unique orders
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/integration.py --run-id b1e64afaff0176de180a --orders-per-day 1000

.PHONY: prepare-silver-fixture
prepare-silver-fixture: ## Prepare the small raw/SQL dataset and missing authoritative Bronze completions
	$(MAKE) gen-sources GEN_ARGS='--config generators/fixtures/bronze.yaml'
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/prepare.py --run-id $(SILVER_RUN_ID)

.PHONY: test-silver-recovery
test-silver-recovery: ## Test real source corrections, shared entity retention and failed publication recovery
	$(BRONZE_TEST) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/fixture_test.py

.PHONY: test-silver-dq
test-silver-dq: ## Five-entity DQ, deterministic ties, cross-channel keys and malformed root accounting
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/dq_test.py

.PHONY: reconcile-silver
reconcile-silver: ## Read-only keys and currency/refund reconciliation against pinned authoritative Bronze
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/reference_verify.py --run-id $(SILVER_RUN_ID)

.PHONY: smoke-gold verify-gold integration-gold integration-gold-reference reconcile-gold test-gold-dq test-gold-recovery
GOLD_RUN_ID ?= 186ae107d0793d2ee237
smoke-gold: ## Build/publish Gold star schema on the small fixture (375 orders)
	$(MAKE) spark-submit JOB=dp2_gold GOLD_ARGS='--run-id $(GOLD_RUN_ID) --mode full'
verify-gold: ## Verify committed Gold snapshots without publishing
	$(MAKE) spark-submit JOB=dp2_gold GOLD_ARGS='--run-id $(GOLD_RUN_ID) --verify'
integration-gold: ## Publish both small fixture and default reference datasets into Gold
	$(MAKE) spark-submit JOB=dp2_gold GOLD_ARGS='--run-id 186ae107d0793d2ee237 --mode full'
	$(MAKE) spark-submit JOB=dp2_gold GOLD_ARGS='--run-id b1e64afaff0176de180a --mode full'
	$(MAKE) verify-gold GOLD_RUN_ID=186ae107d0793d2ee237
	$(MAKE) verify-gold GOLD_RUN_ID=b1e64afaff0176de180a
integration-gold-reference: ## Publish and verify default reference dataset into Gold (15000 orders)
	$(MAKE) spark-submit JOB=dp2_gold GOLD_ARGS='--run-id b1e64afaff0176de180a --mode full'
	$(MAKE) verify-gold GOLD_RUN_ID=b1e64afaff0176de180a
reconcile-gold: ## Three-way reconciliation against Silver publication and Bronze totals
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/gold_reconcile.py --run-id $(GOLD_RUN_ID)
test-gold-dq: ## Check surrogate key formats, non-null decimals, item roll-ups and PII guard
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/gold_dq_test.py --run-id $(GOLD_RUN_ID)
test-gold-recovery: ## Verify partial publish failure points and ledger preservation
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/gold_recovery_test.py --run-id $(GOLD_RUN_ID)

.PHONY: smoke-features verify-features smoke-labels verify-labels integration-dp3 integration-dp3-reference test-dp3-dq test-dp3-recovery
DP3_RUN_ID ?= 186ae107d0793d2ee237
smoke-features: ## Build/publish daily features on the small fixture
	$(MAKE) spark-submit JOB=dp3_features FEATURES_ARGS='--run-id $(DP3_RUN_ID) --all-dates --mode full'
verify-features: ## Verify committed DP3 feature tables without modifying data
	$(MAKE) spark-submit JOB=dp3_features FEATURES_ARGS='--run-id $(DP3_RUN_ID) --verify'
smoke-labels: ## Build/publish supervised return labels on the small fixture
	$(MAKE) spark-submit JOB=dp3_return_labels LABELS_ARGS='--run-id $(DP3_RUN_ID) --all-dates --horizon-days 14 --mode full'
verify-labels: ## Verify committed DP3 label tables without modifying data
	$(MAKE) spark-submit JOB=dp3_return_labels LABELS_ARGS='--run-id $(DP3_RUN_ID) --verify'
integration-dp3: ## Build and verify features and labels for both small fixture and default reference
	$(MAKE) spark-submit JOB=dp3_features FEATURES_ARGS='--run-id 186ae107d0793d2ee237 --all-dates --mode full'
	$(MAKE) spark-submit JOB=dp3_return_labels LABELS_ARGS='--run-id 186ae107d0793d2ee237 --all-dates --horizon-days 14 --mode full'
	$(MAKE) spark-submit JOB=dp3_features FEATURES_ARGS='--run-id b1e64afaff0176de180a --all-dates --mode full'
	$(MAKE) spark-submit JOB=dp3_return_labels LABELS_ARGS='--run-id b1e64afaff0176de180a --all-dates --horizon-days 14 --mode full'
	$(MAKE) verify-features DP3_RUN_ID=186ae107d0793d2ee237
	$(MAKE) verify-labels DP3_RUN_ID=186ae107d0793d2ee237
	$(MAKE) verify-features DP3_RUN_ID=b1e64afaff0176de180a
	$(MAKE) verify-labels DP3_RUN_ID=b1e64afaff0176de180a
integration-dp3-reference: ## Build and verify features and labels on the default reference dataset (15000 orders)
	$(MAKE) spark-submit JOB=dp3_features FEATURES_ARGS='--run-id b1e64afaff0176de180a --all-dates --mode full'
	$(MAKE) spark-submit JOB=dp3_return_labels LABELS_ARGS='--run-id b1e64afaff0176de180a --all-dates --horizon-days 14 --mode full'
	$(MAKE) verify-features DP3_RUN_ID=b1e64afaff0176de180a
	$(MAKE) verify-labels DP3_RUN_ID=b1e64afaff0176de180a
test-dp3-dq: ## Check surrogate key formats, non-negative metrics, censoring and point-in-time join
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp3_features/dq_test.py --run-id $(DP3_RUN_ID)
test-dp3-recovery: ## Verify partial publish failure points and ledger preservation for DP3
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp3_features/recovery_test.py --run-id $(DP3_RUN_ID)

.PHONY: smoke-warehouse verify-warehouse integration-warehouse integration-warehouse-reference reconcile-warehouse test-warehouse-dq test-warehouse-recovery
WAREHOUSE_RUN_ID ?= 186ae107d0793d2ee237
smoke-warehouse: ## Export 30-day Gold slice to PostgreSQL warehouse.* on the small fixture
	$(MAKE) spark-submit JOB=dp4_warehouse WAREHOUSE_ARGS='--run-id $(WAREHOUSE_RUN_ID) --mode full'
verify-warehouse: ## Verify committed PostgreSQL warehouse.* ledger and constraints read-only
	$(MAKE) spark-submit JOB=dp4_warehouse WAREHOUSE_ARGS='--verify'
integration-warehouse: ## Export and verify both small fixture and default reference datasets in warehouse.*
	$(MAKE) spark-submit JOB=dp4_warehouse WAREHOUSE_ARGS='--run-id 186ae107d0793d2ee237 --mode full'
	$(MAKE) spark-submit JOB=dp4_warehouse WAREHOUSE_ARGS='--run-id b1e64afaff0176de180a --mode full'
	$(MAKE) verify-warehouse
	$(MAKE) reconcile-warehouse
integration-warehouse-reference: ## Export and verify default reference dataset into warehouse.* (15000 orders)
	$(MAKE) spark-submit JOB=dp4_warehouse WAREHOUSE_ARGS='--run-id b1e64afaff0176de180a --mode full'
	$(MAKE) spark-submit JOB=dp4_warehouse WAREHOUSE_ARGS='--verify --run-id b1e64afaff0176de180a'
reconcile-warehouse: ## Reconcile Gold Iceberg pinned slices with PostgreSQL warehouse.*
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp4_warehouse/reconcile.py
test-warehouse-dq: ## Verify FK integrity, NUMERIC(18,2) reconciliation, PII guard and trino_ro permissions
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp4_warehouse/dq_test.py --run-id $(WAREHOUSE_RUN_ID)
	$(COMPOSE) run --rm --no-deps --entrypoint /bin/sh postgres-init -c 'export PGUSER=trino_ro PGPASSWORD="$$TRINO_RO_DB_PASSWORD"; psql -X -v ON_ERROR_STOP=1 -d sources -c "SELECT count(*) FROM warehouse.fact_orders" >/dev/null; if psql -X -v ON_ERROR_STOP=1 -d sources -c "DELETE FROM warehouse.fact_orders WHERE false" >/dev/null 2>&1; then echo "ERROR: trino_ro can write warehouse"; exit 1; fi'
test-warehouse-recovery: ## Verify 30-day window pruning, JDBC rollback recovery and dataset isolation
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp4_warehouse/recovery_test.py --run-id $(WAREHOUSE_RUN_ID)

.PHONY: test-review-regressions
test-review-regressions: ## Real Spark regressions for label horizon endpoint and Gold numeric overflow
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/review_regressions.py

.PHONY: test-warehouse-review
test-warehouse-review: ## Test committed DB metadata recovery and full-row corruption detection on owned fixture
	$(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp4_warehouse/review_test.py

.PHONY: benchmark-dataset verify-benchmark-dataset benchmark-preflight benchmark-skew verify-benchmark report-benchmark test-benchmark
SPARK_IMAGE := omnichannel/spark:3.5.5-iceberg1.9.2
BENCH_CONFIG ?= smoke
BENCH_DATASETS ?=
BENCH_ARGS ?=
BENCH_ENV = -e BENCH_COMMIT=$$(git rev-parse HEAD)$$(test -z "$$(git status --porcelain -- spark sql)" || echo -dirty) -e BENCH_IMAGE_ID=$$(docker image inspect --format '{{.Id}}' $(SPARK_IMAGE))
BENCH_RUN = $(BATCH) run --rm --no-deps --name omnichannel-spark-driver $(BENCH_ENV)
benchmark-dataset: ## Build pinned skew datasets from Gold (BENCH_DATASETS=a,b; BENCH_ARGS=--force)
	$(BENCH_RUN) spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/benchmark_skew/dataset.py --datasets '$(BENCH_DATASETS)' $(BENCH_ARGS)
verify-benchmark-dataset: ## Re-check pinned benchmark datasets and ground truth read-only
	$(BENCH_RUN) spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/benchmark_skew/dataset.py --mode verify --datasets '$(BENCH_DATASETS)'
benchmark-preflight: ## Check datasets/disk for BENCH_CONFIG without running trials
	$(BENCH_RUN) --entrypoint python spark-submit /app/spark/jobs/benchmark_skew/run.py --config $(BENCH_CONFIG) --preflight-only
benchmark-skew: ## Run skew benchmark BENCH_CONFIG=smoke|matrix-100k|matrix-1m (BENCH_ARGS=--resume)
	$(BENCH_RUN) --entrypoint python spark-submit /app/spark/jobs/benchmark_skew/run.py --config $(BENCH_CONFIG) $(BENCH_ARGS)
verify-benchmark: ## Fail unless every trial of BENCH_CONFIG is ok, correct and has metrics
	$(BENCH_RUN) --entrypoint python spark-submit /app/spark/jobs/benchmark_skew/run.py --config $(BENCH_CONFIG) --verify
report-benchmark: ## Export redacted summary CSV/JSON/Markdown of BENCH_CONFIG to docs/benchmarks
	$(BENCH_RUN) -v "$(CURDIR)/docs/benchmarks:/out" --entrypoint python spark-submit /app/spark/jobs/benchmark_skew/report.py --experiment $$(python3 -c "import json;print(json.load(open('spark/jobs/benchmark_skew/configs/$(BENCH_CONFIG).json'))['experiment_id'])") --out /out
test-benchmark: ## Real-Spark correctness tests for salting/broadcast/AQE variants (no tables)
	$(BENCH_RUN) spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/benchmark_skew/salting_test.py
