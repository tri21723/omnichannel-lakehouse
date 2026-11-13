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
spark-submit: ## Submit smoke, dp1_bronze or dp2_silver (use BRONZE_ARGS / SILVER_ARGS)
	@case "$(JOB)" in smoke) $(MAKE) smoke-lakehouse ;; dp2_silver) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp2_silver/main.py $(SILVER_ARGS) ;; dp1_bronze) $(BATCH) run --rm --no-deps --name omnichannel-spark-driver spark-submit /opt/spark/bin/spark-submit /app/spark/jobs/dp1_bronze/main.py $(BRONZE_ARGS) ;; *) echo 'Supported JOB=smoke|dp1_bronze|dp2_silver'; exit 2 ;; esac

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
