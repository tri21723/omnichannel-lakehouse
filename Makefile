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
	$(BIN)/yamllint .github .pre-commit-config.yaml .yamllint.yml generators/config.yaml docker/compose.core.yml docker/compose.batch.yml
	for script in docker/core/*.sh; do sh -n "$$script" || exit; done
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
	$(COMPOSE) logs --tail=100 -f $(SVC)

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
spark-submit: ## Submit the supported smoke fixture (JOB=smoke)
	@test "$(JOB)" = "smoke" || (echo 'Supported JOB=smoke; business jobs are not implemented'; exit 2)
	$(MAKE) smoke-lakehouse
