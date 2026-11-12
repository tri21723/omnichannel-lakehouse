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
	$(BIN)/yamllint .github .pre-commit-config.yaml .yamllint.yml generators/config.yaml docker/compose.core.yml
	for script in docker/core/*.sh; do sh -n "$$script" || exit; done
	$(BIN)/sqlfluff lint sql

format: ## Format Python source
	$(BIN)/ruff check --fix .
	$(BIN)/black .

test: ## Run unit tests; use smoke-core for runtime integration
	$(BIN)/python -m pytest

check: lint test ## Run lint and unit tests

up-batch up-orch up-query up-stream up-cdc up-gov gen-batch gen-load gen-stream spark-submit e2e:
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
	$(COMPOSE) down

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
