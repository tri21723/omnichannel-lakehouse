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
