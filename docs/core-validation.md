# Core storage validation — 2026-11-12

Environment: WSL Linux, Docker Engine 29.5.2, Compose v5.1.4, Python 3.12.3.

| Check | Result |
| --- | --- |
| `make check` | Ruff, Black, YAML, SQL, shell syntax and 9 unit tests pass |
| `make config-core` | Compose validates without printing credentials |
| `make build-core` | Server/client compile successfully from pinned release tags |
| `make up-core` | PostgreSQL and MinIO healthy; bootstrap succeeds |
| `make smoke-core` | SQL/S3 reads, writes and negative permission checks pass |
| `make init-core` then `make smoke-core` | Repeated bootstrap and smoke succeed |
| `make restart-core` | Both services return healthy |
| `make verify-persistence-core` | Pre-existing SQL and S3 markers survive restart |

Runtime output excerpts:

```text
PostgreSQL bootstrap complete.
MinIO bootstrap complete.
PostgreSQL smoke passed.
MinIO smoke passed.
MinIO persistence passed.
PostgreSQL persistence passed.
```

SQL tests exercise all service logins, generator INSERT/UPDATE/DELETE, warehouse
DDL and default Trino grants. Assertions cover logical WAL, replication privilege,
publication and replica identity. Negative tests reject Trino writes and generator
access to the Airflow database. Fixtures in sources are rolled back.

S3 tests exercise generator raw writes, Spark raw reads and lakehouse/checkpoint
writes, Trino reads and Lakekeeper reads/writes. Trino writes and generator reads
from lakehouse must fail. Persistence verification reads existing markers without
rewriting them. It tests container restart, not backup/restore or host failure.

Docker Desktop's Windows credential helper failed in WSL. Public image operations
used a temporary empty Docker client config; personal Docker configuration was not
changed. Quay MinIO image pulls returned 401 and Docker Hub server lookup was denied,
so both MinIO images were built from upstream source instead. Initial server compile
step took about 260 seconds; subsequent build used cache.

Not covered: DBeaver UI connection, Python boto3 access (mc covers S3 access here),
Spark/Iceberg/Trino runtime integration, CDC capture/replay and cloud deployment.
These checks do not claim completion of every manual DoD item in the original plan.
