# DBeaver with the local Trino (flow Q3)

Trino is local, loopback-only and read-only ([ADR-0015](adr/0015-trino-query-layer.md)).
Start it with `make up-query` and check it with `make smoke-query`.

## Connection template (no secrets)

| Setting | Value |
|---|---|
| Driver | Trino (DBeaver: *Database → New Database Connection → Trino*; let DBeaver download the driver, or point it at `trino-jdbc-484.jar`) |
| JDBC URL | `jdbc:trino://127.0.0.1:8083/iceberg/gold` |
| Host / port | `127.0.0.1` / `8083` |
| Catalog / schema | `iceberg` / `gold` (other: `iceberg.silver`, `postgresql.walmart`, `postgresql.warehouse`) |
| User | any name, for example your own (no authentication locally) |
| Password | leave empty |
| SSL | off (loopback only) |

`make query-jdbc-check` connects with the same driver and URL from the host (Java 17+), so a
failure there points to Trino, not DBeaver.

## What you will see

- Catalogs `iceberg`, `postgresql`, `system`. `iceberg.bronze` (raw payloads with PII) and
  `iceberg.benchmark` are hidden on purpose.
- `postgresql.walmart.customers.full_name` and `email` show `[redacted]`.
- Any write (INSERT, DELETE, CREATE, ALTER … EXECUTE) fails with `Access Denied`.

## Running the sample queries

1. *File → Open File…* and open a file from `sql/trino/` (01–08); each file starts with a comment
   describing its purpose and parameters.
2. Run it (*Ctrl+Enter*). DBeaver asks for the `:name` parameters:
   - `dataset_run_id`: `b1e64afaff0176de180a` (default, 15,000 orders), `186ae107d0793d2ee237`
     (small), or `5f552019ee41c24af95f` (orchestration e2e). Enter it **with quotes**,
     e.g. `'b1e64afaff0176de180a'`, because DBeaver substitutes the text as-is.
   - `as_of_date`: `'2026-01-03'`; `window_start` / `window_end`: e.g. `'2025-12-19 00:00:00 UTC'`
     and `'2026-01-18 00:00:00 UTC'` (see `postgresql.warehouse.export_state` for each run).
   - `fact_orders_snapshot`: a number from `iceberg.gold."fact_orders$snapshots"`;
     `as_of_ts`: e.g. `'2026-11-18 12:00:00 UTC'`.
3. From the shell the same files run with validated parameters:
   `make query-sql SQL=sql/trino/02_top_sellers.sql PARAMS='dataset_run_id=b1e64afaff0176de180a'`.

## Reading tips

- Always filter `_dataset_run_id`; every table holds several datasets.
- Money is DECIMAL per channel currency (USD, VND); never add USD and VND together.
- `SELECT … FROM iceberg.gold.fact_orders` reads the latest snapshot. For a result that matches
  the pipeline ledgers exactly, use `FOR VERSION AS OF <snapshot_id>` (see `06_time_travel.sql`)
  or run `make verify-query`.

## Evidence

The host JDBC check (driver 484, server 484) returned per-dataset counts 375 / 300 / 15,000,
`[redacted]` for `email`, a parameterized count of 300 and `Access Denied` for DELETE. A DBeaver
screenshot is added only when a user provides one; no GUI demo is claimed by the automation.
