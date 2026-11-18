"""Trino smoke and read-only checks (ADR-0015). Runs from the host against 127.0.0.1:8083.

Prints one JSON line per check and exits non-zero on any failure. Write probes are chosen to
be harmless even if a rule were missing (WHERE false, 100-year expiry threshold, new names).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from trino_client import DEFAULT_URL, TrinoQueryError, run  # noqa: E402

DENIED = [
    "CREATE TABLE iceberg.gold.p6_write_probe (x integer)",
    "CREATE SCHEMA iceberg.p6_write_probe",
    "INSERT INTO iceberg.gold.dim_channel SELECT * FROM iceberg.gold.dim_channel WHERE false",
    "DELETE FROM iceberg.gold.dim_channel WHERE false",
    "ALTER TABLE iceberg.gold.dim_channel EXECUTE"
    " expire_snapshots(retention_threshold => '36500d')",
    "ALTER TABLE iceberg.gold.dim_channel EXECUTE"
    " remove_orphan_files(retention_threshold => '36500d')",
    "CALL iceberg.system.unregister_table('gold', 'p6_write_probe')",
    "CREATE TABLE postgresql.warehouse.p6_write_probe (x integer)",
    "INSERT INTO postgresql.warehouse.dim_channel SELECT * FROM postgresql.warehouse.dim_channel"
    " WHERE false",
    "DELETE FROM postgresql.walmart.orders WHERE false",
    "SELECT count(*) FROM iceberg.bronze.walmart_customers_raw",
    "SELECT count(*) FROM iceberg.bronze.amazon_orders_raw",
    "SELECT count(*) FROM iceberg.benchmark.items_s15k_skew80",
]


def check(name, ok, **details):
    print(json.dumps({"event": "query_smoke_check", "check": name, "ok": bool(ok), **details}))
    return bool(ok)


def scalar(sql, url):
    return run(sql, url).rows[0][0]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=DEFAULT_URL)
    args = parser.parse_args(argv)
    url, results = args.url, []

    catalogs = sorted(r[0] for r in run("SHOW CATALOGS", url).rows)
    results.append(
        check("catalogs", catalogs == ["iceberg", "postgresql", "system"], catalogs=catalogs)
    )
    iceberg = {r[0] for r in run("SHOW SCHEMAS FROM iceberg", url).rows}
    results.append(
        check(
            "iceberg_schemas",
            {"gold", "silver"} <= iceberg and not {"bronze", "benchmark"} & iceberg,
            schemas=sorted(iceberg),
        )
    )
    pg = {r[0] for r in run("SHOW SCHEMAS FROM postgresql", url).rows}
    results.append(check("postgresql_schemas", {"walmart", "warehouse"} <= pg, schemas=sorted(pg)))
    counts = run(
        "SELECT _dataset_run_id, count(*) FROM iceberg.gold.fact_orders GROUP BY 1 ORDER BY 1", url
    ).rows
    results.append(check("iceberg_gold_counts", len(counts) >= 3, counts=counts))
    wh = run(
        "SELECT _dataset_run_id, count(*) FROM postgresql.warehouse.fact_orders"
        " GROUP BY 1 ORDER BY 1",
        url,
    ).rows
    results.append(check("warehouse_counts", len(wh) >= 3, counts=wh))
    snaps = scalar('SELECT count(*) FROM iceberg.gold."fact_orders$snapshots"', url)
    results.append(check("iceberg_metadata_table", snaps > 0, snapshots=snaps))

    masked = run("SELECT full_name, email FROM postgresql.walmart.customers LIMIT 50", url).rows
    star = run("SELECT * FROM postgresql.walmart.customers LIMIT 50", url)
    raw_values = [v for row in star.rows for v in row if isinstance(v, str) and "@" in v]
    results.append(
        check(
            "pii_masked",
            masked and all(v == "[redacted]" for row in masked for v in row) and not raw_values,
            rows=len(masked),
            star_columns=star.columns,
        )
    )

    for sql in DENIED:
        try:
            run(sql, url)
        except TrinoQueryError as exc:
            results.append(check("denied", exc.access_denied, sql=sql, error=exc.error_name))
        else:
            results.append(check("denied", False, sql=sql, error="NOT DENIED"))

    passed = all(results)
    print(json.dumps({"event": "query_smoke_finished", "checks": len(results), "ok": passed}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
