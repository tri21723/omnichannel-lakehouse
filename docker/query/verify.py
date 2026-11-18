"""Trino reconciliation against Spark, ledgers and the warehouse mirror (ADR-0015).

Read-only. Input: parity.json produced by spark/jobs/query_parity (exported by
`make verify-query`). Exits non-zero on any mismatch.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from trino_client import DEFAULT_URL, run  # noqa: E402

from spark.lib.query_parity import (  # noqa: E402
    MONETARY,
    canonical_monetary,
    canonical_rows,
    for_trino,
    monetary_from_rows,
    warehouse_mirror_sql,
    warehouse_monetary_sql,
)
from spark.lib.sql_params import literal, render  # noqa: E402

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "trino"


def report(check: str, ok: bool, **details) -> bool:
    print(
        json.dumps(
            {"event": "query_verify_check", "check": check, "ok": ok, **details}, default=str
        )
    )
    return ok


def verify(expected: dict, url: str) -> list[bool]:
    results = []
    snapshots = expected["snapshots"]

    for table, rows in sorted(expected["expected_rows"].items()):
        got = run(
            f"SELECT COUNT(*) FROM iceberg.gold.{table} FOR VERSION AS OF "
            f"{int(snapshots[table])}",
            url,
        ).rows[0][0]
        results.append(report("ledger_rows", got == rows, table=table, trino=got, ledger=rows))

    for run_id, files in sorted(expected["spark_results"].items()):
        params = expected["params"][run_id]
        for name, spark_rows in sorted(files.items()):
            sql = for_trino(render((SQL_DIR / name).read_text(encoding="utf-8"), params), snapshots)
            trino_rows = canonical_rows(run(sql, url).rows)
            spark_rows = [tuple(r) for r in spark_rows]
            only_trino = [r for r in trino_rows if r not in spark_rows]
            only_spark = [r for r in spark_rows if r not in trino_rows]
            results.append(
                report(
                    "spark_parity",
                    not only_trino and not only_spark and len(trino_rows) == len(spark_rows),
                    run_id=run_id,
                    file=name,
                    rows=len(trino_rows),
                    only_trino=only_trino[:3],
                    only_spark=only_spark[:3],
                )
            )

    for run_id, wh in sorted(expected["warehouse"].items()):
        for table in MONETARY:
            lake = run(
                warehouse_monetary_sql(
                    table, wh["gold_snapshots"], run_id, wh["window_start"], wh["window_end"]
                ),
                url,
            ).dicts()
            mirror = run(warehouse_mirror_sql(table, run_id), url).dicts()
            ledger = canonical_monetary(wh["monetary"].get(table, {}))
            lake_m, mirror_m = monetary_from_rows(table, lake), monetary_from_rows(table, mirror)
            lake_rows = sum(r["row_count"] for r in lake)
            mirror_rows = sum(r["row_count"] for r in mirror)
            ledger_rows = wh["rows"].get(f"{table}")
            results.append(
                report(
                    "warehouse_three_way",
                    lake_m == mirror_m == ledger and lake_rows == mirror_rows == ledger_rows,
                    run_id=run_id,
                    table=table,
                    lake_rows=lake_rows,
                    warehouse_rows=mirror_rows,
                    ledger_rows=ledger_rows,
                    groups=len(ledger),
                    diff=(
                        None
                        if lake_m == mirror_m == ledger
                        else {"lake": lake_m, "warehouse": mirror_m, "ledger": ledger}
                    ),
                )
            )

    # Time travel: a dataset added after an older Gold publication is absent at that snapshot.
    current = snapshots["fact_orders"]
    for _run_id, wh in sorted(expected["warehouse"].items()):
        old = wh["gold_snapshots"]["fact_orders"]
        if old == current:
            continue
        for other in sorted(expected["params"]):
            count_sql = (
                "SELECT COUNT(*) FROM iceberg.gold.fact_orders FOR VERSION AS OF {} "
                "WHERE _dataset_run_id = {}"
            )
            at_old = run(count_sql.format(int(old), literal("dataset_run_id", other)), url).rows[0][
                0
            ]
            at_new = run(
                count_sql.format(int(current), literal("dataset_run_id", other)), url
            ).rows[0][0]
            existed = (
                other in expected["warehouse"]
                and expected["warehouse"][other]["gold_snapshots"]["fact_orders"] == old
            )
            ok = (at_old == at_new) if existed else (at_old == 0 and at_new > 0)
            results.append(
                report(
                    "time_travel",
                    ok,
                    old_snapshot=old,
                    run_id=other,
                    at_old=at_old,
                    at_current=at_new,
                    existed_at_old=existed,
                )
            )
        break
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--url", default=DEFAULT_URL)
    args = parser.parse_args(argv)
    expected = json.loads(args.expected.read_text(encoding="utf-8"))
    results = verify(expected, args.url)
    ok = all(results)
    print(
        json.dumps(
            {
                "event": "query_verify_finished",
                "checks": len(results),
                "ok": ok,
                "gold_publication_id": expected["gold_publication_id"],
            }
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
