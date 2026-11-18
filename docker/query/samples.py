"""Run every sql/trino sample for every dataset and check business invariants (ADR-0015).

Parameters come from Trino itself (latest snapshot, warehouse window from export_state).
Results go to .local-data/query/samples/<run>/<file>.json (git-ignored).
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_sql import render  # noqa: E402
from trino_client import DEFAULT_URL, TrinoQueryError, run  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql" / "trino"
OUT = ROOT / ".local-data" / "query" / "samples"
AS_OF_DATE = "2026-01-03"


def dec(value) -> Decimal:
    return Decimal(str(value)) if value is not None else Decimal(0)


def invariants(name: str, rows: list[dict]) -> list[str]:
    problems = [] if rows else ["no rows"]
    if name.startswith("04_"):
        (r,) = rows
        if r["matched_in_oltp"] != r["gold_orders"] or r["status_mismatches"] != 0:
            problems.append(f"federated mismatch {r}")
        if dec(r["gold_gross_amount"]) != dec(r["oltp_total_amount"]):
            problems.append("gold gross != oltp total")
    if name.startswith("05_"):
        for r in rows:
            if r["lake_lines"] != r["warehouse_lines"] or dec(r["net_diff"]) != 0:
                problems.append(f"warehouse diff {r}")
    if name.startswith("06_"):
        counts = {r["mode"]: r["orders"] for r in rows}
        if counts.get("version") != counts.get("timestamp"):
            problems.append(f"time travel counts differ {counts}")
    if name.startswith("08_"):
        for r in rows:
            if r["is_mature"] is False and r["is_returned"] is not None:
                problems.append(f"immature label has is_returned {r}")
    return problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=DEFAULT_URL)
    args = parser.parse_args(argv)
    url = args.url
    runs = [
        r[0]
        for r in run(
            "SELECT DISTINCT _dataset_run_id FROM iceberg.gold.fact_orders " "ORDER BY 1", url
        ).rows
    ]
    latest = run(
        "SELECT snapshot_id, CAST(committed_at AS varchar) FROM "
        'iceberg.gold."fact_orders$snapshots" ORDER BY committed_at DESC LIMIT 1',
        url,
    )
    snapshot, committed = latest.rows[0]
    export = json.loads(
        run("SELECT json_format(ledger) FROM postgresql.warehouse.export_state", url).rows[0][0]
    )
    failures = 0
    for run_id in runs:
        window = export["runs"][run_id]
        params = {
            "dataset_run_id": run_id,
            "fact_orders_snapshot": str(snapshot),
            "as_of_ts": committed,
            "as_of_date": AS_OF_DATE,
            "window_start": window["window_start"],
            "window_end": window["window_end"],
        }
        for path in sorted(SQL_DIR.glob("*.sql")):
            try:
                result = run(render(path.read_text(encoding="utf-8"), params), url)
            except TrinoQueryError as exc:  # report and continue with the other files
                failures += 1
                print(
                    json.dumps(
                        {
                            "event": "query_sample",
                            "run_id": run_id,
                            "file": path.name,
                            "ok": False,
                            "error_name": exc.error_name,
                            "problems": [str(exc)[:300]],
                        }
                    )
                )
                continue
            rows = result.dicts()
            out = OUT / run_id / f"{path.stem}.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(
                    {"params": params, "columns": result.columns, "rows": result.rows},
                    default=str,
                    indent=1,
                )
            )
            problems = invariants(path.name, rows)
            failures += bool(problems)
            print(
                json.dumps(
                    {
                        "event": "query_sample",
                        "run_id": run_id,
                        "file": path.name,
                        "rows": len(rows),
                        "ok": not problems,
                        "problems": problems[:3],
                    },
                    default=str,
                )
            )
    print(json.dumps({"event": "query_samples_finished", "runs": len(runs), "failures": failures}))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
