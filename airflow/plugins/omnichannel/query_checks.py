"""Read-only Trino reconciliation after warehouse exports (ADR-0015, P6-09).

Imports happen inside the callable: DAG parsing stays free of I/O and of the /app code path.
"""

from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from pathlib import Path

log = logging.getLogger(__name__)
SQL_FILE = Path("/app/sql/trino/05_warehouse_vs_iceberg.sql")


def reconcile_warehouse_window(url: str | None = None) -> dict:
    """For every run in warehouse.export_state, Iceberg window totals must equal the mirror."""
    from spark.lib.sql_params import render
    from spark.lib.trino_http import run

    url = url or os.environ.get("TRINO_URL", "http://trino:8080")
    export = json.loads(
        run("SELECT json_format(ledger) FROM postgresql.warehouse.export_state", url).rows[0][0]
    )
    sql = SQL_FILE.read_text(encoding="utf-8")
    problems, groups = [], 0
    for run_id, entry in sorted(export["runs"].items()):
        params = {
            "dataset_run_id": run_id,
            "window_start": entry["window_start"],
            "window_end": entry["window_end"],
        }
        for row in run(render(sql, params), url).dicts():
            groups += 1
            diff = Decimal(str(row["net_diff"] or 0))
            if row["lake_lines"] != row["warehouse_lines"] or diff != 0:
                problems.append(
                    {
                        "run_id": run_id,
                        "channel": row["channel"],
                        "currency": row["currency"],
                        "net_diff": str(diff),
                    }
                )
    summary = {
        "event": "query_checks_reconciled",
        "runs": len(export["runs"]),
        "groups": groups,
        "problems": len(problems),
    }
    log.info(json.dumps(summary, sort_keys=True))
    if problems:
        raise ValueError(f"warehouse/Iceberg window mismatch: {problems[:5]}")
    return summary
