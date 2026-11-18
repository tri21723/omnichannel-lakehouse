"""Weekly dry-run Iceberg maintenance report (ADR-0014). Reads metadata tables only.

Never calls expire_snapshots, remove_orphan_files or rewrite_data_files.
"""

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

from spark.lib.benchmark_skew import write_json_atomic
from spark.lib.bronze import event
from spark.lib.iceberg_maintenance import (
    SMALL_FILE_BYTES,
    load_runtime_documents,
    pinned_snapshots,
    table_summary,
)

RUNTIME = Path("/runtime")
NAMESPACES = ("bronze", "silver", "gold", "benchmark")


def parse_as_of(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (
        (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC))
        .astimezone(UTC)
        .replace(tzinfo=None)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--as-of", required=True, help="logical timestamp (Airflow interval end)")
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--keep-last", type=int, default=5)
    parser.add_argument("--namespaces", default=",".join(NAMESPACES))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    as_of = parse_as_of(args.as_of)
    namespaces = [n for n in args.namespaces.split(",") if n in NAMESPACES]

    documents, sources, unreadable = load_runtime_documents(RUNTIME)
    pinned = pinned_snapshots(documents)

    from spark.lib.session import get_spark

    spark = get_spark(app_name="iceberg-maintenance-report")
    tables = []
    try:
        for namespace in namespaces:
            for row in spark.sql(f"SHOW TABLES IN lh.{namespace}").collect():
                name = f"lh.{namespace}.{row['tableName']}"
                snaps = [
                    {"snapshot_id": r["snapshot_id"], "committed_at": r["committed_at"]}
                    for r in spark.sql(
                        f"SELECT snapshot_id, committed_at FROM {name}.snapshots"
                    ).collect()
                ]
                current = {
                    r["snapshot_id"]
                    for r in spark.sql(f"SELECT snapshot_id FROM {name}.refs").collect()
                }
                f = spark.sql(
                    f"SELECT count(*) c, coalesce(sum(file_size_in_bytes), 0) b, "
                    f"count_if(file_size_in_bytes < {SMALL_FILE_BYTES}) s FROM {name}.files"
                ).first()
                tables.append(
                    table_summary(
                        name,
                        snaps,
                        pinned,
                        current,
                        {"count": f["c"], "bytes": f["b"], "small": f["s"]},
                        as_of,
                        args.retention_days,
                        args.keep_last,
                    )
                )
    finally:
        spark.stop()

    report = {
        "schema_version": 1,
        "mode": "dry_run",
        "as_of": as_of.isoformat() + "Z",
        "retention_days": args.retention_days,
        "keep_last": args.keep_last,
        "pin_sources": len(sources),
        "unreadable_pin_sources": unreadable,
        "pinned_snapshot_ids": len(pinned),
        "tables": tables,
    }
    out = RUNTIME / "maintenance" / f"iceberg-report-{as_of.date().isoformat()}.json"
    write_json_atomic(out, report)
    event(
        "iceberg_maintenance_reported",
        tables=len(tables),
        snapshots=sum(t["snapshots"] for t in tables),
        candidates=sum(t["expiry_candidates"] for t in tables),
        small_files=sum(t["small_files"] for t in tables),
        unreadable=len(unreadable),
    )


if __name__ == "__main__":
    main()
