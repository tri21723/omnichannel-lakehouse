"""Pinned Gold star-schema publication reader and dataset run isolation for Phase 4D."""

import hashlib
import json
from pathlib import Path
from typing import Any

from spark.lib.gold_entities import GOLD_KEYS

GOLD_TABLES = list(GOLD_KEYS.keys())
DEFAULT_GOLD_ROOT = Path("/runtime/gold/publication")


def compute_publication_id(ledger_dict: dict[str, Any]) -> str:
    """Compute the immutable canonical publication ID from ledger bytes."""
    return hashlib.sha256(json.dumps(ledger_dict, sort_keys=True).encode()).hexdigest()


def load_gold_publication(
    ledger_path: str | Path, publication_id: str | None = None
) -> tuple[dict[str, Any], str]:
    """Load and validate the completed Gold publication ledger and archive."""
    ledger_path = Path(ledger_path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Gold publication ledger not found: {ledger_path}")

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    if ledger.get("status") != "completed":
        raise ValueError(f"Gold publication is not completed (status={ledger.get('status')})")

    computed_id = compute_publication_id(ledger)
    if publication_id and publication_id != computed_id:
        raise ValueError(
            f"Requested publication ID {publication_id} does not match ledger {computed_id}"
        )

    # Verify immutable publication archive exists and matches if present
    archive_path = ledger_path.parent / f"publication-{computed_id}.json"
    if archive_path.exists():
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        if archive != ledger:
            raise ValueError("Corrupted publication archive: content mismatch")

    # Verify all 8 Gold tables are present in the ledger inventory
    required_tables = {f"lh.gold.{name}" for name in GOLD_TABLES}
    missing = required_tables - set(ledger.get("tables", {}).keys())
    if missing:
        raise ValueError(f"Incomplete Gold table inventory in ledger: {missing}")

    return ledger, computed_id


def validate_gold_run_membership(ledger: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Verify that run_id exists in Gold ledger runs."""
    runs = ledger.get("runs", {})
    if run_id not in runs:
        raise ValueError(f"Dataset run {run_id} is not present in Gold publication runs")
    return runs[run_id]


def read_gold_entities(
    spark: Any,
    ledger: dict[str, Any],
    run_id: str,
    publication_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Read Gold tables by pinned snapshot ID, verify summaries, and scope by run_id."""
    from pyspark.sql import functions as F

    from spark.lib.gold_entities import GOLD_AUDIT, GOLD_FIELDS, SELLER_AUDIT
    from spark.lib.silver_publish import frame_at, summary

    if publication_id is None:
        publication_id = compute_publication_id(ledger)

    validate_gold_run_membership(ledger, run_id)

    frames = {}
    snapshots = {}

    for name in GOLD_TABLES:
        table_name = f"lh.gold.{name}"
        evidence = ledger["tables"][table_name]
        snap_id = evidence["snapshot"]
        snapshots[name] = snap_id

        if spark.catalog.tableExists(table_name):
            schema = spark.table(table_name).schema
        else:
            audit_cols = (
                SELLER_AUDIT
                if name == "dim_seller"
                else ("" if name in ("dim_date", "dim_channel") else GOLD_AUDIT)
            )
            expected_ddl = f"{GOLD_FIELDS[name]}, {audit_cols}" if audit_cols else GOLD_FIELDS[name]
            schema = spark.createDataFrame([], expected_ddl).schema

        frame = frame_at(spark, table_name, evidence, schema)

        # Validate summary against committed ledger content
        actual_summary = summary(frame)
        if actual_summary != evidence["content"]:
            raise ValueError(
                f"Gold snapshot verification failed for {table_name}: "
                f"expected {evidence['content']}, got {actual_summary}"
            )

        # Strictly isolate to the selected dataset run
        scoped = frame.filter(F.col("_dataset_run_id") == run_id)
        if name == "fact_orders":
            scoped = scoped.filter(~F.coalesce(F.col("is_deleted"), F.lit(False)))

        frames[name] = scoped.localCheckpoint(eager=True)

    return frames, snapshots, publication_id
