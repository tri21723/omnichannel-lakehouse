"""Pinned Silver publication reader and dataset run isolation."""

import hashlib
import json
from pathlib import Path

SILVER_ENTITIES = ("orders", "order_items", "customers", "products", "returns")


def compute_publication_id(ledger_dict):
    """Compute the immutable canonical publication ID from ledger bytes."""
    return hashlib.sha256(json.dumps(ledger_dict, sort_keys=True).encode()).hexdigest()


def load_publication(ledger_path, publication_id=None):
    """Load and validate the completed Silver publication ledger and archive."""
    ledger_path = Path(ledger_path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"Silver publication ledger not found: {ledger_path}")

    ledger = json.loads(ledger_path.read_text())
    if ledger.get("status") != "completed":
        raise ValueError(f"Silver publication is not completed (status={ledger.get('status')})")

    computed_id = compute_publication_id(ledger)
    if publication_id and publication_id != computed_id:
        raise ValueError(
            f"Requested publication ID {publication_id} does not match ledger {computed_id}"
        )

    # Verify immutable publication archive exists and matches
    archive_path = ledger_path.parent / f"publication-{computed_id}.json"
    if archive_path.exists():
        archive = json.loads(archive_path.read_text())
        if archive != ledger:
            raise ValueError("Corrupted publication archive: content mismatch")

    # Verify all 15 Silver tables are present in the ledger inventory
    required_tables = {
        f"lh.silver.{prefix}{entity}"
        for entity in SILVER_ENTITIES
        for prefix in ("", "_contrib_", "quarantine_")
    }
    missing = required_tables - set(ledger.get("tables", {}).keys())
    if missing:
        raise ValueError(f"Incomplete Silver table inventory in ledger: {missing}")

    return ledger, computed_id


def validate_run_membership(ledger, run_id):
    """Verify that run_id exists in ledger slices."""
    matching_slices = [
        batch_id
        for batch_id, slice_info in ledger.get("slices", {}).items()
        if slice_info.get("selection", {}).get("run_id") == run_id
    ]
    if not matching_slices:
        raise ValueError(f"Dataset run {run_id} has no slices in Silver publication")
    return matching_slices


def read_silver_entities(spark, ledger, run_id, publication_id=None):
    """Read five Silver entity tables by pinned snapshot ID and filter by dataset run."""
    from spark.lib.silver_publish import frame_at, summary

    if publication_id is None:
        publication_id = compute_publication_id(ledger)

    validate_run_membership(ledger, run_id)

    from spark.lib.silver_entities import AUDIT, FIELDS

    frames = {}
    snapshots = {}

    for entity in SILVER_ENTITIES:
        table_name = f"lh.silver.{entity}"
        evidence = ledger["tables"][table_name]
        snap_id = evidence["snapshot"]
        snapshots[entity] = snap_id

        expected_ddl = FIELDS[entity] + ", " + AUDIT
        schema = spark.createDataFrame([], expected_ddl).schema
        frame = frame_at(spark, table_name, evidence, schema)

        # Validate summary against committed ledger content
        actual_summary = summary(frame)
        if actual_summary != evidence["content"]:
            raise ValueError(
                f"Silver snapshot verification failed for {table_name}: "
                f"expected {evidence['content']}, got {actual_summary}"
            )

        # Strictly isolate to the selected dataset run
        from pyspark.sql import functions as F

        scoped = frame.filter(F.col("_dataset_run_id") == run_id).localCheckpoint(eager=True)
        frames[entity] = scoped

    return frames, snapshots, publication_id
