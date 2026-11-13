"""Immutable, validated Bronze references for Silver. No implicit latest reads."""

import hashlib
import json
import os
import re
import tempfile
from argparse import Namespace
from pathlib import Path
from typing import Any

from spark.lib.bronze import AUDIT_SCHEMA, content_summary, input_digest, slice_filter
from spark.lib.bronze_input import canonical, identity

ENTITIES = ("orders", "order_items", "customers", "products", "returns")


def validate_completion(state: dict, selection: dict) -> dict:
    if set(selection) != {"run_id", "run_date", "channel", "source"}:
        raise ValueError("Exactly one explicit Bronze selection is required")
    args = Namespace(**selection)
    batch = identity(args.run_id, args.run_date, args.channel, args.source)
    if args.channel == "walmart" and args.source != "jdbc":
        raise ValueError("Silver requires authoritative Walmart JDBC")
    if state.get("status") != "completed" or state.get("contract") != "bronze-v1":
        raise ValueError("Completed versioned Bronze input required; replay legacy input full")
    if state.get("selection") != selection or state.get("batch_id") != batch:
        raise ValueError("Bronze selection/batch mismatch")
    manifest = state.get("manifest_hash", "")
    if not isinstance(manifest, str) or not re.fullmatch(r"[a-f0-9]{64}", manifest):
        raise ValueError("Invalid source manifest hash")
    if state.get("input_hash") != input_digest(manifest, args):
        raise ValueError("Bronze input identity mismatch")
    expected = (
        {f"lh.bronze.walmart_{name}_raw" for name in ENTITIES}
        if args.source == "jdbc"
        else {f"lh.bronze.{args.channel}_orders_raw"}
    )
    if set(state.get("tables", {})) != expected:
        raise ValueError("Incomplete or unexpected Bronze table set")
    tables = {}
    for table, evidence in state["tables"].items():
        snap = evidence["snapshot"]
        if snap is not None and (type(snap) is not int or snap <= 0):
            raise ValueError("Invalid snapshot ID")
        content = evidence["content"]
        if set(content) != {"rows", "h1", "h16"} or any(
            not isinstance(v, str) or not re.fullmatch(r"\d{1,38}", v) for v in content.values()
        ):
            raise ValueError("Invalid Bronze content summary")
        if snap is None and any(int(v) != 0 for v in content.values()):
            raise ValueError("NULL snapshot requires empty evidence")
        tables[table] = {"snapshot": snap, "content": dict(content)}
    return {
        "contract": "silver-input-v1",
        "selection": dict(selection),
        "batch_id": batch,
        "manifest_hash": manifest,
        "tables": tables,
    }


def pin_completion(completion: Path, selection: dict, directory: Path) -> Path:
    """Capture atomic Bronze completion once; publish a create-only content-addressed file."""
    content = canonical(validate_completion(json.loads(completion.read_text()), selection)).encode()
    digest = hashlib.sha256(content).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{digest}.json"
    # Hard-link publication exposes only the complete file, even with concurrent callers.
    fd, temporary = tempfile.mkstemp(prefix=".pin-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.read_bytes() != content:
                raise ValueError("Pinned input has been modified") from None
    finally:
        Path(temporary).unlink()
    return target


def load_pin(path: Path) -> dict:
    content = path.read_bytes()
    if path.stem != hashlib.sha256(content).hexdigest():
        raise ValueError("Pinned input fingerprint mismatch")
    state = json.loads(content)
    # Reapply the same table/snapshot trust boundary when reading persisted pins.
    validated = validate_completion(
        {
            **state,
            "status": "completed",
            "contract": "bronze-v1",
            "input_hash": input_digest(state["manifest_hash"], Namespace(**state["selection"])),
        },
        state["selection"],
    )
    if validated != state:
        raise ValueError("Pinned contract differs from validated input")
    return state


def read_pinned(spark: Any, path: Path) -> dict[str, Any]:
    from pyspark.sql import functions as F

    state = load_pin(path)
    args = Namespace(**state["selection"])
    frames = {}
    for table, evidence in state["tables"].items():
        if not spark.catalog.tableExists(table):
            raise ValueError("Pinned Bronze table is missing")
        snap = evidence["snapshot"]
        if snap is None:
            frame = spark.createDataFrame([], AUDIT_SCHEMA)
        else:
            if (
                spark.sql(
                    f"SELECT snapshot_id FROM {table}.snapshots WHERE snapshot_id = {snap}"
                ).count()
                != 1
            ):
                raise ValueError("Pinned snapshot is missing; no latest fallback")
            frame = spark.read.format("iceberg").option("snapshot-id", str(snap)).load(table)
            frame = frame.filter(slice_filter(args) & (F.col("_batch_id") == state["batch_id"]))
        if content_summary(frame) != evidence["content"]:
            raise ValueError("Pinned Bronze reconciliation failed")
        frames[table] = frame
    return frames
