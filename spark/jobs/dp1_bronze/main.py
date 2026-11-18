"""Manifest-selected business Bronze ingestion."""

import argparse
import json
import logging
import os
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from spark.lib.bronze import (
    audit,
    event,
    input_digest,
    jdbc_source_unchanged,
    replace_slice,
    save_state,
    verify_state,
    writer_lock,
)
from spark.lib.bronze_input import CHANNELS, identity, read_manifest, select_files, stage
from spark.lib.session import get_spark


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--channel", required=True, choices=CHANNELS)
    parser.add_argument("--source", choices=("jsonl", "jdbc"), default="jsonl")
    parser.add_argument("--mode", required=True, choices=("full", "incremental"))
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    identity(args.run_id, args.run_date, args.channel, args.source)
    return args


def main(args: argparse.Namespace | None = None, spark: Any = None) -> None:
    args = arguments() if args is None else args
    own_spark = spark is None
    batch = identity(args.run_id, args.run_date, args.channel, args.source)
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["SPARK_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["SPARK_S3_SECRET_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(
            connect_timeout=5,
            read_timeout=30,
            retries={"max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )
    manifest, manifest_hash = read_manifest(client, os.environ["S3_RAW_BUCKET"], args.run_id)
    entries = select_files(manifest, args.run_id, args.run_date, args.channel)
    fingerprint = input_digest(manifest_hash, args)
    root = Path("/runtime/bronze")
    with writer_lock(root):
        path = root / f"{batch}.json"
        old = json.loads(path.read_text()) if path.exists() else {}
        spark = get_spark() if spark is None else spark
        state = None
        started = time.monotonic()
        try:
            valid = verify_state(spark, old, args, fingerprint)
            if args.verify:
                if not valid:
                    raise ValueError("No valid completed Bronze slice")
                event("bronze_verified", batch_id=batch, tables=old["tables"])
                return
            if args.mode == "incremental" and valid and args.source == "jsonl":
                event("bronze_skipped", batch_id=batch)
                return
            if args.mode == "incremental" and valid and args.source == "jdbc":
                # OLTP can change, so JDBC is always read. If the snapshot content evidence
                # (per-table row count + stream SHA-256) equals the completed slice, nothing
                # is written and downstream pins stay unchanged (ADR-0007). A different row
                # order only makes the digest differ, which safely falls back to re-ingesting.
                from spark.lib.bronze_jdbc import stage_jdbc

                probe_root = Path(os.environ["BRONZE_STAGE_DIR"])
                probe_root.mkdir(parents=True, exist_ok=True)
                with tempfile.TemporaryDirectory(prefix="probe-", dir=probe_root) as probe:
                    _files, evidence = stage_jdbc(
                        spark, args, Path(probe), int(os.environ["BRONZE_STAGE_MAX_BYTES"])
                    )
                if jdbc_source_unchanged(valid, old, evidence):
                    event("bronze_skipped", batch_id=batch, reason="source_unchanged")
                    return
            attempt = uuid.uuid4().hex
            stage_root = Path(os.environ["BRONZE_STAGE_DIR"])
            stage_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=attempt + "-", dir=stage_root) as directory:
                from pyspark.sql import functions as F

                state = {
                    "status": "running",
                    "started_at": datetime.now(UTC).isoformat(),
                    "batch_id": batch,
                    "attempt": attempt,
                    "input_hash": fingerprint,
                    "manifest_hash": manifest_hash,
                    "source_evidence": {},
                    "selection": {
                        "run_id": args.run_id,
                        "run_date": args.run_date,
                        "channel": args.channel,
                        "source": args.source,
                    },
                    "contract": "bronze-v1",
                    "tables": {},
                }
                save_state(path, state)
                frames = {}
                source_evidence = {}
                if args.source == "jsonl":
                    files = stage(
                        client,
                        os.environ["S3_RAW_BUCKET"],
                        entries,
                        Path(directory),
                        int(os.environ["BRONZE_STAGE_MAX_BYTES"]),
                    )
                    df = (
                        spark.read.schema("_raw string, _source_file string, _parse_status string")
                        .json([str(p) for p in files])
                        .withColumn("_source_table", F.lit(None).cast("string"))
                    )
                    if df.count() != sum(e["rows"] for e in entries):
                        raise ValueError("Staged row count mismatch")
                    suffix = "_export" if args.channel == "walmart" else ""
                    frames[f"lh.bronze.{args.channel}_orders{suffix}_raw"] = df
                else:
                    from spark.lib.bronze_jdbc import stage_jdbc

                    files, source_evidence = stage_jdbc(
                        spark, args, Path(directory), int(os.environ["BRONZE_STAGE_MAX_BYTES"])
                    )
                    for name, file in files.items():
                        frames[f"lh.bronze.walmart_{name}_raw"] = spark.read.schema(
                            "_raw string, _source_file string, "
                            "_source_table string, _parse_status string"
                        ).json(str(file))
                state["source_evidence"] = source_evidence
                for table, df in frames.items():
                    state["tables"][table] = replace_slice(
                        spark, table, audit(df, args, batch), args
                    )
                duration = time.monotonic() - started
                staged_bytes = sum(p.stat().st_size for p in Path(directory).glob("*.jsonl"))
                rows = sum(int(e["content"]["rows"]) for e in state["tables"].values())
                state["metrics"] = {
                    "duration_seconds": round(duration, 3),
                    "rows": rows,
                    "rows_per_second": round(rows / duration, 3),
                    "staged_bytes": staged_bytes,
                }
                state["status"] = "completed"
                state["completed_at"] = datetime.now(UTC).isoformat()
                save_state(path, state)
                event(
                    "bronze_completed",
                    batch_id=batch,
                    attempt=attempt,
                    tables=state["tables"],
                    metrics=state["metrics"],
                )
        except Exception as exc:
            if state is not None:
                state["status"] = "failed"
                state["failed_at"] = datetime.now(UTC).isoformat()
                state["error_type"] = type(exc).__name__
                save_state(path, state)
            raise
        finally:
            if own_spark:
                spark.stop()


def run_cli() -> None:
    """Report safe reason codes and a nonzero exit, without logging raw exceptions."""
    from botocore.exceptions import ClientError

    reasons = {
        "Object fingerprint mismatch": "source_fingerprint_mismatch",
        "Object row count mismatch": "source_row_count_mismatch",
        "Channel/date outside manifest": "invalid_selection",
        "Staging byte/disk limit exceeded": "staging_limit_exceeded",
        "Object exceeds declared size": "staging_limit_exceeded",
        "JDBC staging byte limit exceeded": "staging_limit_exceeded",
    }
    try:
        main()
    except Exception as exc:
        if isinstance(exc, ClientError):
            code = exc.response["Error"]["Code"]
            reason = (
                "source_object_missing" if code in {"NoSuchKey", "404"} else "source_storage_error"
            )
        elif isinstance(exc, UnicodeError):
            reason = "raw_encoding_invalid"
        else:
            reason = reasons.get(str(exc), "runtime_or_validation_failure")
        event("bronze_failed", error_type=type(exc).__name__, reason=reason)
        raise SystemExit(1) from None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_cli()
