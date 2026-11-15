"""Verify partial publish failure injection, ledger preservation, and recovery for DP3."""

import argparse
import json
import logging
from datetime import timedelta
from typing import Any

from pyspark.sql import functions as F

from spark.lib.bronze import event
from spark.lib.dp3_features import build_features
from spark.lib.dp3_input import DEFAULT_GOLD_ROOT, load_gold_publication, read_gold_entities
from spark.lib.dp3_labels import build_return_labels
from spark.lib.dp3_publish import ROOT as DP3_ROOT
from spark.lib.dp3_publish import publish_dp3
from spark.lib.session import get_spark


def test_dp3_recovery(spark: Any, run_id: str) -> None:
    ledger_path = DP3_ROOT / "ledger.json"
    attempt_path = DP3_ROOT / "attempt.json"

    initial_ledger_bytes = ledger_path.read_bytes() if ledger_path.exists() else None

    # Load Gold publication inputs
    gold_ledger, gold_pub_id = load_gold_publication(DEFAULT_GOLD_ROOT / "ledger.json")
    frames, snapshots, pub_id = read_gold_entities(
        spark, gold_ledger, run_id, publication_id=gold_pub_id
    )

    distinct_rows = (
        frames["fact_orders"]
        .select(F.to_date("order_ts").alias("dt"))
        .filter(F.col("dt").isNotNull())
        .distinct()
        .sort("dt")
        .collect()
    )
    feat_dates = [r["dt"].isoformat() for r in distinct_rows]
    mature_dt = (distinct_rows[-1]["dt"] + timedelta(days=15)).isoformat()
    label_dates = feat_dates + ([mature_dt] if mature_dt not in feat_dates else [])
    dates = [feat_dates[-1]]

    features = build_features(spark, frames, run_id, pub_id, dates)
    labels = build_return_labels(spark, frames, run_id, pub_id, dates, horizon_days=14)
    all_tables = {**features, **labels}

    # 1. Failure injection: fail_after="features"
    failed_features = False
    try:
        publish_dp3(
            spark,
            all_tables,
            run_id=run_id,
            gold_pub_id=pub_id,
            root=DP3_ROOT,
            fail_after="features",
            force=True,
            as_of_dates=dates,
        )
    except RuntimeError as err:
        if "Injected features failure" in str(err):
            failed_features = True
    if not failed_features:
        raise AssertionError("Expected failure after features not raised")

    if not attempt_path.exists():
        raise AssertionError("attempt.json missing after failed features attempt")
    attempt_data = json.loads(attempt_path.read_text(encoding="utf-8"))
    if attempt_data.get("status") != "failed":
        raise AssertionError(f"Expected attempt status 'failed', got {attempt_data.get('status')}")

    if initial_ledger_bytes is not None:
        current_bytes = ledger_path.read_bytes()
        if current_bytes != initial_ledger_bytes:
            raise AssertionError("ledger.json modified despite failed publish attempt")

    # 2. Failure injection: fail_after="labels"
    failed_labels = False
    try:
        publish_dp3(
            spark,
            all_tables,
            run_id=run_id,
            gold_pub_id=pub_id,
            root=DP3_ROOT,
            fail_after="labels",
            force=True,
            as_of_dates=dates,
        )
    except RuntimeError as err:
        if "Injected labels failure" in str(err):
            failed_labels = True
    if not failed_labels:
        raise AssertionError("Expected failure after labels not raised")

    attempt_data = json.loads(attempt_path.read_text(encoding="utf-8"))
    if attempt_data.get("status") != "failed":
        raise AssertionError(f"Expected attempt status 'failed', got {attempt_data.get('status')}")

    if initial_ledger_bytes is not None:
        current_bytes = ledger_path.read_bytes()
        if current_bytes != initial_ledger_bytes:
            raise AssertionError("ledger.json modified despite failed publish attempt")

    # 3. Failure injection: fail_after="completion"
    failed_completion = False
    try:
        publish_dp3(
            spark,
            all_tables,
            run_id=run_id,
            gold_pub_id=pub_id,
            root=DP3_ROOT,
            fail_after="completion",
            force=True,
            as_of_dates=dates,
        )
    except RuntimeError as err:
        if "Injected completion failure" in str(err):
            failed_completion = True
    if not failed_completion:
        raise AssertionError("Expected failure after completion not raised")

    attempt_data = json.loads(attempt_path.read_text(encoding="utf-8"))
    if attempt_data.get("status") != "failed":
        raise AssertionError(f"Expected attempt status 'failed', got {attempt_data.get('status')}")

    if initial_ledger_bytes is not None:
        current_bytes = ledger_path.read_bytes()
        if current_bytes != initial_ledger_bytes:
            raise AssertionError("ledger.json modified despite failed publish attempt")

    # 4. Clean publish: must recover atomically and complete across all evaluation dates
    full_features = build_features(spark, frames, run_id, pub_id, feat_dates)
    full_labels = build_return_labels(spark, frames, run_id, pub_id, label_dates, horizon_days=14)
    publish_dp3(
        spark,
        full_features,
        run_id=run_id,
        gold_pub_id=pub_id,
        root=DP3_ROOT,
        force=True,
        as_of_dates=feat_dates,
    )
    ledger, skipped = publish_dp3(
        spark,
        full_labels,
        run_id=run_id,
        gold_pub_id=pub_id,
        root=DP3_ROOT,
        force=True,
        as_of_dates=label_dates,
    )
    if ledger.get("status") != "completed":
        raise AssertionError(f"Recovery failed: ledger status is {ledger.get('status')}")

    attempt_data = json.loads(attempt_path.read_text(encoding="utf-8"))
    if attempt_data.get("status") != "completed":
        raise AssertionError(
            f"Expected attempt status 'completed', got {attempt_data.get('status')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify DP3 fault injection and recovery.")
    parser.add_argument("--run-id", required=True, help="Dataset run ID to test")
    args = parser.parse_args()

    spark = get_spark()
    try:
        test_dp3_recovery(spark, args.run_id)
        event("dp3_recovery_passed", run_id=args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
