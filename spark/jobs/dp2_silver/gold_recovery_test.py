"""Verify partial publish failure injection, ledger preservation, and recovery for Gold."""

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from spark.lib.bronze import event
from spark.lib.gold_input import load_publication, read_silver_entities
from spark.lib.gold_pipeline import build_dimensions, build_facts
from spark.lib.gold_publish import ROOT as GOLD_ROOT
from spark.lib.gold_publish import publish_gold, verify_gold
from spark.lib.session import get_spark

SILVER_ROOT = Path("/runtime/silver/publication")


def test_gold_recovery(spark: Any, run_id: str) -> None:
    ledger_path = GOLD_ROOT / "ledger.json"
    attempt_path = GOLD_ROOT / "attempt.json"

    initial_ledger_bytes = ledger_path.read_bytes() if ledger_path.exists() else None

    # Load input frames
    silver_ledger, silver_pub_id = load_publication(SILVER_ROOT / "ledger.json")
    silver_frames, silver_snapshots, pub_id = read_silver_entities(
        spark, silver_ledger, run_id, publication_id=silver_pub_id
    )
    dimensions = build_dimensions(spark, silver_frames, run_id, pub_id, silver_snapshots)
    facts = build_facts(spark, silver_frames, dimensions, run_id, pub_id, silver_snapshots)
    all_tables = {**dimensions, **facts}

    # 1. Failure injection: fail_after="dimensions"
    failed_dim = False
    try:
        publish_gold(
            spark,
            all_tables,
            run_id=run_id,
            silver_pub_id=pub_id,
            root=GOLD_ROOT,
            fail_after="dimensions",
            force=True,
        )
    except RuntimeError as err:
        if "Injected dimensions failure" in str(err):
            failed_dim = True
    if not failed_dim:
        raise AssertionError("Expected failure after dimensions not raised")

    if not attempt_path.exists():
        raise AssertionError("attempt.json missing after failed attempt")
    attempt_data = json.loads(attempt_path.read_text(encoding="utf-8"))
    if attempt_data.get("status") != "failed":
        raise AssertionError(f"Expected attempt status 'failed', got {attempt_data.get('status')}")

    if initial_ledger_bytes is not None:
        current_bytes = ledger_path.read_bytes()
        if current_bytes != initial_ledger_bytes:
            raise AssertionError("ledger.json was mutated during failed dimensions publication")

    # 2. Failure injection: fail_after="facts"
    failed_facts = False
    try:
        publish_gold(
            spark,
            all_tables,
            run_id=run_id,
            silver_pub_id=pub_id,
            root=GOLD_ROOT,
            fail_after="facts",
            force=True,
        )
    except RuntimeError as err:
        if "Injected facts failure" in str(err):
            failed_facts = True
    if not failed_facts:
        raise AssertionError("Expected failure after facts not raised")

    attempt_data = json.loads(attempt_path.read_text(encoding="utf-8"))
    if attempt_data.get("status") != "failed":
        raise AssertionError(f"Expected attempt status 'failed', got {attempt_data.get('status')}")

    if initial_ledger_bytes is not None:
        current_bytes = ledger_path.read_bytes()
        if current_bytes != initial_ledger_bytes:
            raise AssertionError("ledger.json was mutated during failed facts publication")

    # 3. Successful recovery
    res, _ = publish_gold(
        spark,
        all_tables,
        run_id=run_id,
        silver_pub_id=pub_id,
        root=GOLD_ROOT,
        force=True,
    )
    if res.get("status") != "completed":
        raise AssertionError(f"Recovery publish did not complete: {res}")

    # 4. Verify post-recovery tables
    verify_gold(spark, res, target_run_id=run_id)
    event("gold_recovery_tested", run_id=run_id, status=res.get("status"))


def run() -> None:
    parser = argparse.ArgumentParser(
        description="Test Gold publication failure injection and recovery"
    )
    parser.add_argument("--run-id", default="186ae107d0793d2ee237")
    args = parser.parse_args()

    spark = get_spark()
    try:
        test_gold_recovery(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
