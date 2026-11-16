"""Live recovery, transactional rollback, and 30-day sliding-window pruning test."""

import argparse
import json
import logging
from datetime import date
from typing import Any

from spark.lib.bronze import event
from spark.lib.bronze_jdbc import rollback_close
from spark.lib.session import get_spark
from spark.lib.warehouse_input import (
    DEFAULT_GOLD_ROOT,
    read_gold_star_schema,
    slice_warehouse_window,
)
from spark.lib.warehouse_jdbc import (
    WarehouseFaultInjectionError,
    count_run_rows,
    writer_connection,
)
from spark.lib.warehouse_publish import ROOT as WAREHOUSE_ROOT
from spark.lib.warehouse_publish import publish_warehouse, verify_warehouse_ledger
from spark.lib.warehouse_reconcile import reconcile_run


def _pg_out_of_window_orders(spark: Any, run_id: str, cutoff_iso: str) -> int:
    conn = writer_connection(spark, read_only=True)
    try:
        ps = conn.prepareStatement(
            "SELECT COUNT(*) FROM warehouse.fact_orders "
            "WHERE _dataset_run_id = ? AND order_ts < ?::timestamptz"
        )
        try:
            ps.setString(1, run_id)
            ps.setString(2, cutoff_iso)
            rs = ps.executeQuery()
            try:
                rs.next()
                return int(rs.getLong(1))
            finally:
                rs.close()
        finally:
            ps.close()
    finally:
        rollback_close(conn)


def _pg_run_counts(spark: Any, run_id: str) -> dict[str, int]:
    conn = writer_connection(spark, read_only=True)
    try:
        return count_run_rows(conn, run_id)
    finally:
        rollback_close(conn)


def run_recovery_suite(spark: Any, run_id: str) -> None:
    ledger_path = WAREHOUSE_ROOT / "ledger.json"
    attempt_path = WAREHOUSE_ROOT / "attempt.json"

    frames, _snapshots, _meta, gold_pub_id = read_gold_star_schema(
        spark, run_id, gold_root=DEFAULT_GOLD_ROOT
    )

    # 1. Baseline full window run_date=2026-01-17 (covers 2026-01-01..2026-01-03)
    full_date = date(2026, 1, 17)
    full_sliced, full_start, full_end = slice_warehouse_window(frames, full_date)
    full_ledger, _ = publish_warehouse(
        spark,
        full_sliced,
        run_id,
        gold_pub_id,
        full_date,
        full_start,
        full_end,
        root=WAREHOUSE_ROOT,
        force=True,
    )
    full_counts = _pg_run_counts(spark, run_id)
    if full_counts["fact_orders"] <= 0:
        raise AssertionError(f"Expected non-empty orders for baseline, got {full_counts}")

    # Record coexisting runs before sliding/fault tests so we assert they stay untouched
    other_run_counts = {
        other_id: _pg_run_counts(spark, other_id)
        for other_id in full_ledger.get("runs", {})
        if other_id != run_id
    }

    # 2. Slide forward to run_date=2026-02-01 (window [2026-01-03, 2026-02-02) prunes Jan 01-02)
    pruned_date = date(2026, 2, 1)
    pruned_sliced, pruned_start, pruned_end = slice_warehouse_window(frames, pruned_date)
    pruned_ledger, skipped_pruned = publish_warehouse(
        spark,
        pruned_sliced,
        run_id,
        gold_pub_id,
        pruned_date,
        pruned_start,
        pruned_end,
        root=WAREHOUSE_ROOT,
    )
    if skipped_pruned:
        raise AssertionError("Advancing run_date from 2026-01-17 to 2026-02-01 must not skip")

    stale_orders = _pg_out_of_window_orders(spark, run_id, "2026-01-03T00:00:00Z")
    if stale_orders != 0:
        raise AssertionError(
            f"Expected 0 orders before 2026-01-03 after slide, found {stale_orders}"
        )

    pruned_counts = _pg_run_counts(spark, run_id)
    if not (0 < pruned_counts["fact_orders"] < full_counts["fact_orders"]):
        raise AssertionError(
            f"Expected strict window pruning reduction ({full_counts['fact_orders']} -> pruned), "
            f"got {pruned_counts['fact_orders']}"
        )
    reconcile_run(spark, run_id, pruned_ledger["runs"][run_id], gold_root=DEFAULT_GOLD_ROOT)

    # 3. Transactional fault injection across all 3 boundaries: must rollback DB & keep ledger
    saved_ledger_bytes = ledger_path.read_bytes()
    for stage in ("dimensions", "facts", "completion"):
        raised = False
        try:
            publish_warehouse(
                spark,
                full_sliced,
                run_id,
                gold_pub_id,
                full_date,
                full_start,
                full_end,
                root=WAREHOUSE_ROOT,
                fail_after=stage,
                force=True,
            )
        except WarehouseFaultInjectionError:
            raised = True

        if not raised:
            raise AssertionError(f"Fault injection stage {stage!r} failed to raise")

        attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
        if attempt.get("status") != "failed":
            raise AssertionError(f"Expected failed attempt for stage {stage}, got {attempt}")

        if ledger_path.read_bytes() != saved_ledger_bytes:
            raise AssertionError(f"ledger.json mutated despite rollback at stage {stage}")

        current_counts = _pg_run_counts(spark, run_id)
        if current_counts != pruned_counts:
            raise AssertionError(
                f"PostgreSQL was not rolled back at stage {stage}: "
                f"expected {pruned_counts}, got {current_counts}"
            )

    # 4. Recover cleanly back to baseline run_date=2026-01-17
    recovered_ledger, skipped_recovery = publish_warehouse(
        spark,
        full_sliced,
        run_id,
        gold_pub_id,
        full_date,
        full_start,
        full_end,
        root=WAREHOUSE_ROOT,
    )
    if skipped_recovery:
        raise AssertionError("Recovery run after rollback should execute and commit")

    verify_warehouse_ledger(spark, recovered_ledger, target_run_id=run_id)
    reconcile_run(spark, run_id, recovered_ledger["runs"][run_id], gold_root=DEFAULT_GOLD_ROOT)
    recovered_counts = _pg_run_counts(spark, run_id)
    if recovered_counts != full_counts:
        raise AssertionError(f"Recovery counts mismatch: {recovered_counts} != {full_counts}")

    for other_id, prev_counts in other_run_counts.items():
        after_counts = _pg_run_counts(spark, other_id)
        if after_counts != prev_counts:
            raise AssertionError(
                f"Coexisting run {other_id} mutated during {run_id} recovery: "
                f"{prev_counts} -> {after_counts}"
            )

    event(
        "warehouse_recovery_passed",
        run_id=run_id,
        full_counts=full_counts,
        pruned_counts=pruned_counts,
        recovered_counts=recovered_counts,
        coexisting_runs_preserved=sorted(other_run_counts.keys()),
    )


def run() -> None:
    parser = argparse.ArgumentParser(
        description="Test 30-day window pruning, JDBC rollback, and recovery for dp4_warehouse."
    )
    parser.add_argument("--run-id", default="186ae107d0793d2ee237")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark()
    try:
        run_recovery_suite(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
