"""Apply a CDC scenario to PostgreSQL exactly once and reconcile the source with its ground truth.

Order of effects (same pattern as the stream publisher): ground truth (immutable S3 object) →
transactions on `walmart` as role `generator` → `cdc_applied.json` marker. A rerun with the marker
sends nothing; ground truth without the marker, or scenario rows that already exist, means an
interrupted run and is refused (use a new seed).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path

from generators.batch.dataset import fingerprint
from generators.cdc.scenario import CdcConfig, Step, build, checksum
from generators.common.storage import put_immutable, remote_fingerprint
from generators.stream.publish import write_json

LOCK_WAIT_S = 1.0


def manifest_keys(run_id: str) -> tuple[str, str]:
    return (
        f"manifests/{run_id}/cdc_ground_truth.json",
        f"manifests/{run_id}/cdc_applied.json",
    )


def run_step(step: Step, connect: Callable[[], object]) -> None:
    if step.concurrent is None:
        with connect() as conn:
            for sql, params in step.ops:
                conn.execute(sql, params)
            conn.commit()
        return
    errors: list[BaseException] = []
    with connect() as first, connect() as second:
        for sql, params in step.ops:
            first.execute(sql, params)  # holds the row lock until commit

        def blocked() -> None:
            try:
                for sql, params in step.concurrent:
                    second.execute(sql, params)  # waits for the first session's lock
                second.commit()
            except BaseException as exc:  # surfaced after join
                errors.append(exc)

        thread = threading.Thread(target=blocked)
        thread.start()
        time.sleep(LOCK_WAIT_S)
        if not thread.is_alive():
            raise RuntimeError("second session did not wait for the row lock")
        first.commit()
        thread.join(timeout=30)
        if thread.is_alive() or errors:
            raise RuntimeError(f"concurrent step failed: {errors[:1]}")


def source_rows(connect: Callable[[], object], prefix: str) -> list[dict]:
    with connect() as conn:
        cursor = conn.execute(
            "SELECT order_id, customer_id, order_ts, updated_ts, status, currency, "
            "total_amount::text, shipping_country FROM walmart.orders "
            "WHERE starts_with(order_id, %s) ORDER BY order_id",
            (prefix,),
        )
        names = [d.name for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def check_source(config: CdcConfig, connect: Callable[[], object]) -> dict:
    _, truth = build(config)
    rows = source_rows(connect, truth["prefix"])
    live = {r["order_id"] for r in rows}
    result = {
        "live_orders": len(rows),
        "checksum_equal": checksum(rows) == truth["final_checksum"],
        "deleted_absent": not live & set(truth["deleted_orders"]),
    }
    result["ok"] = (
        result["live_orders"] == truth["live_orders"]
        and result["checksum_equal"]
        and result["deleted_absent"]
    )
    return result


def apply_scenario(
    config: CdcConfig, output: Path, s3, bucket: str, connect: Callable[[], object]
) -> dict:
    steps, truth = build(config)
    run_id = config.cdc_run_id
    root = output / "cdc" / run_id
    truth_path = write_json(root / "cdc_ground_truth.json", truth)
    truth_key, marker_key = manifest_keys(run_id)
    if remote_fingerprint(s3, bucket, marker_key) is not None:
        return {"cdc_run_id": run_id, "skipped": True, "transactions": len(steps)}
    if remote_fingerprint(s3, bucket, truth_key) is not None or source_rows(
        connect, truth["prefix"]
    ):
        raise RuntimeError(
            "scenario rows or ground truth exist without an applied marker: an earlier run was "
            "interrupted; use a new seed instead of re-applying"
        )
    put_immutable(s3, bucket, truth_key, truth_path, fingerprint(truth_path))
    started = time.monotonic()
    for step in steps:
        run_step(step, connect)
    check = check_source(config, connect)
    if not check["ok"]:
        raise RuntimeError(f"source differs from ground truth after apply: {check}")
    marker = {
        "cdc_run_id": run_id,
        "transactions": len(steps),
        "expected_ops": truth["expected_ops"],
    }
    marker_path = write_json(root / "cdc_applied.json", marker)
    put_immutable(s3, bucket, marker_key, marker_path, fingerprint(marker_path))
    return {
        "cdc_run_id": run_id,
        "skipped": False,
        "transactions": len(steps),
        "seconds": round(time.monotonic() - started, 3),
    }
