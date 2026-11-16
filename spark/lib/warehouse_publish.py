"""Export ledger, idempotent skip, and transactional rollback recovery for Phase 4E."""

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from spark.lib.bronze import save_state, writer_lock
from spark.lib.bronze_jdbc import rollback_close
from spark.lib.warehouse_jdbc import (
    DEFAULT_DDL_PATH,
    INSERT_ORDER,
    collect_slice_payload,
    count_run_rows,
    query_run_monetary_totals,
    replace_run_slice,
    writer_connection,
)

ROOT = Path("/runtime/warehouse/export")


def compute_export_id(ledger: dict[str, Any]) -> str:
    """Compute canonical SHA-256 content digest for an export ledger snapshot."""
    return hashlib.sha256(json.dumps(ledger, sort_keys=True).encode("utf-8")).hexdigest()


def load_warehouse_ledger(
    ledger_path: str | Path = ROOT / "ledger.json",
    export_id: str | None = None,
    spark=None,
) -> tuple[dict[str, Any], str]:
    """Load and validate a completed warehouse export ledger and its archive."""
    ledger_path = Path(ledger_path)
    if spark is not None:
        from spark.lib.warehouse_content import read_db_ledger

        conn = writer_connection(spark, read_only=True)
        try:
            authoritative = read_db_ledger(conn)
        finally:
            rollback_close(conn)
        if authoritative is not None:
            computed = compute_export_id(authoritative)
            if export_id and export_id != computed:
                raise ValueError("Requested export is not current PostgreSQL state")
            return authoritative, computed
    if not ledger_path.exists():
        raise FileNotFoundError(f"Warehouse export ledger not found: {ledger_path}")

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    if ledger.get("status") != "completed":
        raise ValueError(
            f"Warehouse export ledger is not completed (status={ledger.get('status')})"
        )

    computed_id = compute_export_id(ledger)
    if export_id and export_id != computed_id:
        raise ValueError(f"Requested export ID {export_id} does not match ledger {computed_id}")

    archive_path = ledger_path.parent / f"export-{computed_id}.json"
    if archive_path.exists():
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        if archive != ledger:
            raise ValueError("Corrupted warehouse export archive: content mismatch")

    return ledger, computed_id


def verify_warehouse_run(
    spark: Any,
    run_id: str,
    expected_counts: dict[str, int],
    expected_monetary: dict[str, dict[str, dict[str, str]]],
    expected_content=None,
) -> None:
    """Read back PostgreSQL warehouse.* counts, NUMERIC(18,2) totals, and constraints."""
    from spark.lib.warehouse_reconcile import check_postgres_constraints

    conn = writer_connection(spark, read_only=True)
    try:
        actual_counts = count_run_rows(conn, run_id)
        if actual_counts != expected_counts:
            raise ValueError(
                f"PostgreSQL row count verification failed for run {run_id}: "
                f"expected {expected_counts}, got {actual_counts}"
            )
        actual_monetary = query_run_monetary_totals(conn, run_id)
        if actual_monetary != expected_monetary:
            raise ValueError(
                f"PostgreSQL monetary verification failed for run {run_id}: "
                f"expected {expected_monetary}, got {actual_monetary}"
            )
        if expected_content is not None:
            from spark.lib.warehouse_content import query_content

            if query_content(conn, run_id) != expected_content:
                raise ValueError("PostgreSQL full-row content verification failed")
        check_postgres_constraints(conn)
    finally:
        rollback_close(conn)


def verify_warehouse_ledger(
    spark: Any,
    ledger: dict[str, Any],
    target_run_id: str | None = None,
) -> list[str]:
    """Verify PostgreSQL warehouse.* state matches committed ledger entries read-only."""
    if ledger.get("status") != "completed":
        raise ValueError(f"Warehouse export ledger not completed (status={ledger.get('status')})")

    runs = ledger.get("runs", {})
    if not runs:
        raise ValueError("Warehouse export ledger contains no committed runs")

    if target_run_id is not None:
        if target_run_id not in runs:
            raise ValueError(f"Run {target_run_id} is not present in warehouse export ledger")
        run_ids = [target_run_id]
    else:
        run_ids = sorted(runs.keys())

    from spark.lib.warehouse_content import read_db_ledger

    conn = writer_connection(spark, read_only=True)
    try:
        authoritative = read_db_ledger(conn)
    finally:
        rollback_close(conn)
    if authoritative is not None and authoritative != ledger:
        raise ValueError("Stale file ledger: use the committed PostgreSQL export state")

    expected_tables = {f"warehouse.{name}" for name in INSERT_ORDER}
    for run_id in run_ids:
        meta = runs[run_id]
        run_tables = meta.get("tables", {})
        missing = expected_tables - set(run_tables.keys())
        if missing:
            raise ValueError(f"Incomplete table inventory for run {run_id}: {missing}")
        expected_counts = {
            name: int(run_tables[f"warehouse.{name}"]["rows"]) for name in INSERT_ORDER
        }
        expected_monetary = meta.get("monetary", {})
        verify_warehouse_run(spark, run_id, expected_counts, expected_monetary, meta.get("content"))
        if "content" not in meta:
            from spark.lib.warehouse_reconcile import reconcile_run

            reconcile_run(spark, run_id, meta)

    return run_ids


def should_skip_export(
    ledger: dict[str, Any],
    run_id: str,
    gold_pub_id: str,
    run_date: str,
    *,
    force: bool = False,
    fail_after: str | None = None,
) -> bool:
    """Return True iff identical (run_id, gold_pub_id, run_date) already completed cleanly."""
    if force or fail_after is not None:
        return False
    if ledger.get("status") != "completed":
        return False
    prior_run = ledger.get("runs", {}).get(run_id)
    if not prior_run:
        return False
    return (
        prior_run.get("gold_publication_id") == gold_pub_id
        and prior_run.get("run_date") == run_date
    )


def publish_warehouse(
    spark: Any,
    sliced_frames: dict[str, Any],
    run_id: str,
    gold_pub_id: str,
    run_date: Any,
    window_start: Any,
    window_end: Any,
    *,
    root: str | Path = ROOT,
    fail_after: str | None = None,
    force: bool = False,
    mode: str = "full",
    batch_size: int = 1000,
    ddl_path: str | Path = DEFAULT_DDL_PATH,
) -> tuple[dict[str, Any], bool]:
    """Synchronize 8 warehouse.* tables atomically with export ledger & POSIX lock."""
    root = Path(root)
    run_date_str = run_date.isoformat() if hasattr(run_date, "isoformat") else str(run_date)
    w_start_str = (
        window_start.isoformat() if hasattr(window_start, "isoformat") else str(window_start)
    )
    w_end_str = window_end.isoformat() if hasattr(window_end, "isoformat") else str(window_end)

    with writer_lock(root):
        ledger_path = root / "ledger.json"
        old = (
            json.loads(ledger_path.read_text(encoding="utf-8"))
            if ledger_path.exists()
            else dict(status="pending", runs={})
        )

        from spark.lib.warehouse_content import (
            payload_content,
            query_content,
            read_db_ledger,
            write_db_ledger,
        )

        conn = writer_connection(spark, read_only=True)
        try:
            authoritative = read_db_ledger(conn)
        finally:
            rollback_close(conn)
        if authoritative is not None:
            old = authoritative

        if should_skip_export(
            old,
            run_id,
            gold_pub_id,
            run_date_str,
            force=force,
            fail_after=fail_after,
        ):
            verify_warehouse_ledger(spark, old, target_run_id=run_id)
            export_id = compute_export_id(old)
            save_state(root / f"export-{export_id}.json", old)
            save_state(ledger_path, old)
            return old, True

        pending = dict(
            status="running",
            run_id=run_id,
            gold_publication_id=gold_pub_id,
            run_date=run_date_str,
            mode=mode,
            fail_after=fail_after,
            updated_at=round(time.time(), 3),
        )
        save_state(root / "attempt.json", pending)
        started = time.monotonic()

        try:
            payload, monetary = collect_slice_payload(sliced_frames)
            content = payload_content(payload)
            row_counts = {name: len(payload[name]) for name in INSERT_ORDER}
            now = round(time.time(), 3)
            current = json.loads(json.dumps(old))
            current["status"] = "completed"
            current["updated_at"] = now
            current["duration_seconds"] = round(time.monotonic() - started, 3)
            runs = current.setdefault("runs", {})
            tables_meta = {f"warehouse.{name}": {"rows": row_counts[name]} for name in INSERT_ORDER}
            runs[run_id] = {
                "gold_publication_id": gold_pub_id,
                "run_date": run_date_str,
                "window_start": w_start_str,
                "window_end": w_end_str,
                "mode": mode,
                "tables": tables_meta,
                "monetary": monetary,
                "content": content,
                "updated_at": now,
            }

            def commit_evidence(conn, actual_counts):
                if query_content(conn, run_id) != content:
                    raise ValueError("In-transaction warehouse content mismatch")
                if query_run_monetary_totals(conn, run_id) != monetary:
                    raise ValueError("In-transaction monetary mismatch")
                current["duration_seconds"] = round(time.monotonic() - started, 3)
                write_db_ledger(conn, current)

            replace_run_slice(
                spark,
                payload,
                run_id,
                fail_after=fail_after if fail_after != "db_commit" else None,
                batch_size=batch_size,
                ddl_path=ddl_path,
                before_commit=commit_evidence,
            )
            if fail_after == "db_commit":
                raise RuntimeError("Injected failure after PostgreSQL commit")
            verify_warehouse_run(spark, run_id, row_counts, monetary, content)

            export_id = compute_export_id(current)
            save_state(root / f"export-{export_id}.json", current)
            save_state(ledger_path, current)
            save_state(
                root / "attempt.json",
                {**pending, "status": "completed", "export_id": export_id},
            )
            return current, False
        except Exception as exc:
            save_state(
                root / "attempt.json",
                {
                    **pending,
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                    "updated_at": round(time.time(), 3),
                },
            )
            raise
