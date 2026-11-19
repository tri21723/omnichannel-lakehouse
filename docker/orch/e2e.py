"""Backfill `batch_daily` through the Airflow REST API and check the outcome (ADR-0013 §6).

Runs inside the Airflow container (`make e2e` pipes it to `python -`). Prints one JSON line
per step and a final summary; exits non-zero on failure, timeout or a changed ledger when
`--expect-unchanged` is given (except ledgers named by `--allow-changed`). Never prints
credentials.

The first replay after a fresh backfill legitimately refreshes DP3 for earlier dates: DP3 pins
the latest Gold publication, which moved on after later dates were published. `make e2e`
therefore runs ingest → converging replay (only the DP3 ledger may change) → replay that must
change nothing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API = os.environ.get("AIRFLOW_E2E_API", "http://localhost:8080")
CHAIN = (
    "dp1_bronze",
    "dp2_silver",
    "dp2_gold",
    "dp3_features",
    "dp3_return_labels",
    "dp4_warehouse",
)
PUBLISH_TASKS = {
    "dp2_gold": "gold_publish",
    "dp3_features": "features_publish",
    "dp3_return_labels": "labels_publish",
    "dp4_warehouse": "warehouse_export",
}
LEDGERS = (
    "silver/publication/ledger.json",
    "gold/publication/ledger.json",
    "dp3/publication/ledger.json",
    "warehouse/export/ledger.json",
)


class Client:
    def __init__(self) -> None:
        self.token = None
        self.token = self.call(
            "POST",
            "/auth/token",
            {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]},
        )["access_token"]

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(
            API + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> HTTP {exc.code}") from None


def emit(event: str, **fields) -> None:
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def ledger_hashes(root: Path = Path("/runtime")) -> dict:
    return {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in LEDGERS
        if (root / name).exists()
    }


def wait_backfill(client: Client, backfill_id: int, timeout: int) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        backfill = client.call("GET", f"/api/v2/backfills/{backfill_id}")
        if backfill.get("completed_at"):
            return backfill
        time.sleep(20)
    raise TimeoutError(f"backfill {backfill_id} not completed after {timeout}s")


def runs_between(client: Client, dag_id: str, start: str, end: str) -> list[dict]:
    query = urllib.parse.urlencode(
        {
            "logical_date_gte": f"{start}T00:00:00Z",
            "logical_date_lte": f"{end}T00:00:00Z",
            "order_by": "logical_date",
            "limit": 100,
        }
    )
    return client.call("GET", f"/api/v2/dags/{dag_id}/dagRuns?{query}")["dag_runs"]


def xcom(client: Client, dag_id: str, run_id: str, task_id: str):
    path = f"/api/v2/dags/{dag_id}/dagRuns/{urllib.parse.quote(run_id)}/taskInstances/{task_id}"
    try:
        value = client.call("GET", path + "/xcomEntries/return_value").get("value")
    except RuntimeError:
        return None
    try:
        return json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError:
        return None


def unexpected_changes(
    changed: list[str], expect_unchanged: bool, allowed: list[str] | None = None
) -> list[str]:
    """Ledgers that changed although the pass expected them unchanged."""
    if not expect_unchanged:
        return []
    return sorted(set(changed) - set(allowed or []))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--from-date", default="2026-01-01")
    parser.add_argument("--to-date", default="2026-01-03")
    parser.add_argument("--reprocess", choices=("none", "completed", "failed"), default="failed")
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--expect-unchanged", action="store_true")
    parser.add_argument(
        "--allow-changed",
        action="append",
        choices=LEDGERS,
        default=[],
        help="ledger that may change despite --expect-unchanged (repeatable)",
    )
    args = parser.parse_args(argv)
    if len(args.run_id) != 20 or any(c not in "0123456789abcdef" for c in args.run_id):
        parser.error("--run-id must be 20 lowercase hex characters")

    client = Client()
    # Backfill runs stay queued on a paused DAG. Unpausing is safe: end_date (2026-01-03)
    # is in the past and catchup=False, so the scheduler creates no automatic run.
    client.call("PATCH", "/api/v2/dags/batch_daily", {"is_paused": False})
    before = ledger_hashes()
    started = time.monotonic()
    backfill = client.call(
        "POST",
        "/api/v2/backfills",
        {
            "dag_id": "batch_daily",
            "from_date": f"{args.from_date}T00:00:00Z",
            "to_date": f"{args.to_date}T00:00:00Z",
            "dag_run_conf": {"dataset_run_id": args.run_id},
            "reprocess_behavior": args.reprocess,
            "max_active_runs": 1,
        },
    )
    emit("e2e_backfill_created", backfill_id=backfill["id"], reprocess=args.reprocess)
    wait_backfill(client, backfill["id"], args.timeout)
    duration = round(time.monotonic() - started, 1)

    problems = []
    created = backfill["created_at"]

    def in_backfill(run: dict) -> bool:
        return (run.get("queued_at") or "") >= created

    controller = [
        r
        for r in runs_between(client, "batch_daily", args.from_date, args.to_date)
        if r.get("run_type") == "backfill"
        and (r.get("conf") or {}).get("dataset_run_id") == args.run_id
        and in_backfill(r)
    ]
    expected_days = (
        int(args.to_date[-2:]) - int(args.from_date[-2:]) + 1
        if args.from_date[:8] == args.to_date[:8]
        else None
    )
    if expected_days is not None and len(controller) != expected_days:
        problems.append(f"expected {expected_days} controller runs, got {len(controller)}")
    for run in controller:
        if run["state"] != "success":
            problems.append(f"batch_daily {run['dag_run_id']} is {run['state']}")
    publishes = {}
    for dag_id in CHAIN:
        for run in runs_between(client, dag_id, args.from_date, args.to_date):
            if not run["dag_run_id"].startswith(f"batch_daily__{dag_id}__"):
                continue
            if not run["dag_run_id"].endswith(args.run_id) or not in_backfill(run):
                continue
            if run["state"] != "success":
                problems.append(f"{dag_id} {run['dag_run_id']} is {run['state']}")
            task = PUBLISH_TASKS.get(dag_id)
            if task:
                value = xcom(client, dag_id, run["dag_run_id"], task) or {}
                publishes[f"{dag_id}@{run['logical_date'][:10]}"] = {
                    "event": value.get("event"),
                    "skipped": value.get("skipped"),
                }
    after = ledger_hashes()
    changed = sorted(name for name in after if before.get(name) != after[name])
    unexpected = unexpected_changes(changed, args.expect_unchanged, args.allow_changed)
    if unexpected:
        problems.append(f"ledgers changed on replay: {unexpected}")
    emit(
        "e2e_summary",
        backfill_id=backfill["id"],
        controller_runs=len(controller),
        duration_seconds=duration,
        ledgers_changed=changed,
        ledger_hashes=after,
        publishes=publishes,
        problems=problems,
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
