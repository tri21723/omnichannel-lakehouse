"""Shared DAG defaults, Spark task factory, manifest sensor and failure callback (ADR-0013).

Import-time work is limited to constants and environment lookups: no I/O, no Spark,
no ``datetime.now()``. Logical dates come from ``ds`` templates.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import urllib.request
from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param

OWNER = "data-platform"
POOL = "spark_driver"
CHANNELS = ("walmart", "amazon", "tiktok_shop", "shopee", "lazada")
RUN_ID_PATTERN = "^[0-9a-f]{20}$"
DEFAULT_RUN_ID = os.environ.get("OMNI_DATASET_RUN_ID", "b1e64afaff0176de180a")
START_DATE = datetime(2026, 1, 1, tzinfo=UTC)
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
JOBS_DIR = "/app/spark/jobs"
EVENTS_SCRIPT = "/app/docker/batch/ci_events.py"
# A manual `make` job may hold the driver lock; tasks wait this long before exit 75.
LOCK_WAIT_SECONDS = 300
RUN_ID = "{{ params.dataset_run_id }}"
DS = "{{ ds }}"

# Execution timeouts from measured durations (docs/phase5-handoff.md §3).
TIMEOUTS = {
    "bronze": timedelta(minutes=15),
    "silver": timedelta(minutes=15),
    "gold": timedelta(minutes=20),
    "dp3": timedelta(minutes=15),
    "warehouse": timedelta(minutes=45),
    "verify": timedelta(minutes=10),
    "sensor": timedelta(minutes=30),
}

log = logging.getLogger(__name__)


def on_failure(context) -> None:
    """Write one structured line; optionally POST it to AIRFLOW_ALERT_WEBHOOK_URL."""
    ti = context.get("task_instance")
    payload = {
        "event": "airflow_task_failed",
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
    }
    log.error(json.dumps(payload, sort_keys=True))
    url = os.environ.get("AIRFLOW_ALERT_WEBHOOK_URL")
    if url:
        try:
            request = urllib.request.Request(
                url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(request, timeout=5).close()
        except Exception as exc:  # alerting must never mask the original failure
            log.warning("alert webhook failed: %s", type(exc).__name__)


def default_args(retries: int = 1) -> dict:
    return {
        "owner": OWNER,
        "retries": retries,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": on_failure,
    }


def dataset_params() -> dict:
    return {
        "dataset_run_id": Param(
            DEFAULT_RUN_ID,
            type="string",
            pattern=RUN_ID_PATTERN,
            description="Generator dataset run ID (20 hex characters)",
        )
    }


def dag_kwargs(dag_id: str, doc: str, tags: list[str]) -> dict:
    """Common arguments for per-job DAGs: manual/controller-triggered, one run at a time."""
    return {
        "dag_id": dag_id,
        "doc_md": doc,
        "schedule": None,
        "start_date": START_DATE,
        "catchup": False,
        "max_active_runs": 1,
        "max_active_tasks": 1,
        "is_paused_upon_creation": False,
        "default_args": default_args(),
        "params": dataset_params(),
        "tags": ["omnichannel", "batch", *tags],
    }


def spark_command(script: str, args: list[str]) -> str:
    """Bash for one locked spark-submit; the last stdout line is the last allowlisted event.

    ``args`` may contain Jinja templates (``{{ ds }}``, params); fixed values are quoted.
    """
    rendered = " ".join(a if "{{" in a else shlex.quote(a) for a in args)
    return (
        "set -uo pipefail\n"
        'log="$(mktemp)"\n'
        "trap 'rm -f \"$log\"' EXIT\n"
        f"python -m spark.lib.driver_lock --wait {LOCK_WAIT_SECONDS} -- "
        f"{SPARK_SUBMIT} {JOBS_DIR}/{script} {rendered}"
        ' 2>&1 | tee "$log"\n'
        "rc=${PIPESTATUS[0]}\n"
        f'python {EVENTS_SCRIPT} "$log" | tail -n 1\n'
        'exit "$rc"\n'
    )


def spark_task(task_id: str, script: str, args: list[str], timeout: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=spark_command(script, args),
        pool=POOL,
        execution_timeout=TIMEOUTS[timeout],
        do_xcom_push=True,
    )


def manifest_exists(run_id: str) -> bool:
    """True when s3://<raw>/manifests/<run_id>/stream_manifest.json exists (published last)."""
    import re

    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    if not re.fullmatch(RUN_ID_PATTERN.strip("^$"), run_id):
        raise ValueError("invalid dataset_run_id")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["SPARK_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["SPARK_S3_SECRET_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 2}),
    )
    try:
        client.head_object(
            Bucket=os.environ.get("S3_RAW_BUCKET", "raw"),
            Key=f"manifests/{run_id}/stream_manifest.json",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True
