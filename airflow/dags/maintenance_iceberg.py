"""maintenance_iceberg: weekly dry-run snapshot retention report (ADR-0014).

Lists expiry/compaction candidates and excludes every snapshot pinned by ledgers, archives,
completion records and benchmark manifests. It never expires snapshots or deletes files.
"""

from datetime import timedelta

from omnichannel.common import START_DATE, default_args, spark_task

from airflow.sdk import DAG

with DAG(
    dag_id="maintenance_iceberg",
    doc_md=__doc__,
    schedule="@weekly",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["omnichannel", "maintenance"],
):
    report = spark_task(
        "iceberg_report",
        "maintenance/iceberg_report.py",
        [
            "--as-of",
            "{{ data_interval_end.isoformat() }}",
            "--retention-days",
            "7",
            "--keep-last",
            "5",
        ],
        "dp3",
    )
    report.execution_timeout = timedelta(minutes=20)
