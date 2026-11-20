"""datahub_ingest: refresh DataHub metadata and lineage (ADR-0018 §3–4).

Runs the read-only recipes (PostgreSQL `sources`, Kafka topics, Iceberg via Lakekeeper), emits the
dataset lineage from the ledgers, then the DataFlow/DataJob lineage of every DAG. The DataHub CLI
lives in its own virtualenv (`/opt/datahub-venv`, from `datahub/requirements.lock`) because it is
incompatible with the Airflow constraints. Needs the `governance` profile; paused at creation; no
Spark, so it does not use the `spark_driver` pool.
"""

from datetime import timedelta

from omnichannel.common import START_DATE, default_args

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain

VENV = "/opt/datahub-venv/bin"
WORKDIR = "/app/datahub"
# The venv must not see Airflow's PYTHONPATH (/app holds a `datahub/` directory, not the SDK).
ENV = "cd /app/datahub && env -u PYTHONPATH DATAHUB_TELEMETRY_ENABLED=false"


def ingest(source: str) -> BashOperator:
    strict = "" if source == "kafka" else "--strict-warnings "  # JSON topics: one warning each
    return BashOperator(
        task_id=f"ingest_{source}",
        bash_command=f"{ENV} {VENV}/datahub ingest {strict}-c recipes/{source}.yml",
        execution_timeout=timedelta(minutes=10),
    )


with DAG(
    dag_id="datahub_ingest",
    doc_md=__doc__,
    schedule="@daily",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["omnichannel", "governance"],
):
    lineage = BashOperator(
        task_id="emit_dataset_lineage",
        bash_command=f"{ENV} {VENV}/python -m lineage.emit_lineage --runtime /runtime",
        execution_timeout=timedelta(minutes=10),
    )
    jobs = BashOperator(
        task_id="emit_job_lineage",
        bash_command=(
            "set -euo pipefail\n"
            'dags="$(mktemp)"\n'
            "trap 'rm -f \"$dags\"' EXIT\n"
            # A subprocess does not inherit Airflow's plugins path, and Airflow 3 tasks have no
            # metadata-DB connection; parsing DAG files needs neither, so use in-memory SQLite.
            "PYTHONPATH=/opt/airflow/plugins:/app AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:// "
            'python -m omnichannel.datahub_export /opt/airflow/dags --out "$dags"\n'
            f'{ENV} {VENV}/python -m lineage.emit_jobs --dags "$dags" --runtime /runtime\n'
        ),
        execution_timeout=timedelta(minutes=10),
    )
    chain(ingest("postgres"), ingest("kafka"), ingest("iceberg"), lineage, jobs)
