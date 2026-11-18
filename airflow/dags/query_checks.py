"""query_checks: read-only Trino reconciliation whenever the warehouse asset updates (ADR-0015).

Data-aware: runs after a `dp4_warehouse` verify emits `sources.warehouse`. Needs the `query`
profile (Trino) to be up; it never writes and does not use the `spark_driver` pool.
Paused at creation so environments without Trino are not affected.
"""

from datetime import timedelta

from omnichannel.common import ASSETS, START_DATE, default_args
from omnichannel.query_checks import reconcile_warehouse_window

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG(
    dag_id="query_checks",
    doc_md=__doc__,
    schedule=[ASSETS["warehouse"]],
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["omnichannel", "query"],
):
    PythonOperator(
        task_id="reconcile_warehouse_window",
        python_callable=reconcile_warehouse_window,
        execution_timeout=timedelta(minutes=10),
    )
