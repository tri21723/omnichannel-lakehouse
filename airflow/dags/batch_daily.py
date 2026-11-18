"""batch_daily: run the whole batch chain for one logical date (ADR-0013 §4).

Each step triggers the per-job DAG for the same logical date and dataset run and waits for
it in deferrable mode, so the single executor slot is free while waiting.
"""

from datetime import UTC, datetime

from omnichannel.common import RUN_ID, START_DATE, dataset_params, default_args

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, chain

CHAIN = (
    "dp1_bronze",
    "dp2_silver",
    "dp2_gold",
    "dp3_features",
    "dp3_return_labels",
    "dp4_warehouse",
)

with DAG(
    dag_id="batch_daily",
    doc_md=__doc__,
    schedule="@daily",
    start_date=START_DATE,
    # Demo data covers 2026-01-01..03; use `airflow backfill create` for the range.
    end_date=datetime(2026, 1, 3, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # depends_on_past: a failed date blocks later dates instead of running them on
    # incomplete upstream data (Gold/DP3 are cumulative over the dataset run).
    default_args={**default_args(retries=0), "depends_on_past": True},
    params=dataset_params(),
    tags=["omnichannel", "batch", "controller"],
):
    chain(
        *[
            TriggerDagRunOperator(
                task_id=f"run_{dag_id}",
                trigger_dag_id=dag_id,
                trigger_run_id=f"batch_daily__{dag_id}__" + "{{ ds }}__" + RUN_ID,
                logical_date="{{ logical_date }}",
                conf={"dataset_run_id": RUN_ID},
                reset_dag_run=True,
                wait_for_completion=True,
                deferrable=True,
                poke_interval=15,
                allowed_states=["success"],
                failed_states=["failed"],
                fail_when_dag_is_paused=True,
            )
            for dag_id in CHAIN
        ]
    )
