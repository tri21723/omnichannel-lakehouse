"""dp1_cdc: Debezium CDC of walmart.orders into Bronze and current-state Silver (ADR-0017).

Every 10 minutes: `dp1_cdc/main.py` reads new Kafka records (`availableNow`, MERGE on Kafka
coordinates), `dp1_cdc/silver.py` applies the latest event per order when it is newer, then
verifies Silver against Bronze. Needs the `cdc` profile (Kafka + Kafka Connect) and Spark; paused
at creation. Not tied to a batch dataset; the Spark driver lock and the `spark_driver` pool
serialize it with batch jobs.
"""

from datetime import timedelta

from omnichannel.common import DS, START_DATE, default_args, spark_task

from airflow.sdk import DAG, chain

with DAG(
    dag_id="dp1_cdc",
    doc_md=__doc__,
    schedule=timedelta(minutes=10),
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["omnichannel", "cdc", "bronze", "silver"],
):
    base = ["--run-date", DS, "--channel", "walmart"]
    bronze = spark_task("cdc_bronze", "dp1_cdc/main.py", [*base, "--mode", "incremental"], "bronze")
    silver = spark_task(
        "cdc_silver", "dp1_cdc/silver.py", [*base, "--mode", "incremental"], "silver"
    )
    verify = spark_task(
        "cdc_silver_verify", "dp1_cdc/silver.py", [*base, "--mode", "verify"], "verify"
    )
    chain(bronze, silver, verify)
