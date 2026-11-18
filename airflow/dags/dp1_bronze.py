"""dp1_bronze: wait for the source manifest, ingest one logical date into Bronze."""

from omnichannel.common import (
    CHANNELS,
    DS,
    RUN_ID,
    TIMEOUTS,
    dag_kwargs,
    manifest_exists,
    spark_task,
)

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, chain

with DAG(
    **dag_kwargs(
        "dp1_bronze",
        "JSONL for five channels plus Walmart JDBC for `ds`; `incremental` skips completed "
        "identical slices (ADR-0007, ADR-0013).",
        ["bronze"],
    )
):
    wait = PythonSensor(
        task_id="wait_for_manifest",
        python_callable=manifest_exists,
        op_kwargs={"run_id": RUN_ID},
        mode="reschedule",
        poke_interval=30,
        timeout=TIMEOUTS["sensor"].total_seconds(),
    )
    base = ["--run-id", RUN_ID, "--run-date", DS, "--mode", "incremental"]
    jsonl = [
        spark_task(f"bronze_{c}_jsonl", "dp1_bronze/main.py", [*base, "--channel", c], "bronze")
        for c in CHANNELS
    ]
    jdbc = spark_task(
        "bronze_walmart_jdbc",
        "dp1_bronze/main.py",
        [*base, "--channel", "walmart", "--source", "jdbc"],
        "bronze",
        outlet="bronze",
    )
    chain(wait, *jsonl, jdbc)
