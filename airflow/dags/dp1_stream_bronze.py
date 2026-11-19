"""dp1_stream_bronze: ingest one closed Flink stream run/event date into Bronze (ADR-0016 §8).

`dataset_run_id` is the stream run id and `ds` the event date. The sensor waits for
`stream_output_manifest.json`, written by `make stream-close` after the jobs were drained; each
channel is one Bronze slice (`--source stream`), replayed without duplicates.
"""

from omnichannel.common import DS, RUN_ID, TIMEOUTS, dag_kwargs, manifest_exists, spark_task

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, chain

with DAG(**dag_kwargs("dp1_stream_bronze", __doc__, ["bronze", "stream"])):
    wait = PythonSensor(
        task_id="wait_for_output_manifest",
        python_callable=manifest_exists,
        op_kwargs={"run_id": RUN_ID, "name": "stream_output_manifest.json"},
        mode="reschedule",
        poke_interval=30,
        timeout=TIMEOUTS["sensor"].total_seconds(),
    )
    base = ["--run-id", RUN_ID, "--run-date", DS, "--source", "stream", "--mode", "incremental"]
    amazon = spark_task(
        "bronze_amazon_stream", "dp1_bronze/main.py", [*base, "--channel", "amazon"], "bronze"
    )
    tiktok = spark_task(
        "bronze_tiktok_shop_stream",
        "dp1_bronze/main.py",
        [*base, "--channel", "tiktok_shop"],
        "bronze",
        outlet="bronze",
    )
    chain(wait, amazon, tiktok)
