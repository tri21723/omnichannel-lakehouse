"""dp2_gold: rebuild the Gold star schema for the dataset run, then verify."""

from omnichannel.common import RUN_ID, dag_kwargs, spark_task

from airflow.sdk import DAG

with DAG(
    **dag_kwargs(
        "dp2_gold",
        "Skips when the Silver publication and Gold policy are unchanged (ADR-0009).",
        ["gold"],
    )
):
    publish = spark_task(
        "gold_publish", "dp2_silver/gold_main.py", ["--run-id", RUN_ID, "--mode", "full"], "gold"
    )
    verify = spark_task(
        "gold_verify",
        "dp2_silver/gold_main.py",
        ["--run-id", RUN_ID, "--verify"],
        "verify",
        outlet="gold",
    )
    publish >> verify
