"""dp3_return_labels: return labels as of `ds` with a 14-day horizon, then verify."""

from omnichannel.common import DS, RUN_ID, dag_kwargs, spark_task

from airflow.sdk import DAG

with DAG(
    **dag_kwargs(
        "dp3_return_labels",
        "Labels with maturity censoring as of `ds` (ADR-0010).",
        ["dp3", "labels"],
    )
):
    publish = spark_task(
        "labels_publish",
        "dp3_return_labels/main.py",
        ["--run-id", RUN_ID, "--as-of-date", DS, "--horizon-days", "14", "--mode", "incremental"],
        "dp3",
    )
    verify = spark_task(
        "labels_verify", "dp3_return_labels/main.py", ["--run-id", RUN_ID, "--verify"], "verify"
    )
    publish >> verify
