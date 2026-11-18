"""dp3_features: daily customer/product features as of `ds`, then verify."""

from omnichannel.common import DS, RUN_ID, dag_kwargs, spark_task

from airflow.sdk import DAG

with DAG(
    **dag_kwargs(
        "dp3_features",
        "Point-in-time features as of `ds`; per table/date provenance skips (ADR-0010).",
        ["dp3", "features"],
    )
):
    publish = spark_task(
        "features_publish",
        "dp3_features/main.py",
        ["--run-id", RUN_ID, "--as-of-date", DS, "--mode", "incremental"],
        "dp3",
    )
    verify = spark_task(
        "features_verify", "dp3_features/main.py", ["--run-id", RUN_ID, "--verify"], "verify"
    )
    publish >> verify
