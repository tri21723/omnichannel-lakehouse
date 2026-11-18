"""dp4_warehouse: export the 30-day window ending at `ds` to PostgreSQL, then verify."""

from omnichannel.common import DS, RUN_ID, dag_kwargs, spark_task

from airflow.sdk import DAG

with DAG(
    **dag_kwargs(
        "dp4_warehouse",
        "One JDBC transaction per export; identical or older (`--no-regress`) dates skip "
        "(ADR-0011, ADR-0013).",
        ["warehouse"],
    )
):
    export = spark_task(
        "warehouse_export",
        "dp4_warehouse/main.py",
        ["--run-id", RUN_ID, "--run-date", DS, "--mode", "full", "--no-regress"],
        "warehouse",
    )
    verify = spark_task(
        "warehouse_verify",
        "dp4_warehouse/main.py",
        ["--verify", "--run-id", RUN_ID],
        "verify",
    )
    export >> verify
