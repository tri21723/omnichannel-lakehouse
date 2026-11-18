"""dp2_silver: publish Silver for each channel of one logical date."""

from omnichannel.common import CHANNELS, DS, RUN_ID, dag_kwargs, spark_task

from airflow.sdk import DAG, chain

with DAG(
    **dag_kwargs(
        "dp2_silver",
        "Five channels for `ds`; an identical pinned input publishes nothing new (ADR-0008).",
        ["silver"],
    )
):
    chain(
        *[
            spark_task(
                f"silver_{c}",
                "dp2_silver/main.py",
                [
                    "--run-id",
                    RUN_ID,
                    "--run-date",
                    DS,
                    "--channel",
                    c,
                    "--mode",
                    "incremental",
                    "--fail-on-reject",
                ],
                "silver",
            )
            for c in CHANNELS
        ]
    )
