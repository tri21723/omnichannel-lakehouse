"""Airflow DataFlow/DataJob inputs and outputs (ADR-0018 §4), derived from the dataset lineage.

The DAG description comes from `airflow/plugins/omnichannel/datahub_export.py` (DagBag → JSON);
each Spark task is identified by its job script and arguments, and its datasets are read from the
same edges `lineage.model` builds, so jobs and datasets never disagree. Pure functions only.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from lineage import model

CLUSTER = "prod"


def flow_urn(dag_id: str) -> str:
    return f"urn:li:dataFlow:(airflow,{dag_id},{CLUSTER})"


def job_urn(dag_id: str, task_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn(dag_id)},{task_id})"


def layer(urn: str) -> str:
    """`bronze`/`silver`/`gold` for Iceberg URNs, `<platform>` otherwise."""
    platform = urn.split("urn:li:dataPlatform:", 1)[1].split(",", 1)[0]
    if platform != "iceberg":
        return platform
    return urn.split(",")[1].split(".", 1)[0]


def downstream(edges: model.Lineage, sources: Iterable[str], wanted: str) -> set[str]:
    sources = set(sources)
    return {d for d, ups in edges.items() if ups & sources and layer(d) == wanted}


def _iceberg_all(names: Iterable[str]) -> set[str]:
    return {model.iceberg(f"lh.gold.{name}") for name in names}


def spark_io(
    script: str, args: Mapping[str, str], edges: model.Lineage
) -> tuple[set[str], set[str]]:
    """Datasets a Spark task reads and writes; verify tasks (`--mode verify`/`--verify`) read."""
    channel = args.get("--channel", "")
    inputs: set[str] = set()
    outputs: set[str] = set()
    if script == "dp1_bronze/main.py":
        source = args.get("--source", "jsonl")
        if source == "jdbc":
            inputs = {model.postgres(f"sources.walmart.{e}") for e in model.ENTITIES}
        else:
            prefix = "raw/stream" if source == "stream" else "raw"
            inputs = {model.s3(f"{prefix}/{channel}")}
        outputs = downstream(edges, inputs, "bronze")
    elif script == "dp2_silver/main.py":
        selection = {"source": "jdbc" if channel == "walmart" else "jsonl", "channel": channel}
        inputs = {model.iceberg(t) for t in model.silver_inputs(selection)}
        first = downstream(edges, inputs, "silver")
        outputs = first | downstream(edges, first, "silver")
    elif script == "dp2_silver/gold_main.py":
        outputs = _iceberg_all(model.GOLD_UPSTREAMS)
        inputs = {
            model.iceberg(f"lh.silver.{e}") for ups in model.GOLD_UPSTREAMS.values() for e in ups
        }
    elif script == "dp3_features/main.py":
        names = ("feat_customer_daily", "feat_product_daily")
        outputs = _iceberg_all(names)
        inputs = _iceberg_all(t for name in names for t in model.DP3_UPSTREAMS[name])
    elif script == "dp3_return_labels/main.py":
        outputs = _iceberg_all(["lbl_order_return"])
        inputs = _iceberg_all(model.DP3_UPSTREAMS["lbl_order_return"])
    elif script == "dp4_warehouse/main.py":
        inputs = _iceberg_all(model.GOLD_UPSTREAMS)
        outputs = {model.postgres(f"sources.warehouse.{name}") for name in model.GOLD_UPSTREAMS}
    elif script == "dp1_cdc/main.py":
        inputs, outputs = {model.kafka(model.CDC_TOPIC)}, {model.iceberg(model.CDC_BRONZE)}
    elif script == "dp1_cdc/silver.py":
        inputs = {model.iceberg(model.CDC_BRONZE)}
        outputs = {model.iceberg(model.CDC_SILVER), model.iceberg(model.CDC_QUARANTINE)}
    if args.get("--mode") == "verify" or args.get("--verify") == "true":
        return inputs | outputs, set()
    return inputs, outputs


def flows(dags: Iterable[Mapping[str, Any]], edges: model.Lineage) -> list[dict[str, Any]]:
    """One record per DAG with its tasks' URNs, upstream tasks and dataset inputs/outputs."""
    result = []
    for dag in sorted(dags, key=lambda d: d["dag_id"]):
        tasks = []
        for task in sorted(dag["tasks"], key=lambda t: t["task_id"]):
            spark = task.get("spark")
            inputs, outputs = spark_io(spark["script"], spark["args"], edges) if spark else ((), ())
            tasks.append(
                {
                    "urn": job_urn(dag["dag_id"], task["task_id"]),
                    "task_id": task["task_id"],
                    "operator": task["operator"],
                    "script": spark["script"] if spark else None,
                    "inputs": sorted(inputs),
                    "outputs": sorted(outputs),
                    "upstream_jobs": sorted(
                        job_urn(dag["dag_id"], t) for t in task.get("upstream_task_ids", [])
                    ),
                }
            )
        result.append(
            {
                **{k: v for k, v in dag.items() if k != "tasks"},
                "urn": flow_urn(dag["dag_id"]),
                "tasks": tasks,
            }
        )
    return result
