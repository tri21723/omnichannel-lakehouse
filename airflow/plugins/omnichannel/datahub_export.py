"""Describe the project's DAGs as JSON for DataHub job lineage (ADR-0018 §3–4).

Runs with Airflow's interpreter (it needs the DagBag); the DataHub CLI lives in a separate
virtualenv, which reads this JSON. Only static DAG structure is exported: ids, owner, tags,
schedule, tasks, task dependencies and, for Spark tasks, the job script and its arguments.
"""

from __future__ import annotations

import json
import shlex
from typing import Any

from omnichannel.common import JOBS_DIR

FLAGS_WITH_VALUES = ("--channel", "--source", "--mode")
BOOLEAN_FLAGS = ("--verify",)  # read-only verification (exported as "true")


def spark_job(bash_command: str) -> dict[str, Any] | None:
    """`dp2_silver/main.py` and selected flags from a `spark_command` script, else None."""
    marker = f"{JOBS_DIR}/"
    for line in bash_command.splitlines():
        if marker not in line:
            continue
        words = shlex.split(line.split(marker, 1)[1].split(" 2>&1", 1)[0])
        script, rest = words[0], words[1:]
        args = {flag: rest[i + 1] for i, flag in enumerate(rest[:-1]) if flag in FLAGS_WITH_VALUES}
        args.update({flag: "true" for flag in rest if flag in BOOLEAN_FLAGS})
        return {"script": script, "args": args}
    return None


def describe(dagbag: Any) -> list[dict[str, Any]]:
    dags = []
    for dag_id in sorted(dagbag.dags):
        dag = dagbag.dags[dag_id]
        tasks = []
        for task in sorted(dag.tasks, key=lambda t: t.task_id):
            command = getattr(task, "bash_command", None)
            tasks.append(
                {
                    "task_id": task.task_id,
                    "operator": type(task).__name__,
                    "upstream_task_ids": sorted(task.upstream_task_ids),
                    "spark": spark_job(command) if isinstance(command, str) else None,
                }
            )
        doc = (dag.doc_md or dag.description or "").strip()
        dags.append(
            {
                "dag_id": dag_id,
                "description": doc.splitlines()[0] if doc else "",
                "owner": dag.owner,
                "tags": sorted(dag.tags),
                "schedule": type(dag.timetable).__name__,
                "tasks": tasks,
            }
        )
    return dags


def main() -> int:
    import argparse
    from pathlib import Path

    from airflow.models.dagbag import DagBag

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dag_folder")
    parser.add_argument("--out", type=Path, required=True, help="JSON file (stdout carries logs)")
    args = parser.parse_args()
    dagbag = DagBag(dag_folder=args.dag_folder)
    if dagbag.import_errors:
        raise SystemExit(f"DAG import errors: {sorted(dagbag.import_errors)}")
    args.out.write_text(json.dumps(describe(dagbag), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
