"""Emit Airflow DataFlow/DataJob entities with dataset inputs/outputs (ADR-0018 §4).

Input: the JSON written by `airflow/plugins/omnichannel/datahub_export.py` and the ledgers in
`/runtime` (for the dataset edges). Every aspect is deterministic, so a rerun with unchanged DAGs
and ledgers writes identical aspects. `--dry-run` prints the job records only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from lineage import jobs
from lineage.emit_lineage import load


def emit(records: list[dict], server: str) -> None:
    from tools import aspects

    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        DataFlowInfoClass,
        DataJobInfoClass,
        DataJobInputOutputClass,
        StatusClass,
    )

    def owners(name: str):
        return aspects.ownership(f"urn:li:corpuser:{name}")

    emitter = DatahubRestEmitter(server)
    proposals = []
    for flow in records:
        info = DataFlowInfoClass(
            name=flow["dag_id"],
            description=flow["description"],
            customProperties={"schedule": flow["schedule"], "tags": ",".join(flow["tags"])},
        )
        proposals += [
            MetadataChangeProposalWrapper(entityUrn=flow["urn"], aspect=info),
            MetadataChangeProposalWrapper(entityUrn=flow["urn"], aspect=owners(flow["owner"])),
            MetadataChangeProposalWrapper(entityUrn=flow["urn"], aspect=StatusClass(removed=False)),
        ]
        for task in flow["tasks"]:
            properties = {"operator": task["operator"]}
            # Task order is not data flow: DataHub draws inputDatajobs as lineage (e.g. the Amazon
            # prefix would appear upstream of TikTok Bronze), so the order is a property only.
            if task["upstream_jobs"]:
                properties["upstream_tasks"] = ",".join(
                    urn.rsplit(",", 1)[1].rstrip(")") for urn in task["upstream_jobs"]
                )
            if task["script"]:
                properties["script"] = task["script"]
            proposals += [
                MetadataChangeProposalWrapper(
                    entityUrn=task["urn"],
                    aspect=DataJobInfoClass(
                        name=task["task_id"],
                        type="COMMAND",
                        flowUrn=flow["urn"],
                        customProperties=properties,
                    ),
                ),
                MetadataChangeProposalWrapper(
                    entityUrn=task["urn"],
                    aspect=DataJobInputOutputClass(
                        inputDatasets=task["inputs"],
                        outputDatasets=task["outputs"],
                        inputDatajobs=[],
                    ),
                ),
                MetadataChangeProposalWrapper(entityUrn=task["urn"], aspect=owners(flow["owner"])),
                MetadataChangeProposalWrapper(
                    entityUrn=task["urn"], aspect=StatusClass(removed=False)
                ),
            ]
    for proposal in proposals:
        emitter.emit_mcp(proposal)
    emitter.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dags", type=Path, required=True, help="datahub_export.py JSON")
    parser.add_argument("--runtime", type=Path, default=Path("/runtime"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    records = jobs.flows(json.loads(args.dags.read_text()), load(args.runtime))
    canonical = json.dumps(records, sort_keys=True).encode()
    summary = {
        "event": "jobs_built",
        "flows": len(records),
        "jobs": sum(len(f["tasks"]) for f in records),
        "jobs_with_datasets": sum(
            1 for f in records for t in f["tasks"] if t["inputs"] or t["outputs"]
        ),
        "jobs_sha256": hashlib.sha256(canonical).hexdigest()[:16],
    }
    if args.dry_run:
        summary["flows_detail"] = records
    else:
        emit(records, os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080"))
        summary["event"] = "jobs_emitted"
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
