"""Emit dataset lineage from the pipeline ledgers to DataHub (ADR-0018 §4).

Reads the read-only `/runtime` volume (Bronze completion records, Silver/Gold/DP3 publication and
warehouse export ledgers), builds the edges with `lineage.model` and upserts one `upstreamLineage`
aspect per downstream dataset. Aspects are deterministic (sorted upstreams, fixed audit time), so a
rerun with unchanged ledgers writes identical aspects. `--dry-run` prints the edges only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from lineage import model

AUDIT_ACTOR = "urn:li:corpuser:datahub"
AUDIT_TIME = 0  # deterministic; the ledgers, not DataHub, carry run timestamps


def load(runtime: Path) -> model.Lineage:
    def read(path: str) -> dict:
        return json.loads((runtime / path).read_text())

    records = [json.loads(p.read_text()) for p in sorted((runtime / "bronze").glob("*.json"))]
    return model.build(
        records,
        read("silver/publication/ledger.json"),
        read("gold/publication/ledger.json"),
        read("dp3/publication/ledger.json"),
        read("warehouse/export/ledger.json"),
        cdc_ran=(runtime / "checkpoints/dp1_cdc").is_dir(),
    )


def digest(edges: model.Lineage) -> str:
    canonical = json.dumps({k: sorted(v) for k, v in sorted(edges.items())}, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def emit(edges: model.Lineage, server: str) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DatasetLineageTypeClass,
        DatasetPropertiesClass,
        StatusClass,
        SubTypesClass,
        UpstreamClass,
        UpstreamLineageClass,
    )

    emitter = DatahubRestEmitter(server)
    stamp = AuditStampClass(time=AUDIT_TIME, actor=AUDIT_ACTOR)
    proposals = []
    for urn in sorted(model.external_datasets(edges)):
        prefix = urn.split(",")[1]
        proposals += [
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(
                    name=prefix,
                    qualifiedName=f"s3://{prefix}",
                    description="MinIO prefix written outside the lakehouse catalog (ADR-0018 §4).",
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=SubTypesClass(typeNames=["Prefix"])
            ),
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=StatusClass(removed=False)),
        ]
    for downstream, upstreams in sorted(edges.items()):
        if not upstreams:
            continue  # static tables (model.STATIC_GOLD) have no upstream to declare
        lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED, auditStamp=stamp)
                for u in sorted(upstreams)
            ]
        )
        proposals.append(MetadataChangeProposalWrapper(entityUrn=downstream, aspect=lineage))
    for proposal in proposals:
        emitter.emit_mcp(proposal)
    emitter.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, default=Path("/runtime"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    edges = load(args.runtime)
    summary = {
        "event": "lineage_built",
        "downstreams": len(edges),
        "edges": sum(len(v) for v in edges.values()),
        "lineage_sha256": digest(edges),
    }
    if args.dry_run:
        summary["lineage"] = {k: sorted(v) for k, v in sorted(edges.items())}
    else:
        emit(edges, os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080"))
        summary["event"] = "lineage_emitted"
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
