"""Live negative test for G1 on a probe entity (ADR-0018 §5); real entities are never modified.

1. Emit probe `iceberg gold.g1_probe` with upstream `silver.orders` → G1 passes.
2. Replace its upstreamLineage with an empty one (edge removed) → G1 must fail on the probe.
3. Restore the edge → G1 passes again.
4. Hard-delete the probe, so the catalogue is back to its baseline.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from lineage import model
from lineage.emit_lineage import load
from tools.gms import all_urns

from assertions.lineage_g1 import evaluate, live_edges

PROBE = model.iceberg("lh.gold.g1_probe")
UPSTREAM = model.iceberg("lh.silver.orders")


def emit_upstreams(server: str, upstreams: list[str]) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata import schema_classes as s

    stamp = s.AuditStampClass(time=0, actor="urn:li:corpuser:datahub")
    lineage = s.UpstreamLineageClass(
        upstreams=[
            s.UpstreamClass(dataset=u, type=s.DatasetLineageTypeClass.TRANSFORMED, auditStamp=stamp)
            for u in upstreams
        ]
    )
    emitter = DatahubRestEmitter(server)
    emitter.emit_mcp(MetadataChangeProposalWrapper(entityUrn=PROBE, aspect=lineage))
    emitter.flush()


def g1(server: str, datasets: list[str], expected, optional: bool) -> tuple[bool, list[dict]]:
    checks = evaluate(datasets, live_edges(datasets, server), expected, optional)
    return all(not item["failed"] for item in checks), checks


def main() -> int:
    import argparse

    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--optional-chains", action="store_true", help="CI: no stream/CDC run")
    args = parser.parse_args()
    server = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080")
    expected = load(Path("/runtime"))
    datasets = sorted(set(all_urns("DATASET")) | {PROBE})
    results = {}
    try:
        emit_upstreams(server, [UPSTREAM])
        results["with_edge"], _ = g1(server, datasets, expected, args.optional_chains)
        emit_upstreams(server, [])
        results["edge_removed"], checks = g1(server, datasets, expected, args.optional_chains)
        failed = {c["check"]: c["failed"] for c in checks if c["failed"]}
        results["failure_names_probe"] = failed == {"gold_has_silver_upstream": ["gold.g1_probe"]}
        emit_upstreams(server, [UPSTREAM])
        results["restored"], _ = g1(server, datasets, expected, args.optional_chains)
    finally:
        DataHubGraph(DatahubClientConfig(server=server)).hard_delete_entity(PROBE)
    results["probe_deleted"] = PROBE not in live_edges_safe(server)
    ok = (
        results["with_edge"]
        and not results["edge_removed"]
        and results["failure_names_probe"]
        and results["restored"]
        and results["probe_deleted"]
    )
    print(json.dumps({"event": "g1_negative_finished", **results, "ok": ok}))
    return 0 if ok else 1


def live_edges_safe(server: str) -> set[str]:
    """The probe if it still has upstreams (a hard-deleted probe has no aspect: 404 → empty)."""
    return {urn for urn, ups in live_edges([PROBE], server).items() if ups}


if __name__ == "__main__":
    raise SystemExit(main())
