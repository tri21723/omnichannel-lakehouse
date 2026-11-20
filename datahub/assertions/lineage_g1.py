"""G1: assert lineage coverage from sources to the warehouse in DataHub (plan G1, ADR-0018 §5).

Reads every catalogued dataset and its live `upstreamLineage` aspect from the internal GMS, then:

- every Gold table (except the static `dim_channel`) reaches a Silver table upstream;
- every Silver table reaches a Bronze table upstream;
- every Bronze table has a source upstream (MinIO prefix, PostgreSQL table or Kafka topic);
- every warehouse table has its Gold table as direct upstream;
- stream Bronze reaches its Kafka topic; CDC Silver reaches the CDC topic and `walmart.orders`;
- every edge built from the ledgers (`lineage.model`) is present in DataHub (cross-check).

Prints one JSON line per check and exits 1 when any check fails. `evaluate()` is pure.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from pathlib import Path

from lineage import model
from lineage.jobs import layer

Edges = dict[str, set[str]]


def reaches(edges: Edges, start: str, wanted, max_hops: int = 6) -> bool:
    """True if an upstream path from `start` hits a dataset for which `wanted(urn)` holds."""
    frontier, seen = {start}, {start}
    for _ in range(max_hops):
        frontier = {u for d in frontier for u in edges.get(d, set())} - seen
        if any(wanted(u) for u in frontier):
            return True
        seen |= frontier
    return False


def name(urn: str) -> str:
    return urn.split(",")[1]


def evaluate(
    datasets: Iterable[str], live: Edges, expected: Edges, optional_chains: bool = False
) -> list[dict]:
    """`optional_chains`: stream/CDC chains are checked only where they exist (a fresh CI runner has
    no stream or CDC run); by default both must be present."""
    datasets = sorted(set(datasets))
    checks = []

    def check(check_id: str, subjects: list[str], ok) -> None:
        failed = [name(s) for s in subjects if not ok(s)]
        checks.append({"check": check_id, "subjects": len(subjects), "failed": failed})

    static = {model.iceberg(f"lh.gold.{t}") for t in model.STATIC_GOLD}
    gold = [d for d in datasets if layer(d) == "gold" and d not in static]
    silver = [d for d in datasets if layer(d) == "silver"]
    bronze = [d for d in datasets if layer(d) == "bronze"]
    warehouse = [
        d
        for d in datasets
        if layer(d) == "postgres"
        and name(d).startswith("sources.warehouse.")
        and name(d).split(".")[-1] in model.GOLD_UPSTREAMS
    ]
    check(
        "gold_has_silver_upstream", gold, lambda d: reaches(live, d, lambda u: layer(u) == "silver")
    )
    check(
        "silver_has_bronze_upstream",
        silver,
        lambda d: reaches(live, d, lambda u: layer(u) == "bronze"),
    )
    sources = ("s3", "postgres", "kafka")
    check(
        "bronze_has_source_upstream",
        bronze,
        lambda d: any(layer(u) in sources for u in live.get(d, ())),
    )
    check(
        "warehouse_has_gold_upstream",
        warehouse,
        lambda d: model.iceberg(f"lh.gold.{name(d).split('.')[-1]}") in live.get(d, set()),
    )
    present = set(datasets)

    def chain(urns: list[str]) -> list[str]:
        return [u for u in urns if u in present] if optional_chains else urns

    stream = chain([model.iceberg(f"lh.bronze.{t}") for t in model.STREAM_TABLES.values()])
    topics = {model.kafka(t) for t in model.STREAM_TOPICS.values()}
    check("stream_bronze_reaches_topic", stream, lambda d: reaches(live, d, lambda u: u in topics))
    cdc = chain([model.iceberg(model.CDC_SILVER), model.iceberg(model.CDC_QUARANTINE)])
    check(
        "cdc_silver_reaches_source",
        cdc,
        lambda d: reaches(live, d, lambda u: u == model.kafka(model.CDC_TOPIC))
        and reaches(live, d, lambda u: u == model.postgres(model.CDC_SOURCE)),
    )
    pairs = sorted((d, u) for d, ups in expected.items() for u in ups)
    missing = [f"{name(u)} -> {name(d)}" for d, u in pairs if u not in live.get(d, set())]
    checks.append({"check": "ledger_edges_present", "subjects": len(pairs), "failed": missing})
    absent = sorted(name(d) for d in expected if d not in set(datasets))
    checks.append(
        {"check": "ledger_downstreams_catalogued", "subjects": len(expected), "failed": absent}
    )
    return checks


def live_edges(urns: Iterable[str], gms_url: str) -> Edges:
    import urllib.error
    import urllib.request

    edges: Edges = {}
    for urn in urns:
        quoted = urllib.parse.quote(urn, safe="")
        url = f"{gms_url}/openapi/v3/entity/dataset/{quoted}?aspects=upstreamLineage"
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                body = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code != 404:  # 404: no upstreamLineage aspect (sources, deleted probes)
                raise
            body = {}
        upstreams = (body.get("upstreamLineage") or {}).get("value", {}).get("upstreams", [])
        edges[urn] = {u["dataset"] for u in upstreams}
    return edges


def main() -> int:
    import os

    from lineage.emit_lineage import load
    from tools.gms import all_urns

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, default=Path("/runtime"))
    parser.add_argument("--include", action="append", default=[], help="extra dataset URN to check")
    parser.add_argument("--optional-chains", action="store_true", help="CI: no stream/CDC run")
    args = parser.parse_args()
    gms_url = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080")
    datasets = sorted(set(all_urns("DATASET")) | set(args.include))
    checks = evaluate(
        datasets, live_edges(datasets, gms_url), load(args.runtime), args.optional_chains
    )
    for item in checks:
        print(json.dumps({"event": "g1_check", "ok": not item["failed"], **item}))
    ok = all(not item["failed"] for item in checks)
    print(
        json.dumps(
            {"event": "g1_finished", "checks": len(checks), "datasets": len(datasets), "ok": ok}
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
