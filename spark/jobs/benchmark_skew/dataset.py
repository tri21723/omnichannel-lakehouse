"""Build or verify pinned benchmark datasets from a completed Gold publication (ADR-0012 §2).

Synthetic amplification: lines of one Gold run are replicated with new deterministic keys
and reassigned to products with a configured hot share. This is not an end-to-end ingest.
Writes only ``lh.benchmark.items_<name>`` / ``products_<name>`` and the dataset manifest.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from spark.lib.benchmark_skew import (
    SCHEMA_VERSION,
    dataset_manifest_path,
    table_name,
    validate_dataset_spec,
    write_json_atomic,
)
from spark.lib.bronze import event

CONFIG = Path(__file__).with_name("configs") / "datasets.json"
GOLD_LEDGER = Path("/runtime/gold/publication/ledger.json")
TOP_SHARE_TOLERANCE = 0.01


def load_specs(names: list[str]) -> list[dict]:
    specs = {s["name"]: s for s in json.loads(CONFIG.read_text())["datasets"]}
    unknown = set(names) - set(specs)
    if unknown:
        raise ValueError(f"unknown datasets: {sorted(unknown)}")
    return [validate_dataset_spec(specs[name]) for name in names or specs]


def read_pinned(spark, manifest: dict):
    from spark.lib.silver_publish import frame_at, summary

    frames = {}
    for kind in ("items", "products"):
        evidence = manifest["tables"][kind]
        frame = frame_at(spark, evidence["table"], evidence, None)
        actual = summary(frame)
        if actual != evidence["content"]:
            raise ValueError(f"{evidence['table']} content mismatch: {actual}")
        frames[kind] = frame
    return frames["items"], frames["products"]


def check_ground_truth(spec: dict, source_rows: int, dist: dict, unmatched: int) -> list[str]:
    problems = []
    if dist["rows"] != source_rows * spec["factor"]:
        problems.append(f"rows {dist['rows']} != {source_rows} x {spec['factor']}")
    if dist["distinct_orders"] != dist["rows"] or dist["max_lines_per_order"] != 1:
        problems.append("expected one line per order after amplification")
    if unmatched:
        problems.append(f"{unmatched} lines without dim_product")
    for channel, share in dist["per_channel_top_share"].items():
        cold_keys = dist["per_channel_keys"][channel] - (1 if spec["hot_share"] > 0 else 0)
        expected = spec["hot_share"] if spec["hot_share"] > 0 else 1 / max(cold_keys, 1)
        if abs(share - expected) > max(TOP_SHARE_TOLERANCE, 3 / dist["rows"] ** 0.5):
            problems.append(f"{channel} top share {share} far from {expected:.4f}")
    return problems


def build(spark, spec: dict, force: bool) -> dict:
    from spark.lib.benchmark_workload import (
        amplify_dataset,
        check_dimension_unique,
        distribution,
        unmatched_lines,
    )
    from spark.lib.dp3_input import load_gold_publication, read_gold_entities
    from spark.lib.silver_publish import summary

    ledger, publication_id = load_gold_publication(GOLD_LEDGER)
    path = dataset_manifest_path(spec["name"])
    if path.exists() and not force:
        current = json.loads(path.read_text())
        if current.get("spec") == spec and current.get("gold_publication_id") == publication_id:
            read_pinned(spark, current)  # verifies pinned content before skipping
            event("benchmark_dataset_skipped", dataset=spec["name"], reason="up_to_date")
            return current

    frames, snapshots, _ = read_gold_entities(spark, ledger, spec["source_run"], publication_id)
    source_rows = frames["fact_order_items"].count()
    bench_run = f"bench_{spec['name']}"
    items, products = amplify_dataset(
        frames["fact_order_items"],
        frames["dim_product"],
        spec["source_run"],
        bench_run,
        spec["factor"],
        spec["hot_share"],
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.benchmark")
    tables = {}
    for kind, frame, partitions in (("items", items, 16), ("products", products, 1)):
        target = table_name(kind, spec["name"])
        (
            frame.repartition(partitions, frame.columns[0])
            .writeTo(target)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .createOrReplace()
        )
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        pinned = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(target)
        tables[kind] = {"table": target, "snapshot": snapshot, "content": summary(pinned)}

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "spec": spec,
        "kind": "synthetic_amplification",
        "gold_publication_id": publication_id,
        "gold_snapshots": {name: snapshots[name] for name in ("fact_order_items", "dim_product")},
        "source_rows": source_rows,
        "tables": tables,
        "created_at_epoch": round(time.time(), 3),
    }
    pinned_items, pinned_products = read_pinned(spark, manifest)
    check_dimension_unique(pinned_products)
    manifest["distribution"] = distribution(pinned_items)
    manifest["unmatched_lines"] = unmatched_lines(pinned_items, pinned_products)
    problems = check_ground_truth(
        spec, source_rows, manifest["distribution"], manifest["unmatched_lines"]
    )
    if problems:
        raise ValueError(f"ground truth check failed for {spec['name']}: {problems}")
    write_json_atomic(path, manifest)
    event("benchmark_dataset_built", dataset=spec["name"], **manifest["distribution"])
    return manifest


def verify(spark, spec: dict) -> dict:
    from spark.lib.benchmark_workload import check_dimension_unique, distribution, unmatched_lines

    path = dataset_manifest_path(spec["name"])
    manifest = json.loads(path.read_text())
    if manifest.get("spec") != spec:
        raise ValueError(f"manifest spec differs from config for {spec['name']}")
    items, products = read_pinned(spark, manifest)
    check_dimension_unique(products)
    dist = distribution(items)
    if dist != manifest["distribution"]:
        raise ValueError(f"distribution changed for {spec['name']}: {dist}")
    unmatched = unmatched_lines(items, products)
    problems = check_ground_truth(spec, manifest["source_rows"], dist, unmatched)
    if problems:
        raise ValueError(f"ground truth check failed for {spec['name']}: {problems}")
    event("benchmark_dataset_verified", dataset=spec["name"], rows=dist["rows"])
    return manifest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasets", default="", help="comma-separated names (default: all)")
    parser.add_argument("--mode", choices=("build", "verify"), default="build")
    parser.add_argument("--force", action="store_true", help="rebuild even if up to date")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    names = [n for n in args.datasets.split(",") if n]
    specs = load_specs(names)

    from spark.lib.session import get_spark

    spark = get_spark(app_name="benchmark-dataset")
    try:
        for spec in specs:
            if args.mode == "build":
                build(spark, spec, args.force)
            else:
                verify(spark, spec)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
