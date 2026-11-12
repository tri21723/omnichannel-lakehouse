"""Immutable local datasets, chunked JSONL and manifest reconciliation."""

import hashlib
import json
import random
import tempfile
from datetime import timedelta
from pathlib import Path

from generators.common.config import CONTRACT_VERSION, GeneratorConfig, canonical_json
from generators.common.orders import daily_orders, source_order, stable_seed


def fingerprint(path: Path) -> dict:
    digest = hashlib.sha256()
    rows = 0
    with path.open("rb") as stream:
        for line in stream:
            digest.update(line)
            rows += 1
    return {"sha256": digest.hexdigest(), "rows": rows, "bytes": path.stat().st_size}


def manifest_files(root: Path, manifest: dict) -> list[tuple[Path, dict]]:
    """Reject absolute/traversal paths before reading manifest-controlled files."""
    files = []
    seen = set()
    for channel in manifest["channels"].values():
        for entry in channel["files"]:
            relative = Path(entry["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Manifest path must be relative without traversal")
            path = root / relative
            if path.resolve().is_relative_to(root.resolve()) is False or path == root:
                raise ValueError("Manifest path escapes dataset directory")
            if entry["path"] in seen:
                raise ValueError("Duplicate manifest path")
            seen.add(entry["path"])
            files.append((path, entry))
    return files


def verify_local(root: Path) -> dict:
    manifest = json.loads((root / "stream_manifest.json").read_text())
    config = GeneratorConfig.model_validate(manifest["config"])
    if manifest["contract"] != CONTRACT_VERSION or config.run_id != manifest["run_id"]:
        raise ValueError("Manifest contract or run identity mismatch")
    listed = manifest_files(root, manifest)
    if {p.resolve() for p, _ in listed} != {p.resolve() for p in root.rglob("*.jsonl")}:
        raise ValueError("Manifest file inventory mismatch")
    for path, entry in listed:
        actual = fingerprint(path)
        if any(actual[key] != entry[key] for key in actual):
            raise ValueError(f"File reconciliation failed: {entry['path']}")
    if set(manifest["channels"]) != set(config.channels):
        raise ValueError("Manifest channel inventory mismatch")
    for channel in manifest["channels"].values():
        if sum(f["rows"] for f in channel["files"]) != channel["rows"]:
            raise ValueError("Manifest channel row count mismatch")
        expected_unique = config.days * config.orders_per_day_per_channel
        expected_dup = config.days * round(config.orders_per_day_per_channel * config.dup_rate)
        if (channel["unique_orders"], channel["dup_rows"], channel["rows"]) != (
            expected_unique,
            expected_dup,
            expected_unique + expected_dup,
        ):
            raise ValueError("Manifest ground truth mismatch")
    return manifest


def generate(config: GeneratorConfig, output: Path) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    final = output / config.run_id
    if final.exists():
        verify_local(final)
        return final
    with tempfile.TemporaryDirectory(prefix=".generating-", dir=output) as temp:
        root = Path(temp) / "dataset"
        root.mkdir()
        manifest = {
            "contract": CONTRACT_VERSION,
            "run_id": config.run_id,
            "seed": config.seed,
            "config": config.model_dump(mode="json"),
            # Logical time guarantees byte-identical artifacts on replay.
            "generated_at": f"{config.start_date.isoformat()}T00:00:00+00:00",
            "channels": {},
        }
        for channel in config.channels:
            summary = {
                "files": [],
                "rows": 0,
                "unique_orders": 0,
                "dup_rows": 0,
                "hot_seller_orders": 0,
                "return_orders": 0,
                "min_ts": None,
                "max_ts": None,
            }
            for day in range(config.days):
                count = config.orders_per_day_per_channel
                dup_rng = random.Random(stable_seed(config.seed, channel, day, "dup"))
                duplicates = set(dup_rng.sample(range(count), round(count * config.dup_rate)))
                partition = root / channel / f"dt={config.start_date + timedelta(days=day)}"
                partition = partition / f"run_id={config.run_id}"
                partition.mkdir(parents=True)
                part = 0
                written = 0
                stream = None
                try:
                    for index, order in enumerate(daily_orders(config, channel, day)):
                        ts = order["order_ts"]
                        summary["min_ts"] = min(summary["min_ts"] or ts, ts)
                        summary["max_ts"] = max(summary["max_ts"] or ts, ts)
                        summary["unique_orders"] += 1
                        summary["hot_seller_orders"] += int(
                            order["items"][0]["seller_id"] == f"{channel}-s00"
                        )
                        summary["return_orders"] += bool(order["returns"])
                        payload = canonical_json(source_order(order, channel, day, config)) + "\n"
                        for _ in range(1 + (index in duplicates)):
                            if written % config.rows_per_file == 0:
                                if stream is not None:
                                    stream.close()
                                stream = (partition / f"part-{part:05d}.jsonl").open("w")
                                part += 1
                            stream.write(payload)
                            written += 1
                    summary["rows"] += written
                    summary["dup_rows"] += len(duplicates)
                finally:
                    if stream is not None:
                        stream.close()
                for path in sorted(partition.glob("*.jsonl")):
                    summary["files"].append(
                        {"path": path.relative_to(root).as_posix(), **fingerprint(path)}
                    )
            manifest["channels"][channel] = summary
        (root / "stream_manifest.json").write_text(canonical_json(manifest) + "\n")
        verify_local(root)
        root.rename(final)
    return final
