"""Bounded immutable JSONL ingestion, independent of Spark and business schemas."""

import hashlib
import json
import re
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any

CHANNELS = ("walmart", "amazon", "tiktok_shop", "shopee", "lazada")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def identity(run_id: str, run_date: str, channel: str, source: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{20}", run_id) or channel not in CHANNELS:
        raise ValueError("Invalid run ID or channel")
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError("Date must be ISO YYYY-MM-DD")
    if source not in {"jsonl", "jdbc"} or (source == "jdbc" and channel != "walmart"):
        raise ValueError("Unsupported source/channel")
    return hashlib.sha256(
        canonical(["bronze-v1", run_id, run_date, channel, source]).encode()
    ).hexdigest()[:24]


def select_files(manifest: dict, run_id: str, run_date: str, channel: str) -> list[dict]:
    identity(run_id, run_date, channel, "jsonl")
    config = manifest["config"]
    expected_id = hashlib.sha256(
        canonical({"contract": "batch-v1", "config": config}).encode()
    ).hexdigest()[:20]
    if manifest["contract"] != "batch-v1" or manifest["run_id"] != run_id or expected_id != run_id:
        raise ValueError("Manifest identity mismatch")
    dates = {
        (date.fromisoformat(config["start_date"]) + timedelta(days=n)).isoformat()
        for n in range(config["days"])
    }
    if run_date not in dates or channel not in config["channels"]:
        raise ValueError("Channel/date outside manifest")
    if set(manifest["channels"]) != set(config["channels"]):
        raise ValueError("Channel inventory mismatch")
    seen, selected = set(), []
    for name, summary in manifest["channels"].items():
        totals = {day: 0 for day in dates}
        for entry in summary["files"]:
            pattern = rf"{name}/dt=(\d{{4}}-\d{{2}}-\d{{2}})/run_id={run_id}/part-\d{{5}}\.jsonl"
            match = re.fullmatch(pattern, entry["path"])
            if not match or match[1] not in dates or entry["path"] in seen:
                raise ValueError("Invalid/duplicate object key")
            seen.add(entry["path"])
            if any(type(entry[k]) is not int or entry[k] < 0 for k in ("rows", "bytes")):
                raise ValueError("Invalid inventory sizes")
            if not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]):
                raise ValueError("Invalid checksum")
            totals[match[1]] += entry["rows"]
            if name == channel and match[1] == run_date:
                selected.append(entry)
        unique = config["days"] * config["orders_per_day_per_channel"]
        duplicates = config["days"] * round(
            config["orders_per_day_per_channel"] * config["dup_rate"]
        )
        if (summary["rows"], summary["unique_orders"], summary["dup_rows"]) != (
            unique + duplicates,
            unique,
            duplicates,
        ):
            raise ValueError("Ground truth mismatch")
        expected = config["orders_per_day_per_channel"] + round(
            config["orders_per_day_per_channel"] * config["dup_rate"]
        )
        if any(total != expected for total in totals.values()):
            raise ValueError("Partition row inventory mismatch")
    if not selected:
        raise ValueError("No source files")
    return sorted(selected, key=lambda e: e["path"])


def read_manifest(client: Any, bucket: str, run_id: str) -> tuple[dict, str]:
    response = client.get_object(Bucket=bucket, Key=f"manifests/{run_id}/stream_manifest.json")
    with response["Body"] as body:
        content = body.read(4 * 1024 * 1024 + 1)
    if len(content) > 4 * 1024 * 1024:
        raise ValueError("Manifest too large")
    return json.loads(content), hashlib.sha256(content).hexdigest()


def parse_status(raw: str) -> str:
    if not raw.strip():
        return "blank"
    try:
        json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        return "valid"
    except (ValueError, RecursionError):
        return "invalid"


def stage(client: Any, bucket: str, entries: list[dict], root: Path, limit: int) -> list[Path]:
    total = sum(e["bytes"] for e in entries)
    # Raw download plus JSON escaping can consume up to 6x input. Reserve overhead.
    if total > limit or shutil.disk_usage(root).free < total * 8 + 1024 * 1024:
        raise ValueError("Staging byte/disk limit exceeded")
    paths = []
    for index, entry in enumerate(entries):
        response = client.get_object(Bucket=bucket, Key=entry["path"])
        download = root / f"{index}.download"
        digest, size = hashlib.sha256(), 0
        with response["Body"] as body, download.open("wb") as target:
            while chunk := body.read(1024 * 1024):
                size += len(chunk)
                if size > entry["bytes"] or size > limit:
                    raise ValueError("Object exceeds declared size")
                digest.update(chunk)
                target.write(chunk)
        if size != entry["bytes"] or digest.hexdigest() != entry["sha256"]:
            raise ValueError("Object fingerprint mismatch")
        output = root / f"{index}.jsonl"
        rows = 0
        with (
            download.open("rb") as source,
            output.with_suffix(".tmp").open("w", encoding="utf-8") as target,
        ):
            for line in source:
                payload = line
                if payload.endswith(b"\n"):
                    payload = payload[:-1]
                    if payload.endswith(b"\r"):
                        payload = payload[:-1]
                raw = payload.decode("utf-8")
                target.write(
                    canonical(
                        {
                            "_raw": raw,
                            "_source_file": f"s3://{bucket}/{entry['path']}",
                            "_parse_status": parse_status(raw),
                        }
                    )
                    + "\n"
                )
                rows += 1
        if rows != entry["rows"]:
            raise ValueError("Object row count mismatch")
        output.with_suffix(".tmp").replace(output)
        download.unlink()
        paths.append(output)
    return paths
