"""Close a stream run: inventory committed Flink files into stream_output_manifest.json.

Run after `make stream-stop` (drain + final savepoint). The manifest is written last and is
immutable, like the batch manifest, so Bronze only ever sees a complete, fingerprinted run
(ADR-0016 §8). Refuses while in-progress (hidden) files exist or when raw event rows do not add up
to the ground truth's valid records.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from generators.batch.dataset import fingerprint
from generators.common.storage import put_immutable
from generators.stream.publish import write_json
from generators.stream.verify import read_json

CONTRACT = "stream-output-v1"
EVENT_BRANCHES = ("accepted", "late", "duplicates")
BRANCHES = (*EVENT_BRANCHES, "windows")


def object_fingerprint(s3, bucket: str, key: str) -> dict:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    digest, size, rows = hashlib.sha256(), 0, 0
    try:
        for chunk in body.iter_chunks(chunk_size=1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            rows += chunk.count(b"\n")
    finally:
        body.close()
    return {"sha256": digest.hexdigest(), "bytes": size, "rows": rows}


def inventory(s3, bucket: str, run_id: str, channel: str) -> tuple[list[dict], int]:
    files, hidden = [], 0
    for branch in BRANCHES:
        prefix = f"stream/{channel}/{branch}/run={run_id}/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                name = key.rsplit("/", 1)[-1]
                if name.startswith("."):
                    hidden += 1
                    continue
                date = key[len(prefix) :].split("/", 1)[0].removeprefix("date=")
                files.append(
                    {
                        "path": key,
                        "branch": branch,
                        "date": date,
                        **object_fingerprint(s3, bucket, key),
                    }
                )
    return sorted(files, key=lambda f: f["path"]), hidden


def build_manifest(s3, bucket: str, run_id: str) -> dict:
    truth_key = f"manifests/{run_id}/stream_ground_truth.json"
    truth = read_json(s3, bucket, truth_key)
    truth_hash = object_fingerprint(s3, bucket, truth_key)["sha256"]
    channels = {}
    for channel, data in sorted(truth["channels"].items()):
        files, hidden = inventory(s3, bucket, run_id, channel)
        if hidden:
            raise RuntimeError(
                f"{channel}: {hidden} in-progress files; stop the jobs with --drain first"
            )
        event_rows = sum(f["rows"] for f in files if f["branch"] in EVENT_BRANCHES)
        if event_rows != data["valid_records"]:
            raise RuntimeError(
                f"{channel}: {event_rows} raw event rows, ground truth has {data['valid_records']}"
            )
        channels[channel] = {
            "valid_records": data["valid_records"],
            "dates": sorted({f["date"] for f in files if f["branch"] in EVENT_BRANCHES}),
            "files": files,
        }
    return {
        "contract": CONTRACT,
        "stream_run_id": run_id,
        "ground_truth_sha256": truth_hash,
        "channels": channels,
    }


def close_stream(run_id: str, s3, bucket: str, output: Path) -> dict:
    manifest = build_manifest(s3, bucket, run_id)
    path = write_json(output / "stream" / run_id / "stream_output_manifest.json", manifest)
    put_immutable(
        s3, bucket, f"manifests/{run_id}/stream_output_manifest.json", path, fingerprint(path)
    )
    return {
        "stream_run_id": run_id,
        "files": sum(len(c["files"]) for c in manifest["channels"].values()),
        "rows": {c: m["valid_records"] for c, m in manifest["channels"].items()},
        "dates": {c: m["dates"] for c, m in manifest["channels"].items()},
    }


__all__ = ["close_stream", "build_manifest"]
