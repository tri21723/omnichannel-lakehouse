"""Publish immutable S3 objects and reconcile actual content, not ETags."""

import hashlib
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from generators.batch.dataset import fingerprint, manifest_files, verify_local


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["GENERATOR_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["GENERATOR_S3_SECRET_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def remote_fingerprint(client, bucket: str, key: str) -> dict | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise
    digest = hashlib.sha256()
    rows = size = 0
    body = response["Body"]
    try:
        for chunk in body.iter_chunks(chunk_size=1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            rows += chunk.count(b"\n")
    finally:
        body.close()
    return {"sha256": digest.hexdigest(), "rows": rows, "bytes": size}


def put_immutable(client, bucket: str, key: str, path: Path, expected: dict) -> None:
    actual = remote_fingerprint(client, bucket, key)
    if actual is None:
        try:
            with path.open("rb") as stream:
                client.put_object(Bucket=bucket, Key=key, Body=stream, IfNoneMatch="*")
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                raise
        actual = remote_fingerprint(client, bucket, key)
    if actual != expected:
        raise ValueError(f"S3 immutable object mismatch: {key}")


def publish(root: Path, client, bucket: str) -> dict:
    manifest = verify_local(root)
    for path, entry in manifest_files(root, manifest):
        put_immutable(
            client,
            bucket,
            entry["path"],
            path,
            {key: entry[key] for key in ("sha256", "rows", "bytes")},
        )
    manifest_path = root / "stream_manifest.json"
    key = f"manifests/{manifest['run_id']}/stream_manifest.json"
    put_immutable(client, bucket, key, manifest_path, fingerprint(manifest_path))
    return manifest


def verify_remote(root: Path, client, bucket: str) -> dict:
    manifest = verify_local(root)
    key = f"manifests/{manifest['run_id']}/stream_manifest.json"
    if remote_fingerprint(client, bucket, key) != fingerprint(root / "stream_manifest.json"):
        raise ValueError("S3 completion manifest mismatch or missing")
    expected_keys = set()
    prefixes = set()
    for _, entry in manifest_files(root, manifest):
        expected_keys.add(entry["path"])
        prefixes.add(entry["path"].rsplit("/", 1)[0] + "/")
        expected = {key: entry[key] for key in ("sha256", "rows", "bytes")}
        if remote_fingerprint(client, bucket, entry["path"]) != expected:
            raise ValueError(f"S3 file reconciliation failed: {entry['path']}")
    actual_keys = set()
    for prefix in prefixes:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            actual_keys.update(obj["Key"] for obj in page.get("Contents", []))
    if actual_keys != expected_keys:
        raise ValueError("S3 file inventory mismatch")
    return manifest
