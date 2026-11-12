"""Publication ordering, immutability and remote reconciliation failure paths."""

import io

import pytest
from botocore.exceptions import ClientError

from generators.batch.dataset import generate
from generators.common.config import GeneratorConfig
from generators.common.storage import publish, verify_remote


class Body(io.BytesIO):
    def iter_chunks(self, chunk_size):
        while chunk := self.read(chunk_size):
            yield chunk


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.fail = False

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": Body(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch):
        assert IfNoneMatch == "*"
        if self.fail:
            raise RuntimeError("Injected upload failure")
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = Body.read()
        self.puts.append(Key)

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        yield {"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]}


def test_manifest_last_replay_no_writes_and_remote_tamper_detected(tmp_path):
    root = generate(GeneratorConfig(days=1, orders_per_day_per_channel=2), tmp_path)
    s3 = MemoryS3()
    publish(root, s3, "raw")
    assert s3.puts[-1] == f"manifests/{root.name}/stream_manifest.json"
    first_puts = s3.puts.copy()
    publish(root, s3, "raw")
    assert s3.puts == first_puts
    verify_remote(root, s3, "raw")
    s3.objects[s3.puts[0]] = b"corrupt\n"
    with pytest.raises(ValueError, match="reconciliation"):
        verify_remote(root, s3, "raw")
    with pytest.raises(ValueError, match="immutable"):
        publish(root, s3, "raw")


def test_failed_upload_never_publishes_completion_manifest(tmp_path):
    root = generate(GeneratorConfig(days=1, orders_per_day_per_channel=1), tmp_path)
    s3 = MemoryS3()
    s3.fail = True
    with pytest.raises(RuntimeError, match="Injected"):
        publish(root, s3, "raw")
    assert not any(key.endswith("stream_manifest.json") for key in s3.objects)
