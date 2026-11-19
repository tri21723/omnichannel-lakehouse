import base64
import runpy
from pathlib import Path


def test_stream_settings_are_added_once_without_rotation(monkeypatch, tmp_path):
    script = Path("docker/stream/env.py").resolve()
    monkeypatch.chdir(tmp_path)
    path = tmp_path / ".env"
    path.write_text("EXISTING=preserved\nFLINK_S3_SECRET_KEY=keep-me")
    runpy.run_path(str(script))
    first = path.read_text()
    values = dict(line.split("=", 1) for line in first.splitlines())
    assert values["EXISTING"] == "preserved"
    assert values["FLINK_S3_SECRET_KEY"] == "keep-me"
    assert values["FLINK_S3_ACCESS_KEY"] == "flink_local"
    assert values["KAFKA_IMAGE"] == "apache/kafka:4.3.2"
    assert values["FLINK_IMAGE"] == "flink:2.2.1-java17"
    cluster_id = values["KAFKA_CLUSTER_ID"]
    assert len(cluster_id) == 22
    assert len(base64.urlsafe_b64decode(cluster_id + "==")) == 16
    runpy.run_path(str(script))
    assert path.read_text() == first
    assert path.stat().st_mode & 0o777 == 0o600


def test_flink_policy_is_limited_to_stream_prefixes():
    import json

    policy = json.loads(Path("docker/stream/policies/flink.json").read_text())
    writable = {
        resource
        for statement in policy["Statement"]
        if "s3:PutObject" in statement["Action"]
        for resource in statement["Resource"]
    }
    assert writable == {"arn:aws:s3:::raw/stream/*", "arn:aws:s3:::checkpoints/flink/*"}
