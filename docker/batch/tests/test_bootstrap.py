"""Catalog bootstrap must not mutate an existing warehouse location."""

import importlib.util
import json
from pathlib import Path

import pytest

source = Path(__file__).parents[1] / "bootstrap.py"
spec = importlib.util.spec_from_file_location("bootstrap", source)
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


@pytest.mark.parametrize("wrong_bucket", [False, True])
def test_existing_profile_is_verified_before_runtime_config(tmp_path, monkeypatch, wrong_bucket):
    env = {
        "LAKEKEEPER_URI": "http://catalog",
        "LAKEHOUSE_WAREHOUSE": "test",
        "S3_LAKEHOUSE_BUCKET": "lakehouse",
        "S3_ENDPOINT": "http://minio:9000",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    calls = []

    def request(base, method, path, payload=None):
        calls.append(method)
        if path.endswith("/info"):
            return {"bootstrapped": True, "default-project-id": "project"}
        if "?project-id=" in path:
            return {"warehouses": [{"id": "warehouse", "name": "test"}]}
        return {
            "storage-profile": {
                "type": "s3",
                "bucket": "wrong" if wrong_bucket else "lakehouse",
                "key-prefix": "warehouse",
                "endpoint": "http://minio:9000/",
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
            }
        }

    monkeypatch.setattr(bootstrap, "request", request)
    if wrong_bucket:
        with pytest.raises(ValueError, match="refusing"):
            bootstrap.main(tmp_path)
        assert not (tmp_path / "catalog.json").exists()
    else:
        bootstrap.main(tmp_path)
        bootstrap.main(tmp_path)
        assert json.loads((tmp_path / "catalog.json").read_text())["warehouse_id"] == "warehouse"
    assert set(calls) == {"GET"}
