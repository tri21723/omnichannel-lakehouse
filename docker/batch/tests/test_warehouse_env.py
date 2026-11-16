"""Verify least-privilege database credential wiring for Phase 4E warehouse export."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_warehouse_jdbc_env_scoped_to_driver_only():
    compose = yaml.safe_load((REPO_ROOT / "docker/compose.batch.yml").read_text())
    services = compose["services"]

    submit_env = services["spark-submit"]["environment"]
    assert "SPARK_WRITER_DB_PASSWORD" in submit_env
    assert "WAREHOUSE_JDBC_URL" in submit_env

    for service_name in ("spark-master", "spark-worker1", "spark-worker2"):
        worker_env = services[service_name]["environment"]
        assert "SPARK_WRITER_DB_PASSWORD" not in worker_env
        assert "WAREHOUSE_JDBC_URL" not in worker_env
        assert "SPARK_READER_DB_PASSWORD" not in worker_env


def test_env_example_declares_warehouse_jdbc_settings():
    env_text = (REPO_ROOT / ".env.example").read_text()
    keys = {
        line.split("=", 1)[0]
        for line in env_text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    assert "SPARK_WRITER_DB_PASSWORD" in keys
    assert "WAREHOUSE_JDBC_URL" in keys
