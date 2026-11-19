import json
import runpy
from pathlib import Path

CONNECTOR = json.loads(Path("docker/cdc/connectors/walmart-orders.json").read_text())


def test_connector_holds_no_secret_and_captures_orders_only():
    assert CONNECTOR["database.password"] == "${env:DEBEZIUM_REPL_DB_PASSWORD}"
    assert CONNECTOR["database.user"] == "debezium_repl"
    assert CONNECTOR["table.include.list"] == "walmart.orders"  # no PII tables
    assert CONNECTOR["publication.name"] == "dbz_pub"
    assert CONNECTOR["publication.autocreate.mode"] == "disabled"
    assert CONNECTOR["topic.prefix"] == "platform_cdc"


def test_connector_keeps_money_exact_and_heartbeat_out_of_kafka():
    assert CONNECTOR["decimal.handling.mode"] == "string"
    assert "pg_logical_emit_message" in CONNECTOR["heartbeat.action.query"]
    assert "'debezium-heartbeat'" in CONNECTOR["heartbeat.action.query"]
    # Logical messages would go to platform_cdc.message, which does not exist (auto-create off).
    assert CONNECTOR["message.prefix.exclude.list"] == "debezium-heartbeat"
    # Connector-level topic.creation.* keys switch on topic creation groups (needs more config).
    assert not any(key.startswith("topic.creation") for key in CONNECTOR)


def test_cdc_env_adds_image_once(monkeypatch, tmp_path):
    script = Path("docker/cdc/env.py").resolve()
    monkeypatch.chdir(tmp_path)
    path = tmp_path / ".env"
    path.write_text("DEBEZIUM_REPL_DB_PASSWORD=keep-me")
    runpy.run_path(str(script))
    first = path.read_text()
    assert "DEBEZIUM_REPL_DB_PASSWORD=keep-me" in first
    assert "DEBEZIUM_CONNECT_IMAGE=quay.io/debezium/connect:3.6.3.Final" in first
    runpy.run_path(str(script))
    assert path.read_text() == first
    assert path.stat().st_mode & 0o777 == 0o600
