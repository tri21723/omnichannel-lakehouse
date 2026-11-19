"""Stream generator: determinism, ground truth, delay classes, publish-once semantics."""

import json
import re
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from generators.cli import app
from generators.common.config import GeneratorConfig
from generators.stream.config import (
    LATE_MAX_MS,
    LATE_MIN_MS,
    ON_TIME_MAX_MS,
    RETRY_MAX_MS,
    RETRY_MIN_MS,
    TOO_LATE_MIN_MS,
    TOPICS,
    WATERMARK_BOUND_MS,
    StreamConfig,
    read_stream_config,
)
from generators.stream.events import generate, wire_value
from generators.stream.publish import expected_summary, manifest_keys, publish_stream, summarize
from generators.tests.test_storage import MemoryS3

SMALL = Path("generators/fixtures/stream-small.yaml")
REQUIRED = {
    "schema_version",
    "event_id",
    "event_type",
    "stream_run_id",
    "channel",
    "order_id",
    "event_ts",
    "currency",
    "amount",
    "payload",
}


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


@pytest.fixture(scope="module")
def small():
    config = read_stream_config(SMALL)
    sends, truth = generate(config)
    return config, sends, truth


def test_batch_run_ids_do_not_change():
    expected = {
        "generators/fixtures/bronze.yaml": "186ae107d0793d2ee237",
        "generators/config.yaml": "b1e64afaff0176de180a",
        "generators/fixtures/orchestration.yaml": "5f552019ee41c24af95f",
    }
    for path, run_id in expected.items():
        assert GeneratorConfig(**yaml.safe_load(Path(path).read_text())).run_id == run_id


def test_same_seed_same_stream_and_different_seed_differs(small):
    config, sends, truth = small
    again_sends, again_truth = generate(read_stream_config(SMALL))
    assert again_sends == sends and again_truth == truth
    other = config.model_copy(update={"seed": config.seed + 1})
    assert other.stream_run_id != config.stream_run_id
    assert generate(other)[1]["channels"] != truth["channels"]


def test_counts_match_configuration(small):
    config, sends, truth = small
    n = config.events_per_channel
    amazon, tiktok = truth["channels"]["amazon"], truth["channels"]["tiktok_shop"]
    late = round(n * config.late_rate)
    too_late = round(late * config.too_late_share)
    assert amazon["classes"] == {
        "on_time": n - late,
        "late_accepted": late - too_late,
        "too_late": too_late,
    }
    assert tiktok["retries"] == round(n * config.dup_rate) and amazon["retries"] == 0
    assert tiktok["bursts"]["events"] == round(n * config.burst_share)
    for data in truth["channels"].values():
        assert data["malformed"] == round(n * config.malformed_rate)
        assert data["records_sent"] == data["valid_records"] + data["malformed"]
    assert truth["records_total"] == len(sends)


def test_delay_classes_have_margins_and_watermark_can_pass_them(small):
    config, sends, truth = small
    ranges = {
        "on_time": (0, ON_TIME_MAX_MS),
        "late_accepted": (LATE_MIN_MS, LATE_MAX_MS),
        "too_late": (TOO_LATE_MIN_MS, config.late_max_minutes * 60_000),
    }
    first_send = {}
    for send in sends:
        if send.kind == "event":
            delay = send.send_ms - ms(send.value["event_ts"])
            low, high = ranges[send.delay_class]
            assert low <= delay <= high, (send.delay_class, delay)
            first_send[send.event_id] = send.send_ms
    for channel, data in truth["channels"].items():
        newest = ms(data["event_ts_max"])
        for send in sends:
            if send.channel == channel and send.kind == "event" and send.delay_class != "on_time":
                assert newest >= send.send_ms + WATERMARK_BOUND_MS
    for send in sends:
        if send.kind == "retry":
            assert RETRY_MIN_MS <= send.send_ms - first_send[send.event_id] <= RETRY_MAX_MS
    assert [s.send_ms for s in sends] == sorted(s.send_ms for s in sends)


def test_windows_cover_exactly_the_accepted_events(small):
    _, sends, truth = small
    for channel, data in truth["channels"].items():
        accepted = [
            s
            for s in sends
            if s.channel == channel and s.kind == "event" and s.delay_class != "too_late"
        ]
        assert sum(w["events"] for w in data["windows"]) == len(accepted)
        cents = sum(int(w["amount"].replace(".", "")) for w in data["windows"])
        assert cents == sum(int(s.value["amount"].replace(".", "")) for s in accepted)
        for window in data["windows"]:
            assert ms(window["window_start"]) % data["window_ms"] == 0


def test_valid_records_follow_the_contract_and_malformed_do_not(small):
    config, sends, _ = small
    for send in sends:
        payload = json.loads(wire_value(send, 1_767_225_600_000)) if send.raw is None else None
        if send.kind == "malformed":
            try:
                broken = json.loads(send.raw)
            except ValueError:
                continue
            assert (
                "event_id" not in broken
                or broken["schema_version"] != 1
                or not re.fullmatch(r"\d{4}-\d\d-\d\dT[\d:.]+Z", broken["event_ts"])
            )
            continue
        assert set(payload) == REQUIRED | {"emitted_ts"}
        assert payload["schema_version"] == 1 and payload["currency"] == "USD"
        assert send.topic == TOPICS[payload["channel"]] and send.key == payload["order_id"]
        assert re.fullmatch(r"\d+\.\d{2}", payload["amount"])
        assert payload["stream_run_id"] == config.stream_run_id
        ms(payload["event_ts"])


def test_short_stream_is_rejected_instead_of_silently_dropping_late_events():
    config = StreamConfig(events_per_channel=500, events_per_minute=60)
    with pytest.raises(ValueError, match="too short"):
        generate(config)


class FakeFuture:
    def add_errback(self, callback):
        return self


class FakeProducer:
    def __init__(self, sink):
        self.sink = sink

    def send(self, topic, key, value):
        self.sink.append((topic, key, value))
        return FakeFuture()

    def flush(self):
        pass

    def close(self):
        pass


def test_publish_once_then_skip_and_refuse_interrupted(tmp_path):
    config = StreamConfig(events_per_channel=1500, events_per_minute=30, late_max_minutes=32)
    s3, sent = MemoryS3(), []
    first = publish_stream(config, tmp_path, s3, "raw", lambda: FakeProducer(sent))
    assert not first["skipped"] and len(sent) == first["records"]
    truth_key, marker_key = manifest_keys(config.stream_run_id)
    assert set(s3.puts) == {truth_key, marker_key}
    _, truth = generate(config)
    assert summarize(sent, config.stream_run_id) == expected_summary(truth)

    again = publish_stream(config, tmp_path, s3, "raw", lambda: pytest.fail("must not resend"))
    assert again["skipped"]

    del s3.objects[marker_key]
    with pytest.raises(RuntimeError, match="interrupted"):
        publish_stream(config, tmp_path, s3, "raw", lambda: FakeProducer([]))


def test_cli_writes_ground_truth_without_publishing(tmp_path):
    result = CliRunner().invoke(
        app, ["stream", "--config", str(SMALL), "--output", str(tmp_path), "--seed", "3"]
    )
    assert result.exit_code == 0, result.exception
    (path,) = (tmp_path / "stream").glob("*/stream_ground_truth.json")
    assert json.loads(path.read_text())["config"]["seed"] == 3


def test_flink_rules_match_generator_constants():
    """Rules.java and config.py must agree, or ground truth and Flink classify differently."""
    from generators.stream import config

    java = Path("flink/jobs/src/main/java/omnichannel/stream/common/Rules.java").read_text()
    values = {}
    for name, expr in re.findall(r"static final long (\w+) = ([^;]+);", java):
        values[name] = eval(expr.replace("L", ""), {})  # noqa: S307 - literal arithmetic only
    assert values["WATERMARK_BOUND_MS"] == config.WATERMARK_BOUND_MS
    assert values["AMAZON_WINDOW_MS"] == config.AMAZON_WINDOW_MS
    assert values["TIKTOK_WINDOW_MS"] == config.TIKTOK_WINDOW_MS
    assert values["AMAZON_ALLOWED_LATENESS_MS"] == config.ALLOWED_LATENESS_MS
    assert values["TIKTOK_ALLOWED_LATENESS_MS"] == config.TIKTOK_ALLOWED_LATENESS_MS
    assert values["DEDUP_TTL_MS"] == config.DEDUP_TTL_MS
    assert config.RETRY_MAX_MS < config.DEDUP_TTL_MS
    for event_ts in range(0, config.AMAZON_WINDOW_MS, 977):
        for watermark in (event_ts - 1, event_ts + 299_999, event_ts + 1_499_999, event_ts + 10**8):
            window, lateness = config.EVENT_TIME["amazon"]
            got = config.classify(event_ts, watermark, window, lateness)
            max_ts = event_ts - event_ts % window + window - 1
            want = (
                "on_time"
                if watermark < max_ts
                else "late_accepted" if watermark < max_ts + lateness else "too_late"
            )
            assert got == want
