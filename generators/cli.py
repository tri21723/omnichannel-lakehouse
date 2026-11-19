"""Batch/load/stream CLI. Logs never contain payloads or credentials."""

import json
import logging
import os
from pathlib import Path
from typing import Annotated

import typer

from generators.batch.dataset import generate, verify_local
from generators.cdc.apply import apply_scenario, check_source
from generators.cdc.scenario import CdcConfig
from generators.common.config import read_config
from generators.common.storage import publish, s3_client, verify_remote
from generators.load.postgres import connect, load
from generators.stream.close import close_stream
from generators.stream.config import read_stream_config
from generators.stream.events import generate as generate_stream
from generators.stream.publish import check_stream, kafka_producer, publish_stream, write_json
from generators.stream.verify import verify_stream

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
log = logging.getLogger("generators")
ConfigPath = Annotated[Path, typer.Option(exists=True, dir_okay=False)]


def event(name: str, **fields: object) -> None:
    log.info(json.dumps({"event": name, **fields}, sort_keys=True))


def dataset(
    config: Path, output: Path, seed: int | None, start_date: str | None, days: int | None
) -> Path:
    cfg = read_config(config, seed=seed, start_date=start_date, days=days)
    root = generate(cfg, output)
    event("dataset_ready", run_id=cfg.run_id)
    return root


@app.command()
def batch(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
    upload: bool = False,
) -> None:
    """Generate JSONL plus manifest; optionally publish to S3."""
    root = dataset(config, output, seed, start_date, days)
    if upload:
        manifest = publish(root, s3_client(), os.environ["S3_RAW_BUCKET"])
        event(
            "s3_published",
            run_id=manifest["run_id"],
            rows={name: data["rows"] for name, data in manifest["channels"].items()},
        )


@app.command("load")
def load_command(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
) -> None:
    """Load unique Walmart entities from the generated JSONL into PostgreSQL."""
    root = dataset(config, output, seed, start_date, days)
    with connect() as connection:
        result = load(root, connection)
    event("walmart_loaded", run_id=root.name, tables=result)


@app.command()
def sources(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
) -> None:
    """Publish batch and load Walmart using the same dataset."""
    batch(config, output, seed, start_date, days, True)
    load_command(config, output, seed, start_date, days)


@app.command()
def verify(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
    remote: bool = False,
    database: bool = False,
) -> None:
    """Reconcile existing data; never regenerate or upsert business records."""
    cfg = read_config(config, seed=seed, start_date=start_date, days=days)
    root = output / cfg.run_id
    verify_local(root)
    if remote:
        verify_remote(root, s3_client(), os.environ["S3_RAW_BUCKET"])
    if database:
        with connect() as connection:
            result = load(root, connection, verify_only=True)
        event("walmart_verified", run_id=cfg.run_id, tables=result)
    event("reconciliation_passed", run_id=cfg.run_id, remote=remote, database=database)


@app.command()
def stream(
    config: ConfigPath = Path("generators/stream.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    events_per_channel: int | None = None,
    publish: bool = False,
    rate: float | None = None,
) -> None:
    """Generate an order-event stream and its ground truth; optionally publish to Kafka once."""
    cfg = read_stream_config(config, seed=seed, events_per_channel=events_per_channel)
    if not publish:
        _, truth = generate_stream(cfg)
        write_json(output / "stream" / cfg.stream_run_id / "stream_ground_truth.json", truth)
        event("stream_ready", run_id=cfg.stream_run_id, records=truth["records_total"])
        return
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    result = publish_stream(
        cfg,
        output,
        s3_client(),
        os.environ["S3_RAW_BUCKET"],
        lambda: kafka_producer(bootstrap),
        rate=rate,
    )
    name = "stream_publish_skipped" if result["skipped"] else "stream_published"
    event(
        name,
        run_id=result["stream_run_id"],
        records=result["records"],
        seconds=result.get("seconds"),
    )


@app.command("stream-check")
def stream_check(
    config: ConfigPath = Path("generators/stream.yaml"),
    seed: int | None = None,
    events_per_channel: int | None = None,
) -> None:
    """Read the run back from Kafka and compare it with the ground truth (read-only)."""
    cfg = read_stream_config(config, seed=seed, events_per_channel=events_per_channel)
    result = check_stream(cfg, os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    event("stream_checked", run_id=cfg.stream_run_id, ok=result["ok"], topics=result["actual"])
    if not result["ok"]:
        raise SystemExit(1)


@app.command("stream-verify")
def stream_verify(
    config: ConfigPath = Path("generators/stream.yaml"),
    seed: int | None = None,
    events_per_channel: int | None = None,
    failover: bool = False,
) -> None:
    """Reconcile committed Flink output and DLQs with the ground truth (read-only).

    --failover: the run restored from a checkpoint, so exact class counts are not expected
    (watermarks are not checkpointed); completeness and exactly-once are still required."""
    cfg = read_stream_config(config, seed=seed, events_per_channel=events_per_channel)
    result = verify_stream(
        cfg.stream_run_id,
        s3_client(),
        os.environ["S3_RAW_BUCKET"],
        os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        failover=failover,
    )
    for check in result["checks"]:
        event("stream_verify_check", run_id=cfg.stream_run_id, **check)
    failed = sum(not c["ok"] for c in result["checks"])
    event(
        "stream_verified",
        run_id=cfg.stream_run_id,
        ok=result["ok"],
        failures=failed,
        failover=failover,
    )
    if not result["ok"]:
        raise SystemExit(1)


@app.command("stream-close")
def stream_close(
    config: ConfigPath = Path("generators/stream.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    events_per_channel: int | None = None,
) -> None:
    """Write the immutable stream_output_manifest.json for a stopped run (Bronze input)."""
    cfg = read_stream_config(config, seed=seed, events_per_channel=events_per_channel)
    result = close_stream(cfg.stream_run_id, s3_client(), os.environ["S3_RAW_BUCKET"], output)
    event("stream_closed", run_id=cfg.stream_run_id, files=result["files"], rows=result["rows"])


def read_cdc_config(path: Path, seed: int | None) -> CdcConfig:
    import yaml

    content = yaml.safe_load(path.read_text())
    if seed is not None:
        content["seed"] = seed
    return CdcConfig.model_validate(content)


@app.command()
def cdc(
    config: ConfigPath = Path("generators/cdc.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    apply: bool = False,
) -> None:
    """Build a CDC mutation scenario; with --apply run it once on PostgreSQL (role generator)."""
    cfg = read_cdc_config(config, seed)
    if not apply:
        from generators.cdc.scenario import build

        _, truth = build(cfg)
        write_json(output / "cdc" / cfg.cdc_run_id / "cdc_ground_truth.json", truth)
        event("cdc_scenario_ready", run_id=cfg.cdc_run_id, ops=truth["expected_ops"])
        return
    result = apply_scenario(cfg, output, s3_client(), os.environ["S3_RAW_BUCKET"], connect)
    name = "cdc_scenario_skipped" if result["skipped"] else "cdc_scenario_applied"
    event(name, run_id=result["cdc_run_id"], transactions=result["transactions"])


@app.command("cdc-check")
def cdc_check(
    config: ConfigPath = Path("generators/cdc.yaml"),
    seed: int | None = None,
) -> None:
    """Compare the scenario rows in PostgreSQL with the ground truth (read-only)."""
    cfg = read_cdc_config(config, seed)
    result = check_source(cfg, connect)
    event("cdc_source_checked", run_id=cfg.cdc_run_id, **result)
    if not result["ok"]:
        raise SystemExit(1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)  # client chatter is not an event
    try:
        app()
    except Exception as exc:
        # Driver exception messages may contain connection details; do not log them.
        event("generator_failed", error_type=type(exc).__name__)
        raise SystemExit(1) from None
