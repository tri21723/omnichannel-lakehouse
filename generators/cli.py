"""Batch/load/stream CLI. Logs never contain payloads or credentials."""

import json
import logging
import os
from pathlib import Path
from typing import Annotated

import typer

from generators.batch.dataset import generate, verify_local
from generators.common.config import read_config
from generators.common.storage import publish, s3_client, verify_remote
from generators.load.postgres import connect, load
from generators.stream.config import read_stream_config
from generators.stream.events import generate as generate_stream
from generators.stream.publish import check_stream, kafka_producer, publish_stream, write_json

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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)  # client chatter is not an event
    try:
        app()
    except Exception as exc:
        # Driver exception messages may contain connection details; do not log them.
        event("generator_failed", error_type=type(exc).__name__)
        raise SystemExit(1) from None
