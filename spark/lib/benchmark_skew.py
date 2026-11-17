"""Pure-Python benchmark definitions for Phase 4F (ADR-0012): variants, config, statistics.

No pyspark import here, so validation and reporting are unit-testable without a cluster.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import statistics
from collections.abc import Iterable
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
RUNTIME_ROOT = Path(os.environ.get("BENCHMARK_ROOT", "/runtime/benchmark"))
NAMESPACE = "lh.benchmark"
WORKLOADS = ("w1_join_agg", "w2_agg")
DATASET_NAME = re.compile(r"^[a-z0-9_]{1,40}$")
EXPERIMENT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")

# B0: AQE off, broadcast off, fixed shuffle partitions (ADR-0012 §4).
BASELINE_CONF: dict[str, str] = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "16",
}

# Each variant changes one dimension from B0. ``conf`` replaces BASELINE_CONF keys;
# ``baseline=False`` means "start from production defaults" (only P0).
VARIANTS: dict[str, dict[str, Any]] = {
    "B0": {"conf": {}, "description": "baseline: AQE off, broadcast off, 16 partitions"},
    "B1": {
        "conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
        },
        "description": "AQE + skew join, default skew thresholds (256MB / factor 5)",
    },
    "B1T": {
        "conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "4m",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "4m",
        },
        "description": "AQE + skew join, thresholds tuned to local data size",
    },
    "B2": {"conf": {}, "broadcast": True, "description": "broadcast hint on dim_product"},
    "B3": {"conf": {}, "salt": 8, "description": "salting N=8 on order_key, two-step agg"},
    "B4_32": {"conf": {"spark.sql.shuffle.partitions": "32"}, "description": "32 partitions"},
    "B4_64": {"conf": {"spark.sql.shuffle.partitions": "64"}, "description": "64 partitions"},
    "P0": {
        "conf": {},
        "baseline": False,
        "description": "production session defaults (AQE default on, 10MB broadcast)",
    },
}
# Variants that are meaningful for the aggregate-only workload (no join to broadcast/salt).
W2_VARIANTS = ("B0", "B1", "B1T", "B4_32", "B4_64", "P0")

# Effective-config keys allowed into artifacts/reports; everything else is dropped.
CONF_ALLOWLIST = (
    "spark.app.id",
    "spark.cores.max",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.driver.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.speculation",
    "spark.eventLog.enabled",
    "spark.eventLog.compress",
)

LIMITS = {
    "repetitions": (1, 10),
    "warmups": (0, 3),
    "timeout_seconds": (60, 3600),
    "salt": (2, 64),
    "amplification": (1, 200),
}


def validate_dataset_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Validate a benchmark dataset spec (synthetic amplification of a pinned Gold run)."""
    allowed = {"name", "source_run", "factor", "hot_share", "description"}
    unknown = set(spec) - allowed
    if unknown:
        raise ValueError(f"unknown dataset keys: {sorted(unknown)}")
    name = spec.get("name")
    if not isinstance(name, str) or not DATASET_NAME.fullmatch(name):
        raise ValueError(f"invalid dataset name: {name!r}")
    source_run = spec.get("source_run")
    if not isinstance(source_run, str) or not re.fullmatch(r"[0-9a-f]{20}", source_run):
        raise ValueError(f"invalid source_run: {source_run!r}")
    share = spec.get("hot_share")
    if isinstance(share, bool) or not isinstance(share, int | float) or not 0 <= share <= 1:
        raise ValueError(f"hot_share must be in [0, 1], got {share!r}")
    return {
        "name": name,
        "source_run": source_run,
        "factor": _check_range("amplification", spec.get("factor")),
        "hot_share": float(share),
        "description": str(spec.get("description", ""))[:200],
    }


def table_name(kind: str, *parts: str) -> str:
    """Iceberg table in the benchmark namespace; parts are sanitized to [a-z0-9_]."""
    if kind not in ("items", "products", "ref", "res"):
        raise ValueError(f"unknown benchmark table kind: {kind}")
    safe = "_".join(re.sub(r"[^a-z0-9_]", "_", part.lower()) for part in parts)
    return f"{NAMESPACE}.{kind}_{safe}"


def dataset_manifest_path(name: str, root: Path | None = None) -> Path:
    return (root or RUNTIME_ROOT) / "datasets" / f"{name}.json"


def experiment_dir(experiment_id: str, root: Path | None = None) -> Path:
    return (root or RUNTIME_ROOT) / "experiments" / experiment_id


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)


def preserve_previous_attempt(path: Path) -> Path | None:
    """Rename an existing trial artifact to ``<name>.attemptN.json`` instead of overwriting."""
    if not path.exists():
        return None
    attempt = 1
    while (target := path.with_name(f"{path.stem}.attempt{attempt}.json")).exists():
        attempt += 1
    os.replace(path, target)
    return target


def variant_conf(name: str) -> dict[str, str]:
    """Spark conf for a variant (B0 conf plus its overrides; P0 adds nothing)."""
    spec = VARIANTS[name]
    if spec.get("baseline", True) is False:
        return {}
    return {**BASELINE_CONF, **spec["conf"]}


def _check_range(name: str, value: Any) -> int:
    low, high = LIMITS[name]
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{name} must be an integer in [{low}, {high}], got {value!r}")
    return value


def validate_experiment(config: dict[str, Any]) -> dict[str, Any]:
    """Validate an experiment config against allowlists and ranges; return a normalized copy."""
    allowed = {
        "experiment_id",
        "datasets",
        "workloads",
        "variants",
        "w2_variants",
        "repetitions",
        "warmups",
        "timeout_seconds",
        "description",
    }
    unknown = set(config) - allowed
    if unknown:
        raise ValueError(f"unknown experiment keys: {sorted(unknown)}")
    experiment_id = config.get("experiment_id")
    if not isinstance(experiment_id, str) or not EXPERIMENT_ID.fullmatch(experiment_id):
        raise ValueError(f"invalid experiment_id: {experiment_id!r}")
    datasets = config.get("datasets")
    if not datasets or not isinstance(datasets, list):
        raise ValueError("datasets must be a non-empty list")
    for name in datasets:
        if not isinstance(name, str) or not DATASET_NAME.fullmatch(name):
            raise ValueError(f"invalid dataset name: {name!r}")
    workloads = config.get("workloads", list(WORKLOADS))
    if not workloads or any(w not in WORKLOADS for w in workloads):
        raise ValueError(f"workloads must be a subset of {WORKLOADS}")
    variants = config.get("variants", list(VARIANTS))
    if "B0" not in variants or any(v not in VARIANTS for v in variants):
        raise ValueError("variants must include B0 and only known variants")
    w2_variants = config.get("w2_variants", [v for v in W2_VARIANTS if v in variants])
    if "B0" not in w2_variants or any(v not in W2_VARIANTS for v in w2_variants):
        raise ValueError(f"w2_variants must include B0 and be a subset of {W2_VARIANTS}")
    return {
        "experiment_id": experiment_id,
        "description": str(config.get("description", ""))[:200],
        "datasets": list(dict.fromkeys(datasets)),
        "workloads": list(dict.fromkeys(workloads)),
        "variants": list(dict.fromkeys(variants)),
        "w2_variants": list(dict.fromkeys(w2_variants)),
        "repetitions": _check_range("repetitions", config.get("repetitions", 3)),
        "warmups": _check_range("warmups", config.get("warmups", 1)),
        "timeout_seconds": _check_range("timeout_seconds", config.get("timeout_seconds", 900)),
    }


def cells(config: dict[str, Any]) -> list[tuple[str, str, str]]:
    """All (dataset, workload, variant) cells in canonical order; B0 first per group."""
    result = []
    for dataset in config["datasets"]:
        for workload in config["workloads"]:
            names = config["variants"] if workload == "w1_join_agg" else config["w2_variants"]
            ordered = ["B0"] + [v for v in names if v != "B0"]
            result.extend((dataset, workload, variant) for variant in ordered)
    return result


def trial_plan(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Warm-ups in canonical order (B0 first creates the reference), then measured rounds
    with the variant order rotated per repetition inside each (dataset, workload) group."""
    plan = []
    groups: dict[tuple[str, str], list[str]] = {}
    for dataset, workload, variant in cells(config):
        groups.setdefault((dataset, workload), []).append(variant)
    for (dataset, workload), variants in groups.items():
        for warmup in range(config["warmups"]):
            for variant in variants:
                plan.append(_trial(dataset, workload, variant, "warmup", warmup))
        for rep in range(config["repetitions"]):
            shift = rep % len(variants)
            for variant in variants[shift:] + variants[:shift]:
                plan.append(_trial(dataset, workload, variant, "measure", rep))
    return plan


def _trial(dataset: str, workload: str, variant: str, phase: str, rep: int) -> dict[str, Any]:
    trial_id = f"{dataset}.{workload}.{variant}.{phase}{rep}"
    return {
        "trial_id": trial_id,
        "dataset": dataset,
        "workload": workload,
        "variant": variant,
        "phase": phase,
        "repetition": rep,
    }


def redact_conf(conf: Iterable[tuple[str, str]] | dict[str, str]) -> dict[str, str]:
    """Keep only allowlisted Spark conf keys (never credentials, URIs or SQL)."""
    items = conf.items() if isinstance(conf, dict) else conf
    return {key: value for key, value in items if key in CONF_ALLOWLIST}


def config_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def summarize_trials(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Per-cell statistics over measured trials; failures are kept and counted, not hidden.

    Speedup = B0 median query seconds / variant median, only when both cells have at least
    one ok trial. ``ok`` requires status ok and a passing correctness gate.
    """
    by_cell: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for trial in trials:
        if trial.get("phase") != "measure":
            continue
        key = (trial["dataset"], trial["workload"], trial["variant"])
        by_cell.setdefault(key, []).append(trial)

    rows = []
    for (dataset, workload, variant), group in sorted(by_cell.items()):
        ok = [t for t in group if t.get("status") == "ok" and t.get("correct") is True]
        query = sorted(t["timings"]["query_s"] for t in ok)
        ratios = [t["metrics"].get("worst_shuffle_stage_ratio") for t in ok if t.get("metrics")]
        ratios = sorted(r for r in ratios if r is not None)
        row = {
            "dataset": dataset,
            "workload": workload,
            "variant": variant,
            "trials": len(group),
            "ok_trials": len(ok),
            "failed_trials": len(group) - len(ok),
            "statuses": sorted({t.get("status", "unknown") for t in group}),
            "query_s_median": round(statistics.median(query), 3) if query else None,
            "query_s_min": round(query[0], 3) if query else None,
            "query_s_max": round(query[-1], 3) if query else None,
            "shuffle_task_ratio_median": round(statistics.median(ratios), 3) if ratios else None,
        }
        for metric in (
            "max_shuffle_task_ms",
            "shuffle_bytes_written",
            "shuffle_remote_bytes_read",
            "shuffle_local_bytes_read",
            "memory_bytes_spilled",
            "disk_bytes_spilled",
            "failed_tasks",
        ):
            values = [t["metrics"].get(metric) for t in ok if t.get("metrics")]
            values = [v for v in values if v is not None]
            row[f"{metric}_median"] = statistics.median(values) if values else None
        rows.append(row)

    baseline = {
        (r["dataset"], r["workload"]): r["query_s_median"] for r in rows if r["variant"] == "B0"
    }
    for row in rows:
        base = baseline.get((row["dataset"], row["workload"]))
        mine = row["query_s_median"]
        row["speedup_vs_b0"] = round(base / mine, 3) if base and mine else None
    return rows
