"""Unit tests for benchmark config validation, trial planning and statistics (ADR-0012)."""

import json

import pytest

from spark.lib import benchmark_skew as bs
from spark.lib.benchmark_events import plan_features, query_plans, summarize_events

CONFIG = {
    "experiment_id": "exp-1",
    "datasets": ["d_control", "d_skew"],
    "workloads": ["w1_join_agg", "w2_agg"],
    "variants": ["B0", "B1T", "B3"],
    "w2_variants": ["B0", "B1T"],
    "repetitions": 3,
    "warmups": 1,
    "timeout_seconds": 600,
}


def test_variant_conf_changes_one_dimension_from_baseline():
    assert bs.variant_conf("B0") == bs.BASELINE_CONF
    b1t = bs.variant_conf("B1T")
    assert b1t["spark.sql.adaptive.enabled"] == "true"
    assert b1t["spark.sql.autoBroadcastJoinThreshold"] == "-1"
    assert bs.variant_conf("B4_64")["spark.sql.shuffle.partitions"] == "64"
    assert bs.variant_conf("B2") == bs.BASELINE_CONF and bs.VARIANTS["B2"]["broadcast"]
    assert bs.variant_conf("B3") == bs.BASELINE_CONF and bs.VARIANTS["B3"]["salt"] == 8
    assert bs.variant_conf("P0") == {}


@pytest.mark.parametrize(
    "change, message",
    [
        ({"experiment_id": "../x"}, "experiment_id"),
        ({"datasets": []}, "datasets"),
        ({"datasets": ["Bad Name"]}, "dataset name"),
        ({"workloads": ["w9"]}, "workloads"),
        ({"variants": ["B1"]}, "B0"),
        ({"variants": ["B0", "B9"]}, "known variants"),
        ({"w2_variants": ["B0", "B3"]}, "w2_variants"),
        ({"repetitions": 0}, "repetitions"),
        ({"repetitions": True}, "repetitions"),
        ({"timeout_seconds": 10}, "timeout_seconds"),
        ({"surprise": 1}, "unknown"),
    ],
)
def test_validate_experiment_rejects_bad_values(change, message):
    with pytest.raises(ValueError, match=message):
        bs.validate_experiment({**CONFIG, **change})


def test_trial_plan_runs_b0_first_and_rotates_measured_order():
    config = bs.validate_experiment(CONFIG)
    plan = bs.trial_plan(config)
    w1 = [t for t in plan if t["dataset"] == "d_control" and t["workload"] == "w1_join_agg"]

    assert [t["variant"] for t in w1 if t["phase"] == "warmup"] == ["B0", "B1T", "B3"]
    measured = [t["variant"] for t in w1 if t["phase"] == "measure"]
    assert measured == ["B0", "B1T", "B3", "B1T", "B3", "B0", "B3", "B0", "B1T"]
    assert len(plan) == 2 * (3 + 2) * (1 + 3)
    assert len({t["trial_id"] for t in plan}) == len(plan)
    assert all(t["variant"] in ("B0", "B1T") for t in plan if t["workload"] == "w2_agg")


def test_validate_dataset_spec_and_table_names():
    spec = bs.validate_dataset_spec(
        {"name": "l1m_skew80", "source_run": "b1e64afaff0176de180a", "factor": 67, "hot_share": 0.8}
    )
    assert spec["hot_share"] == 0.8
    with pytest.raises(ValueError, match="amplification"):
        bs.validate_dataset_spec({**spec, "factor": 0})
    with pytest.raises(ValueError, match="hot_share"):
        bs.validate_dataset_spec({**spec, "hot_share": 1.5})
    with pytest.raises(ValueError, match="source_run"):
        bs.validate_dataset_spec({**spec, "source_run": "x; drop"})
    assert bs.table_name("res", "matrix-1m", "l1m", "w1_join_agg", "B4_64") == (
        "lh.benchmark.res_matrix_1m_l1m_w1_join_agg_b4_64"
    )
    with pytest.raises(ValueError):
        bs.table_name("gold", "x")


def test_redact_conf_keeps_only_allowlisted_keys():
    conf = {
        "spark.sql.shuffle.partitions": "16",
        "spark.sql.catalog.lh.s3.secret-access-key": "s3cr3t",
        "spark.sql.catalog.lh.uri": "http://lakekeeper:8181/catalog",
        "spark.sql.adaptive.enabled": "false",
    }
    assert bs.redact_conf(conf) == {
        "spark.sql.shuffle.partitions": "16",
        "spark.sql.adaptive.enabled": "false",
    }
    assert bs.redact_conf(list(conf.items())) == bs.redact_conf(conf)


def test_preserve_previous_attempt_never_overwrites(tmp_path):
    path = tmp_path / "t.json"
    assert bs.preserve_previous_attempt(path) is None
    bs.write_json_atomic(path, {"n": 1})
    first = bs.preserve_previous_attempt(path)
    bs.write_json_atomic(path, {"n": 2})
    second = bs.preserve_previous_attempt(path)
    assert first.name == "t.attempt1.json" and second.name == "t.attempt2.json"
    assert json.loads(first.read_text()) == {"n": 1}
    assert not path.exists()


def trial(variant, rep, query_s, *, status="ok", correct=True, ratio=2.0, phase="measure"):
    return {
        "dataset": "d",
        "workload": "w1_join_agg",
        "variant": variant,
        "phase": phase,
        "repetition": rep,
        "status": status,
        "correct": correct,
        "timings": {"query_s": query_s},
        "metrics": {
            "worst_shuffle_stage_ratio": ratio,
            "shuffle_bytes_written": 100,
            "disk_bytes_spilled": None,
        },
    }


def test_summarize_trials_reports_median_speedup_and_keeps_failures():
    trials = [
        trial("B0", 0, 10.0),
        trial("B0", 1, 12.0),
        trial("B0", 2, 11.0),
        trial("B0", 0, 99.0, phase="warmup"),
        trial("B3", 0, 5.0, ratio=1.1),
        trial("B3", 1, 6.0, ratio=1.3),
        trial("B3", 2, 4.0, status="correctness_failed", correct=False),
        trial("B2", 0, 1.0, status="timeout", correct=False),
    ]
    rows = {r["variant"]: r for r in bs.summarize_trials(trials)}

    assert rows["B0"]["trials"] == 3 and rows["B0"]["query_s_median"] == 11.0
    assert rows["B0"]["query_s_min"] == 10.0 and rows["B0"]["query_s_max"] == 12.0
    assert rows["B3"]["ok_trials"] == 2 and rows["B3"]["failed_trials"] == 1
    assert rows["B3"]["query_s_median"] == 5.5
    assert rows["B3"]["speedup_vs_b0"] == round(11.0 / 5.5, 3)
    assert rows["B3"]["statuses"] == ["correctness_failed", "ok"]
    assert rows["B2"]["query_s_median"] is None and rows["B2"]["speedup_vs_b0"] is None
    assert rows["B0"]["disk_bytes_spilled_median"] is None
    assert rows["B3"]["shuffle_task_ratio_median"] == 1.2


def test_event_filter_by_job_description_and_plan_extraction():
    events = [
        {"Event": "SparkListenerApplicationStart", "Timestamp": 0},
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 0,
            "Stage IDs": [0],
            "Properties": {"spark.job.description": "bench:setup"},
        },
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 1,
            "Stage IDs": [1, 2],
            "Properties": {"spark.job.description": "bench:query"},
        },
        {
            "Event": bs_sql_start(),
            "executionId": 3,
            "description": "bench:query",
            "physicalPlanDescription": "AdaptiveSparkPlan isFinalPlan=false SortMergeJoin",
        },
        {
            "Event": bs_sql_update(),
            "executionId": 3,
            "physicalPlanDescription": "AdaptiveSparkPlan isFinalPlan=true SortMergeJoin(skew=true)"
            " AQEShuffleRead skewed",
        },
    ]
    for stage_id in (0, 1, 2):
        events.append(
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": stage_id,
                "Stage Attempt ID": 0,
                "Task End Reason": {"Reason": "Success"},
                "Task Info": {"Index": 0, "Attempt": 0, "Launch Time": 1, "Finish Time": 11},
                "Task Metrics": {"Executor Run Time": 10},
            }
        )
        events.append(
            {
                "Event": "SparkListenerStageCompleted",
                "Stage Info": {"Stage ID": stage_id, "Stage Attempt ID": 0},
            }
        )
    events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": 100})
    lines = [json.dumps(e) for e in events]

    summary = summarize_events(lines, "bench:query")
    assert [s["stage_id"] for s in summary["stages"]] == [1, 2]
    assert summary["matched_jobs"] == 1
    assert summary["totals"]["executor_run_time_ms"] == 20
    assert len(summarize_events(lines)["stages"]) == 3

    (plan,) = query_plans(lines, "bench:query")
    assert plan_features(plan["initial"])["skew_join"] is False
    final = plan_features(plan["final"])
    assert final["skew_join"] and final["aqe_shuffle_read"] and final["sort_merge_join"]
    assert not final["broadcast_hash_join"]
    assert query_plans(lines, "bench:verify") == []


def bs_sql_start():
    return "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"


def bs_sql_update():
    return "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"


FORMATTED_AQE = """== Physical Plan ==
AdaptiveSparkPlan (20)
+- == Final Plan ==
   * HashAggregate (9)
   +- * SortMergeJoin(skew=true) Inner (8)
      :- * Sort (3)
      :  +- AQEShuffleRead (2)
      :     +- ShuffleQueryStage (1)
      :        +- Exchange (0)
+- == Initial Plan ==
   HashAggregate (19)
   +- BroadcastHashJoin Inner BuildRight (18)
      +- BroadcastExchange (17)

(1) BatchScan lh.benchmark.items_x
Arguments: BroadcastHashJoin hashpartitioning(product_key)
"""


def test_plan_features_use_final_tree_only():
    from spark.lib.benchmark_events import plan_tree

    features = plan_features(FORMATTED_AQE)
    assert features["skew_join"] and features["sort_merge_join"] and features["aqe_shuffle_read"]
    assert features["shuffle_exchange"]
    assert not features["broadcast_hash_join"] and not features["broadcast_exchange"]
    assert "BatchScan" not in plan_tree(FORMATTED_AQE)
    not_final = plan_features("AdaptiveSparkPlan\n+- BroadcastExchange (2)\n\n(1) Exchange")
    assert not_final["broadcast_exchange"] and not not_final["shuffle_exchange"]
