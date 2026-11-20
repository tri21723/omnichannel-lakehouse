"""DagBag and DAG contract tests (ADR-0013 §6). Run with `make test-dags` in the Airflow image."""

import pytest

# The repo directory `airflow/` is a namespace package, so probe a real Airflow module.
pytest.importorskip("airflow.configuration")

from datetime import timedelta  # noqa: E402

from airflow.configuration import conf  # noqa: E402
from airflow.models import DagBag  # noqa: E402

JOB_DAGS = {
    "dp1_bronze",
    "dp2_silver",
    "dp2_gold",
    "dp3_features",
    "dp3_return_labels",
    "dp4_warehouse",
    "dp1_stream_bronze",
}
RUN = "b1e64afaff0176de180a"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=conf.get("core", "dags_folder"))


def render(task, ds="2026-01-02", run=RUN):
    return task.render_template(task.bash_command, {"params": {"dataset_run_id": run}, "ds": ds})


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == JOB_DAGS | {
        "batch_daily",
        "maintenance_iceberg",
        "query_checks",
        "dp1_cdc",
        "datahub_ingest",
    }


def test_every_dag_follows_defaults(dagbag):
    for dag_id in dagbag.dag_ids:
        dag = dagbag.dags[dag_id]
        assert dag.max_active_runs == 1, dag_id
        assert dag.catchup is False, dag_id
        assert "omnichannel" in dag.tags, dag_id
        if dag_id not in ("maintenance_iceberg", "query_checks", "dp1_cdc", "datahub_ingest"):
            assert dag.params.get_param("dataset_run_id").value == RUN
        for task in dag.tasks:
            assert task.owner == "data-platform", (dag_id, task.task_id)
            assert task.on_failure_callback, (dag_id, task.task_id)


def test_spark_tasks_use_pool_timeout_retries_and_lock(dagbag):
    for dag_id in JOB_DAGS:
        dag = dagbag.dags[dag_id]
        assert dag.schedule is None or str(dag.timetable.summary) in ("None", "")
        spark = [t for t in dag.tasks if hasattr(t, "bash_command")]
        assert spark, dag_id
        for task in spark:
            assert task.pool == "spark_driver", task.task_id
            assert task.retries >= 1 and task.retry_delay == timedelta(minutes=2)
            assert timedelta(0) < task.execution_timeout <= timedelta(minutes=45)
            command = render(task)
            assert (
                "python -m spark.lib.driver_lock --wait 300 -- /opt/spark/bin/spark-submit"
                in command
            )
            assert "ci_events.py" in command and 'exit "$rc"' in command
            assert "{{" not in command


def test_rendered_commands_match_job_cli(dagbag):
    bronze = dagbag.dags["dp1_bronze"]
    assert [t.task_id for t in bronze.topological_sort()] == [
        "wait_for_manifest",
        "bronze_walmart_jsonl",
        "bronze_amazon_jsonl",
        "bronze_tiktok_shop_jsonl",
        "bronze_shopee_jsonl",
        "bronze_lazada_jsonl",
        "bronze_walmart_jdbc",
    ]
    jdbc = render(bronze.get_task("bronze_walmart_jdbc"))
    assert (
        f"dp1_bronze/main.py --run-id {RUN} --run-date 2026-01-02 --mode incremental "
        "--channel walmart --source jdbc"
    ) in jdbc
    silver = render(dagbag.dags["dp2_silver"].get_task("silver_shopee"))
    assert "--channel shopee --mode incremental --fail-on-reject" in silver
    gold = render(dagbag.dags["dp2_gold"].get_task("gold_publish"))
    assert f"gold_main.py --run-id {RUN} --mode full" in gold
    labels = render(dagbag.dags["dp3_return_labels"].get_task("labels_publish"))
    # Daily DP3 must be incremental: `full` would replace every other date of the run.
    assert "--as-of-date 2026-01-02 --horizon-days 14 --mode incremental" in labels
    features = render(dagbag.dags["dp3_features"].get_task("features_publish"))
    assert "--as-of-date 2026-01-02 --mode incremental" in features
    export = render(dagbag.dags["dp4_warehouse"].get_task("warehouse_export"))
    assert "--run-date 2026-01-02 --mode full --no-regress" in export
    assert dagbag.dags["dp4_warehouse"].get_task("warehouse_export").execution_timeout == (
        timedelta(minutes=45)
    )


def test_manifest_sensor_does_not_hold_a_slot(dagbag):
    sensor = dagbag.dags["dp1_bronze"].get_task("wait_for_manifest")
    assert sensor.mode == "reschedule"
    assert sensor.pool != "spark_driver"
    assert sensor.timeout == 1800


def test_controller_chains_jobs_in_order_without_holding_slots(dagbag):
    controller = dagbag.dags["batch_daily"]
    order = [t.trigger_dag_id for t in controller.topological_sort()]
    assert order == [
        "dp1_bronze",
        "dp2_silver",
        "dp2_gold",
        "dp3_features",
        "dp3_return_labels",
        "dp4_warehouse",
    ]
    for task in controller.tasks:
        assert task.deferrable and task.wait_for_completion and task.reset_dag_run
        # A failed date must block later dates in a backfill.
        assert task.depends_on_past
        assert task.pool != "spark_driver"
    assert controller.start_date.isoformat().startswith("2026-01-01")
    assert controller.end_date.isoformat().startswith("2026-01-03")


def test_dataset_param_rejects_unsafe_values(dagbag):
    from airflow.sdk.definitions.param import ParamValidationError

    param = dagbag.dags["dp2_gold"].params.get_param("dataset_run_id")
    assert param.resolve(RUN) == RUN
    for bad in ("b1e64afaff0176de180", "'; rm -rf / #", "B1E64AFAFF0176DE180A"):
        with pytest.raises(ParamValidationError):
            param.resolve(bad)


def test_last_stage_tasks_emit_layer_assets(dagbag):
    expected = {
        ("dp1_bronze", "bronze_walmart_jdbc"): "lh.bronze",
        ("dp2_silver", "silver_lazada"): "lh.silver",
        ("dp2_gold", "gold_verify"): "lh.gold.star_schema",
        ("dp3_features", "features_verify"): "lh.gold.features",
        ("dp3_return_labels", "labels_verify"): "lh.gold.labels",
        ("dp4_warehouse", "warehouse_verify"): "sources.warehouse",
    }
    for (dag_id, task_id), name in expected.items():
        outlets = dagbag.dags[dag_id].get_task(task_id).outlets
        assert [a.name for a in outlets] == [name], (dag_id, task_id)
    # Only the read-only query_checks DAG is scheduled on an asset.
    for dag_id, dag in dagbag.dags.items():
        asset_scheduled = "Asset" in type(dag.timetable).__name__
        assert asset_scheduled == (dag_id == "query_checks"), dag_id


def test_query_checks_is_read_only_and_data_aware(dagbag):
    dag = dagbag.dags["query_checks"]
    (task,) = dag.tasks
    assert task.pool != "spark_driver"
    assert task.python_callable.__name__ == "reconcile_warehouse_window"
    assert task.execution_timeout.total_seconds() == 600


def test_maintenance_dag_is_report_only_and_paused(dagbag):
    dag = dagbag.dags["maintenance_iceberg"]
    (task,) = dag.tasks
    command = task.render_template(
        task.bash_command, {"data_interval_end": __import__("datetime").datetime(2026, 11, 22)}
    )
    assert "maintenance/iceberg_report.py --as-of 2026-11-22T00:00:00" in command
    assert task.pool == "spark_driver"
    for forbidden in ("expire_snapshots", "remove_orphan_files", "rewrite_data_files"):
        assert forbidden not in command


def test_stream_bronze_waits_for_closed_run_and_ingests_both_channels(dagbag):
    dag = dagbag.dags["dp1_stream_bronze"]
    assert [t.task_id for t in dag.topological_sort()] == [
        "wait_for_output_manifest",
        "bronze_amazon_stream",
        "bronze_tiktok_shop_stream",
    ]
    sensor = dag.get_task("wait_for_output_manifest")
    assert sensor.mode == "reschedule" and sensor.pool != "spark_driver"
    assert sensor.op_kwargs["name"] == "stream_output_manifest.json"
    command = render(dag.get_task("bronze_tiktok_shop_stream"), ds="2026-02-01")
    assert (
        f"dp1_bronze/main.py --run-id {RUN} --run-date 2026-02-01 --source stream "
        "--mode incremental --channel tiktok_shop"
    ) in command
    assert [a.name for a in dag.get_task("bronze_tiktok_shop_stream").outlets] == ["lh.bronze"]


def test_cdc_dag_is_scheduled_paused_and_uses_the_driver_lock(dagbag):
    dag = dagbag.dags["dp1_cdc"]
    assert type(dag.timetable).__name__ == "DeltaTriggerTimetable"  # every 10 minutes
    assert dag.is_paused_upon_creation is True
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert [t.task_id for t in dag.topological_sort()] == [
        "cdc_bronze",
        "cdc_silver",
        "cdc_silver_verify",
    ]
    for task in dag.tasks:
        assert task.pool == "spark_driver" and task.retries >= 1
        assert timedelta(0) < task.execution_timeout <= timedelta(minutes=15)
        command = task.render_template(task.bash_command, {"params": {}, "ds": "2026-11-19"})
        assert (
            "python -m spark.lib.driver_lock --wait 300 -- /opt/spark/bin/spark-submit" in command
        )
        assert "--run-date 2026-11-19 --channel walmart" in command and "{{" not in command
    assert "--mode verify" in dag.get_task("cdc_silver_verify").bash_command


def test_datahub_dag_is_paused_outside_the_driver_pool_and_uses_the_venv(dagbag):
    dag = dagbag.dags["datahub_ingest"]
    assert dag.is_paused_upon_creation is True
    assert [t.task_id for t in dag.topological_sort()] == [
        "ingest_postgres",
        "ingest_kafka",
        "ingest_iceberg",
        "emit_dataset_lineage",
        "emit_job_lineage",
    ]
    for task in dag.tasks:
        assert task.pool != "spark_driver" and task.retries >= 1
        assert "/opt/datahub-venv/bin/" in task.bash_command
        assert "env -u PYTHONPATH" in task.bash_command
        assert "spark-submit" not in task.bash_command
    assert "--strict-warnings" in dag.get_task("ingest_postgres").bash_command
    assert "--strict-warnings" not in dag.get_task("ingest_kafka").bash_command


def test_datahub_export_maps_every_spark_task_to_datasets(dagbag):
    import sys
    from pathlib import Path

    # Repo checkout (CI `dags` job) first; the Airflow image has the tooling at /app/datahub.
    repo = Path(__file__).resolve().parents[2] / "datahub"
    sys.path.insert(0, str(repo if (repo / "lineage").is_dir() else Path("/app/datahub")))
    from lineage import jobs, model
    from omnichannel.datahub_export import describe

    dags = describe(dagbag)
    assert [d["dag_id"] for d in dags] == sorted(dagbag.dag_ids)
    tasks = {(d["dag_id"], t["task_id"]): t for d in dags for t in d["tasks"]}
    assert tasks[("dp1_bronze", "bronze_walmart_jdbc")]["spark"] == {
        "script": "dp1_bronze/main.py",
        "args": {"--channel": "walmart", "--source": "jdbc", "--mode": "incremental"},
    }
    assert tasks[("dp2_gold", "gold_verify")]["spark"]["args"] == {"--verify": "true"}
    assert tasks[("datahub_ingest", "ingest_kafka")]["spark"] is None
    # Contract edges only (no ledgers needed): every publishing Spark task writes a dataset.
    edges = model.merge(model.stream_edges(), model.cdc_edges())
    for (dag_id, task_id), task in tasks.items():
        spark = task["spark"]
        if not spark or spark["script"].startswith("maintenance/"):
            continue
        inputs, outputs = jobs.spark_io(spark["script"], spark["args"], edges)
        if "verify" in (spark["args"].get("--mode"), spark["args"].get("--verify") and "verify"):
            assert inputs and not outputs, (dag_id, task_id)
        elif spark["script"] != "dp1_bronze/main.py" and spark["script"] != "dp2_silver/main.py":
            assert outputs, (dag_id, task_id)  # Bronze/Silver outputs come from ledger edges
