from lineage import jobs, model

ice = model.iceberg


def edges():
    bronze = model.bronze_edges(
        [
            {
                "status": "completed",
                "selection": {"source": "jsonl", "channel": "amazon"},
                "tables": {"lh.bronze.amazon_orders_raw": {}},
            },
            {
                "status": "completed",
                "selection": {"source": "jdbc", "channel": "walmart"},
                "tables": {f"lh.bronze.walmart_{e}_raw": {} for e in model.ENTITIES},
            },
        ]
    )
    silver = model.silver_edges(
        {
            "slices": {"a": {"selection": {"source": "jsonl", "channel": "amazon"}}},
            "accounting": {"a": {e: {"input": 1} for e in model.ENTITIES}},
        }
    )
    return model.merge(bronze, silver, model.stream_edges(), model.cdc_edges())


def test_bronze_and_silver_tasks_read_and_write_their_slice():
    e = edges()
    inputs, outputs = jobs.spark_io("dp1_bronze/main.py", {"--channel": "amazon"}, e)
    assert (inputs, outputs) == ({model.s3("raw/amazon")}, {ice("lh.bronze.amazon_orders_raw")})
    inputs, outputs = jobs.spark_io(
        "dp1_bronze/main.py", {"--channel": "walmart", "--source": "jdbc"}, e
    )
    assert model.postgres("sources.walmart.customers") in inputs
    assert outputs == {ice(f"lh.bronze.walmart_{x}_raw") for x in model.ENTITIES}
    inputs, outputs = jobs.spark_io("dp2_silver/main.py", {"--channel": "amazon"}, e)
    assert inputs == {ice("lh.bronze.amazon_orders_raw")}
    assert {ice("lh.silver._contrib_orders"), ice("lh.silver.orders")} <= outputs
    assert ice("lh.silver.quarantine_returns") in outputs


def test_verify_mode_only_reads_and_unknown_scripts_have_no_datasets():
    inputs, outputs = jobs.spark_io("dp4_warehouse/main.py", {"--mode": "verify"}, {})
    assert model.postgres("sources.warehouse.fact_orders") in inputs and outputs == set()
    assert jobs.spark_io("maintenance/iceberg_report.py", {}, {}) == (set(), set())
    inputs, outputs = jobs.spark_io("dp1_cdc/silver.py", {"--mode": "incremental"}, {})
    assert inputs == {ice(model.CDC_BRONZE)}
    assert outputs == {ice(model.CDC_SILVER), ice(model.CDC_QUARANTINE)}


def test_flows_are_deterministic_and_link_upstream_tasks():
    dags = [
        {
            "dag_id": "dp2_gold",
            "description": "Gold",
            "owner": "data-platform",
            "tags": ["gold", "omnichannel"],
            "schedule": "NullTimetable",
            "tasks": [
                {
                    "task_id": "gold_verify",
                    "operator": "BashOperator",
                    "upstream_task_ids": ["gold_publish"],
                    "spark": {"script": "dp2_silver/gold_main.py", "args": {"--verify": "true"}},
                },
                {
                    "task_id": "gold_publish",
                    "operator": "BashOperator",
                    "upstream_task_ids": [],
                    "spark": {"script": "dp2_silver/gold_main.py", "args": {"--mode": "full"}},
                },
            ],
        }
    ]
    first = jobs.flows(dags, {})
    assert first == jobs.flows(list(reversed(dags)), {})
    flow = first[0]
    assert flow["urn"] == "urn:li:dataFlow:(airflow,dp2_gold,prod)"
    publish, verify = flow["tasks"]
    assert publish["task_id"] == "gold_publish" and ice("lh.gold.fact_orders") in publish["outputs"]
    assert verify["outputs"] == [] and ice("lh.gold.dim_channel") in verify["inputs"]
    assert verify["upstream_jobs"] == [jobs.job_urn("dp2_gold", "gold_publish")]
