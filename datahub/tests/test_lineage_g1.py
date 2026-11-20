import copy

from assertions.lineage_g1 import evaluate, reaches
from lineage import model

ice = model.iceberg


def full_lineage():
    """A complete, consistent graph: every layer, stream and CDC chain."""
    bronze = model.bronze_edges(
        [
            {
                "status": "completed",
                "selection": {"source": "jsonl", "channel": "amazon"},
                "tables": {"lh.bronze.amazon_orders_raw": {}},
            },
            *(
                {
                    "status": "completed",
                    "selection": {"source": "stream", "channel": c},
                    "tables": {f"lh.bronze.{t}": {}},
                }
                for c, t in model.STREAM_TABLES.items()
            ),
        ]
    )
    silver = model.silver_edges(
        {
            "slices": {"a": {"selection": {"source": "jsonl", "channel": "amazon"}}},
            "accounting": {"a": {e: {"input": 1} for e in model.ENTITIES}},
        }
    )
    gold = model.gold_edges({"tables": {f"lh.gold.{t}": {} for t in model.GOLD_UPSTREAMS}})
    dp3 = model.dp3_edges({"tables": {f"lh.gold.{t}": {} for t in model.DP3_UPSTREAMS}})
    warehouse = model.warehouse_edges(
        {"runs": {"r": {"tables": {f"warehouse.{t}": 1 for t in model.GOLD_UPSTREAMS}}}}
    )
    return model.merge(
        bronze, silver, gold, dp3, warehouse, model.stream_edges(), model.cdc_edges()
    )


def datasets(edges):
    return set(edges) | {u for ups in edges.values() for u in ups}


def failures(checks):
    return {c["check"]: c["failed"] for c in checks if c["failed"]}


def test_complete_lineage_passes_every_check():
    edges = full_lineage()
    checks = evaluate(datasets(edges), edges, edges)
    assert failures(checks) == {}
    assert {c["check"] for c in checks} >= {
        "gold_has_silver_upstream",
        "silver_has_bronze_upstream",
        "bronze_has_source_upstream",
        "warehouse_has_gold_upstream",
        "stream_bronze_reaches_topic",
        "cdc_silver_reaches_source",
        "ledger_edges_present",
    }
    gold = next(c for c in checks if c["check"] == "gold_has_silver_upstream")
    assert gold["subjects"] == 10  # 11 Gold tables minus the static dim_channel


def test_removing_one_edge_fails_g1():
    expected = full_lineage()
    live = copy.deepcopy(expected)
    live[ice("lh.gold.fact_returns")] = set()
    result = failures(evaluate(datasets(expected), live, expected))
    assert result["gold_has_silver_upstream"] == ["gold.fact_returns"]
    assert len(result["ledger_edges_present"]) == 3


def test_broken_cdc_and_warehouse_edges_are_reported():
    expected = full_lineage()
    live = copy.deepcopy(expected)
    live[ice(model.CDC_BRONZE)] = set()
    live[model.postgres("sources.warehouse.fact_orders")] = {ice("lh.gold.fact_returns")}
    result = failures(evaluate(datasets(expected), live, expected))
    assert result["cdc_silver_reaches_source"] == [
        "silver.walmart_orders_cdc_current",
        "silver.quarantine_walmart_orders_cdc",
    ]
    assert result["warehouse_has_gold_upstream"] == ["sources.warehouse.fact_orders"]
    assert "bronze.walmart_orders_cdc" in result["bronze_has_source_upstream"]


def test_reaches_handles_cycles_and_hop_limits():
    edges = {"a": {"b"}, "b": {"a", "c"}, "c": {"d"}}
    assert reaches(edges, "a", lambda u: u == "d")
    assert not reaches(edges, "a", lambda u: u == "d", max_hops=2)
    assert not reaches(edges, "a", lambda u: u == "z")


def test_chains_follow_evidence_and_can_be_optional_on_ci():
    expected = model.build([], {}, {}, {}, {}, cdc_ran=False)
    assert expected == {}  # no stream slice, no CDC run: nothing to claim
    batch = {k: v for k, v in full_lineage().items() if "cdc" not in k and "stream" not in k}
    batch = {k: v for k, v in batch.items() if "webhooks" not in k and "notifications" not in k}
    present = datasets(batch)
    strict = failures(evaluate(present, batch, batch))
    assert set(strict) == {"stream_bronze_reaches_topic", "cdc_silver_reaches_source"}
    assert failures(evaluate(present, batch, batch, optional_chains=True)) == {}
