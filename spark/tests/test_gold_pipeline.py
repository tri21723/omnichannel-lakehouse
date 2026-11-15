import json
from decimal import Decimal
from pathlib import Path

import pytest

from spark.lib.gold_entities import GOLD_FIELDS
from spark.lib.gold_publish import verify_gold


def test_gold_fixture_cases_consistency():
    fixtures_path = Path(__file__).parent / "fixtures" / "gold_cases.json"
    assert fixtures_path.exists()
    data = json.loads(fixtures_path.read_text())
    assert data["contract"] == "gold-policy-v1"

    cases = {c["name"]: c for c in data["cases"]}

    # Case 1: multi_line_order
    case1 = cases["multi_line_order"]
    calculated_net = sum(
        (Decimal(item["qty"]) * Decimal(item["unit_price"])) - Decimal(item["discount"])
        for item in case1["items"]
    )
    assert calculated_net == Decimal(case1["expected_order_net"])
    assert len(case1["items"]) == case1["expected_item_count"]
    assert sum(item["qty"] for item in case1["items"]) == case1["expected_total_qty"]

    # Case 2: partial_and_multiple_returns
    case2 = cases["partial_and_multiple_returns"]
    refund_sum = sum(Decimal(ret["refund_amount"]) for ret in case2["returns"])
    assert refund_sum == Decimal(case2["expected_total_refund"])
    assert refund_sum <= Decimal(case2["paid_net"])
    assert Decimal(case2["expected_exceeded_refund"]) > Decimal(case2["paid_net"])

    # Case 3: return_after_order_date
    case3 = cases["return_after_order_date"]
    order_dt = case3["order_ts"].replace("T", " ").replace("Z", "")
    return_dt = case3["return_ts"].replace("T", " ").replace("Z", "")
    assert return_dt >= order_dt

    # Case 4: customer_pii_boundary
    case_pii = cases["customer_pii_boundary"]
    dim_cust_cols = [c.split()[0] for c in GOLD_FIELDS["dim_customer"].split(", ")]
    for allowed in case_pii["allowed_hash_fields"]:
        assert allowed in dim_cust_cols
    for prohibited in case_pii["prohibited_clear_fields"]:
        assert prohibited not in dim_cust_cols

    # Case 5: seller_id_only_derivation
    case_seller = cases["seller_id_only_derivation"]
    dim_seller_cols = [c.split()[0] for c in GOLD_FIELDS["dim_seller"].split(", ")]
    for prohibited in case_seller["prohibited_fields"]:
        assert prohibited not in dim_seller_cols


def test_verify_gold_guards():
    # Incomplete status
    with pytest.raises(ValueError, match="not marked completed"):
        verify_gold(None, dict(status="running"))

    # Missing tables
    with pytest.raises(ValueError, match="Incomplete Gold table inventory"):
        verify_gold(None, dict(status="completed", tables={}))
