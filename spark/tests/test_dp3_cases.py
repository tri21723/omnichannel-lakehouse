"""Validate Phase 4D edge cases fixture, schema and point-in-time constraints."""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

FIXTURES_PATH = Path(__file__).parent / "fixtures" / "dp3_cases.json"


def test_dp3_fixture_structure() -> None:
    assert FIXTURES_PATH.exists(), f"Missing fixture file: {FIXTURES_PATH}"
    data = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))

    assert data["contract"] == "dp3-contract-v1"
    assert data["policy_version"] == "gold-policy-v1"
    assert data["horizon_days"] == 14
    assert len(data["cases"]) >= 6


def test_dp3_maturity_censoring_logic() -> None:
    data = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))
    horizon = timedelta(days=data["horizon_days"])

    for case in data["cases"]:
        order = case["order"]
        order_ts = datetime.fromisoformat(order["order_ts"].replace("Z", "+00:00"))
        eval_ts = datetime.fromisoformat(case["eval_as_of_ts"].replace("Z", "+00:00"))
        expected_label = case.get("expected_label")

        if order.get("is_deleted", False):
            assert expected_label is None
            continue

        assert expected_label is not None, f"Missing expected_label for {case['case_id']}"
        mature_calculated = (order_ts + horizon) <= eval_ts
        assert (
            expected_label["is_mature"] == mature_calculated
        ), f"Maturity mismatch in {case['case_id']}"

        if not mature_calculated:
            # ADR-0003: immature labels are NULL, not negative
            assert expected_label["is_returned"] is None
            assert expected_label["days_to_return"] is None
            assert expected_label["return_line_count"] is None
            assert expected_label["refund_amount"] is None
            assert expected_label["refund_ratio"] is None
        else:
            assert expected_label["is_returned"] in (0, 1)
            if expected_label["is_returned"] == 1:
                assert expected_label["days_to_return"] is not None
                assert Decimal(expected_label["refund_amount"]) > 0
                assert Decimal(expected_label["refund_ratio"]) > 0
            else:
                assert expected_label["days_to_return"] is None
                assert Decimal(expected_label["refund_amount"]) == Decimal("0.00")
                assert Decimal(expected_label["refund_ratio"]) == Decimal("0.0000")


def test_dp3_dual_timestamp_cutoff() -> None:
    data = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))

    # Verify future return leakage case
    leakage_case = next(c for c in data["cases"] if c["case_id"] == "case_future_return_leakage")
    eval_ts = datetime.fromisoformat(leakage_case["eval_as_of_ts"].replace("Z", "+00:00"))
    order_ts = datetime.fromisoformat(leakage_case["order"]["order_ts"].replace("Z", "+00:00"))
    return_ts = datetime.fromisoformat(
        leakage_case["returns"][0]["return_ts"].replace("Z", "+00:00")
    )

    assert order_ts < eval_ts, "Order should be placed before eval_ts"
    assert return_ts >= eval_ts, "Return should occur at or after eval_ts"

    # Because return_ts >= eval_ts, the 30d return metrics at eval_ts must be 0
    cust_feat = leakage_case["expected_customer_features"]
    assert cust_feat["returned_orders_30d"] == 0
    assert cust_feat["refund_amount_30d"] == "0.00"
    assert cust_feat["return_rate_30d"] == "0.0000"
