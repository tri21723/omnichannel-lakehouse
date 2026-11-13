"""Flatten source occurrences; preserve parent references and missing-array failures."""

import hashlib
import json
from decimal import Decimal


def expand(raw: str, channel: str) -> list[dict]:
    root_hash = hashlib.sha256((raw if raw is not None else "<null>").encode()).hexdigest()
    try:
        order = json.loads(
            raw, parse_float=Decimal, parse_constant=lambda _: (_ for _ in ()).throw(ValueError())
        )
    except (ValueError, TypeError, RecursionError):
        order = None
    records = []

    def emit(entity, value, ordinal, parent=None):
        records.append(
            dict(
                entity=entity,
                _raw=json.dumps(value, default=str),
                _source_record_hash=root_hash,
                _source_ordinal=ordinal,
                _parent_order_id=parent,
            )
        )

    if not isinstance(order, dict):
        # Keep the original malformed/scalar payload so the parser supplies the right reason.
        emit("orders", None, "order")
        records[0]["_raw"] = raw
        return records
    parent = order.get("order_id") if isinstance(order.get("order_id"), str) else None
    order["_mapping_error"] = False
    if channel == "amazon":
        old, new = order.get("buyer_id"), order.get("customer_id")
        order["_mapping_error"] = old is not None and new is not None and old != new
        if "customer_id" in order or "buyer_id" in order:
            order["customer_id"] = new if new is not None else old
    child_customer = order.get("customer")
    order["_customer_error"] = (
        isinstance(child_customer, dict)
        and child_customer.get("customer_id") is not None
        and child_customer["customer_id"] != order.get("customer_id")
    )
    order["_array_errors"] = []
    for name in ("items", "returns"):
        if name not in order or not isinstance(order[name], list):
            order["_array_errors"].append(name + ":invalid_or_missing_array")
        elif name == "items" and not order[name]:
            order["_array_errors"].append("items:empty_required_array")
    emit("orders", order, "order", parent)
    emit("customers", order.get("customer"), "customer", parent)
    items = order.get("items")
    if isinstance(items, list):
        for index, item in enumerate(items):
            value = dict(item) if isinstance(item, dict) else item
            if isinstance(value, dict):
                value["_parent_error"] = "order_id" in value and value["order_id"] != parent
                value["order_id"] = parent
            if isinstance(value, dict):
                product = value.get("product")
                value["_product_error"] = (
                    isinstance(product, dict)
                    and product.get("product_id") is not None
                    and product["product_id"] != value.get("product_id")
                )
            emit("order_items", value, f"item/{index}", parent)
            emit(
                "products",
                item.get("product") if isinstance(item, dict) else None,
                f"item/{index}/product",
                parent,
            )
    returns = order.get("returns")
    if isinstance(returns, list):
        for index, item in enumerate(returns):
            value = dict(item) if isinstance(item, dict) else item
            if isinstance(value, dict):
                value["_parent_error"] = "order_id" in value and value["order_id"] != parent
                value["order_id"] = parent
            emit("returns", value, f"return/{index}", parent)
    return records
