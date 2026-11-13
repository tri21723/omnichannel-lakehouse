"""Strict Walmart row normalization. Rejections carry codes, never raw PII."""

import hashlib
import hmac
import json
import re
import unicodedata
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

POLICY = "silver-policy-v1"
STATUSES = {"paid", "pending", "shipped", "delivered", "cancelled", "refunded"}


class Fields:
    def __init__(self, value: dict):
        self.value = value
        self.errors: list[str] = []

    def required(self, name: str) -> Any:
        if name not in self.value:
            self.errors.append(name + ":missing_required_field")
        elif self.value[name] is None:
            self.errors.append(name + ":null_required_field")
        return self.value.get(name)

    def text(self, name: str) -> str | None:
        value = self.required(name)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            self.errors.append(name + ":invalid_string")
            return None
        return value

    def timestamp(self, name: str) -> datetime | None:
        value = self.text(name)
        if value is None:
            return None
        try:
            if not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value
            ):
                raise ValueError
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(UTC)
        except ValueError:
            self.errors.append(name + ":invalid_timestamp")
            return None

    def integer(self, name: str) -> int | None:
        value = self.required(name)
        if value is not None and (type(value) is not int or not 0 < value <= 2147483647):
            self.errors.append(name + ":invalid_positive_integer")
            return None
        return value

    def money(self, name: str) -> Decimal | None:
        value = self.required(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, str | Decimal | int):
            self.errors.append(name + ":invalid_decimal")
            return None
        try:
            number = Decimal(value)
            if not number.is_finite():
                raise InvalidOperation
            if number < 0:
                self.errors.append(name + ":negative_amount")
                return None
            if number >= Decimal("10000000000000000"):
                self.errors.append(name + ":decimal_overflow")
                return None
            with localcontext() as context:
                context.prec = max(40, len(number.as_tuple().digits) + 4)
                quantized = number.quantize(Decimal("0.01"))
            if number != quantized:
                self.errors.append(name + ":decimal_scale")
                return None
            return quantized
        except (InvalidOperation, ValueError):
            self.errors.append(name + ":invalid_decimal")
            return None


def pii_hash(value: str | None, field: str, channel: str, key: bytes) -> str | None:
    if len(key) < 32:
        raise ValueError("PII key must have at least 32 bytes")
    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value)
    normalized = " ".join(normalized.split()) if field == "name" else normalized.strip().lower()
    payload = json.dumps([POLICY, channel, field, normalized], ensure_ascii=False).encode()
    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def normalize(
    raw: str, entity: str, channel: str, key: bytes, key_version: str
) -> tuple[dict, list[str]]:
    if entity not in {"orders", "customers", "order_items"} or channel != "walmart":
        raise ValueError("Unsupported normalization entity/source")
    if len(key) < 32 or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", key_version):
        raise ValueError("Valid PII key and version required")
    try:
        value = json.loads(
            raw, parse_float=Decimal, parse_constant=lambda _: (_ for _ in ()).throw(ValueError())
        )
    except (ValueError, RecursionError):
        return {}, ["_raw:invalid_json"]
    if not isinstance(value, dict):
        return {}, ["_raw:expected_object"]
    fields = Fields(value)
    row = {"channel": channel}
    if entity == "orders":
        for name in ("order_id", "customer_id", "status", "currency", "shipping_country"):
            row[name] = fields.text(name)
        for name in ("order_ts", "updated_ts"):
            row[name] = fields.timestamp(name)
        row["total_amount"] = fields.money("total_amount")
        row["is_deleted"] = False
        if row["status"] is not None and row["status"] not in STATUSES:
            fields.errors.append("status:invalid_enum")
        if row["currency"] is not None and not re.fullmatch(r"[A-Z]{3}", row["currency"]):
            fields.errors.append("currency:invalid_currency")
    elif entity == "customers":
        for name in ("customer_id", "country"):
            row[name] = fields.text(name)
        row["updated_ts"] = fields.timestamp("updated_ts")
        row["name_hash"] = pii_hash(fields.text("full_name"), "name", channel, key)
        row["email_hash"] = pii_hash(fields.text("email"), "email", channel, key)
        row["_pii_key_version"] = key_version
    else:
        for name in ("order_id", "product_id", "seller_id"):
            row[name] = fields.text(name)
        for name in ("line_no", "qty"):
            row[name] = fields.integer(name)
        for name in ("unit_price", "discount"):
            row[name] = fields.money(name)
    if entity == "order_items" and all(
        row.get(n) is not None for n in ("qty", "unit_price", "discount")
    ):
        if row["discount"] > row["qty"] * row["unit_price"]:
            fields.errors.append("discount:exceeds_line_amount")
    return row if not fields.errors else {}, sorted(set(fields.errors))
