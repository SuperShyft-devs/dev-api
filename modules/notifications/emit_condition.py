"""Runtime matching for optional binding emit conditions (payments-first)."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Literal

from core.exceptions import AppError

EmitConditionOp = Literal["is_empty", "is_not_empty", "equals", "one_of"]

PAYMENT_DISCOUNT_CODE_PATH = "payment.discount_code"
ALLOWED_OPS: tuple[EmitConditionOp, ...] = ("is_empty", "is_not_empty", "equals", "one_of")


def _normalize_code(value: str) -> str:
    return value.strip().upper()


def get_context_value(context: dict[str, Any], path: str) -> Any:
    if not path:
        return None
    current: Any = context
    for segment in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    return current


def _string_value(context: dict[str, Any], path: str) -> str:
    raw = get_context_value(context, path)
    if raw is None:
        return ""
    return str(raw).strip()


def matches_condition(context: dict[str, Any], condition: dict[str, Any] | None) -> bool:
    if not condition:
        return True
    path = str(condition.get("path") or PAYMENT_DISCOUNT_CODE_PATH).strip()
    op = str(condition.get("op") or "").strip().lower()
    value = condition.get("value")
    actual = _string_value(context, path)

    if op == "is_empty":
        return actual == ""
    if op == "is_not_empty":
        return actual != ""
    if op == "equals":
        expected = _normalize_code(str(value or ""))
        if not expected:
            return False
        return _normalize_code(actual) == expected
    if op == "one_of":
        parts = [
            _normalize_code(p)
            for p in re.split(r"[,;\n]+", str(value or ""))
            if p.strip()
        ]
        if not parts:
            return False
        normalized = _normalize_code(actual)
        return normalized in parts
    return True


def validate_emit_condition(
    condition: dict[str, Any] | None,
    *,
    event_code: str,
) -> dict[str, Any] | None:
    if condition is None:
        return None
    path = str(condition.get("path") or PAYMENT_DISCOUNT_CODE_PATH).strip()
    op = str(condition.get("op") or "").strip().lower()
    value = condition.get("value")

    if event_code != "payments.completed":
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Emit conditions are only supported for payments.completed in this release",
        )
    if path != PAYMENT_DISCOUNT_CODE_PATH:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=f"Unsupported condition path '{path}'",
        )
    if op not in ALLOWED_OPS:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=f"Unknown condition op '{op}'",
        )
    if op in ("equals", "one_of"):
        if value is None or not str(value).strip():
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Condition value is required for equals / one_of",
            )
    out: dict[str, Any] = {"path": path, "op": op}
    if value is not None and str(value).strip():
        out["value"] = str(value).strip()
    return out


def derive_condition_key(condition: dict[str, Any] | None) -> str:
    if not condition:
        return "default"
    op = str(condition.get("op") or "").strip().lower()
    raw_val = str(condition.get("value") or "").strip()
    if op == "is_empty":
        return "dc-empty"
    if op == "is_not_empty":
        return "dc-not-empty"
    if op == "equals":
        slug = re.sub(r"[^a-z0-9]+", "-", _normalize_code(raw_val).lower()).strip("-")
        return f"dc-eq-{slug or 'unknown'}"[:64]
    if op == "one_of":
        parts = sorted(_normalize_code(p) for p in re.split(r"[,;\n]+", raw_val) if p.strip())
        digest = hashlib.sha256(",".join(parts).encode()).hexdigest()[:12]
        return f"dc-in-{digest}"
    return "default"


def format_emit_condition_label(condition: dict[str, Any] | None) -> str:
    if not condition:
        return "Always"
    op = str(condition.get("op") or "").strip().lower()
    value = str(condition.get("value") or "").strip()
    if op == "is_empty":
        return "No discount code"
    if op == "is_not_empty":
        return "Any discount code"
    if op == "equals":
        return f"Discount = {value}"
    if op == "one_of":
        return f"Discount in {value}"
    return "Always"


def emit_condition_fields_for_event(event_code: str) -> list[dict[str, object]]:
    if event_code != "payments.completed":
        return []
    return [
        {
            "path": PAYMENT_DISCOUNT_CODE_PATH,
            "label": "Payment discount code",
            "allowed_ops": list(ALLOWED_OPS),
        }
    ]
