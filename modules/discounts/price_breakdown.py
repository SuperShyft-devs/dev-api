"""Build checkout price_breakdown payloads for cart preview APIs."""

from __future__ import annotations

from typing import Any

from modules.discounts.engine import ResolvedLine
from modules.discounts.schemas import CartLine, EvaluateResult


def build_price_breakdown(
    items: list[CartLine],
    resolved_by_index: dict[int, ResolvedLine],
    *,
    discounts_by_index: dict[int, int],
    promo_code: str | None,
    promo_applied: bool,
) -> dict[str, Any]:
    lines: list[dict[str, Any]] = []
    original_total = 0
    discount_total = 0

    for idx, item in enumerate(items):
        resolved = resolved_by_index.get(idx)
        if resolved is not None:
            original = int(resolved.amount_paise)
            package_name = resolved.package_name or ""
            entity_type = resolved.entity_type
            entity_id = resolved.entity_id
        else:
            original = int(item.amount_paise or 0)
            package_name = ""
            entity_type = item.entity_type
            entity_id = item.entity_id

        discount = int(discounts_by_index.get(idx, 0))
        payable = max(0, original - discount)
        line_promo = promo_applied and discount > 0

        original_total += original
        discount_total += discount

        lines.append(
            {
                "index": idx,
                "user_id": item.user_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "package_name": package_name,
                "original_paise": original,
                "discount_paise": discount,
                "payable_paise": payable,
                "promo_applied": line_promo,
            }
        )

    payable_total = max(0, original_total - discount_total)
    return {
        "currency": "INR",
        "promo": {
            "code": promo_code,
            "applied": promo_applied,
        },
        "lines": lines,
        "totals": {
            "original_paise": original_total,
            "discount_paise": discount_total,
            "payable_paise": payable_total,
        },
    }


def resolved_lines_by_index(resolved: list[ResolvedLine]) -> dict[int, ResolvedLine]:
    return {line.index: line for line in resolved}


def discounts_from_evaluate(result: EvaluateResult) -> dict[int, int]:
    out: dict[int, int] = {}
    if not result.ok:
        return out
    for j, cart_idx in enumerate(result.eligible_line_indexes):
        off = result.line_discounts_paise[j] if j < len(result.line_discounts_paise) else 0
        out[int(cart_idx)] = int(off)
    return out


def preview_response_from_breakdown(
    breakdown: dict[str, Any],
    *,
    ok: bool,
    message: str | None = None,
    reason: str | None = None,
    code: str | None = None,
    discount_code_id: int | None = None,
) -> dict[str, Any]:
    totals = breakdown.get("totals") or {}
    payload: dict[str, Any] = {
        "ok": ok,
        "code": code,
        "discount_code_id": discount_code_id,
        "original_paise": int(totals.get("original_paise") or 0),
        "discount_paise": int(totals.get("discount_paise") or 0),
        "final_paise": int(totals.get("payable_paise") or 0),
        "price_breakdown": breakdown,
    }
    if message is not None:
        payload["message"] = message
    if reason is not None:
        payload["reason"] = reason
    return payload
