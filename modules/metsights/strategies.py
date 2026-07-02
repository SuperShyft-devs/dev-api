"""Strategy-based transformation engine for Metsights push/pull.

Each strategy is a pure function that transforms between SuperShyft's internal
answer format and Metsights' API format. The strategy to use is determined by
the ``metsights_sync`` JSON column on ``questionnaire_definitions``.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Push strategies: local answer -> Metsights API payload fields
# ---------------------------------------------------------------------------

def push_scale_emit(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Scale answer ``{value, unit}`` -> ``{key: value, key_unit: unit}``."""
    if not isinstance(answer, dict):
        return {}
    raw_val = answer.get("value")
    unit_raw = answer.get("unit")
    if raw_val is None or unit_raw is None or str(unit_raw).strip() == "":
        return {}
    try:
        val = float(raw_val)
    except (TypeError, ValueError):
        return {}
    return {key: val, f"{key}_unit": str(unit_raw).strip()}


def push_scale_emit_unitless(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Scale answer ``{value, unit}`` -> ``{key: value}`` (no ``*_unit`` field)."""
    if not isinstance(answer, dict):
        return {}
    raw_val = answer.get("value")
    if raw_val is None:
        return {}
    try:
        val = float(raw_val)
    except (TypeError, ValueError):
        return {}
    return {key: val}


def push_passthrough(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Pass the answer as-is with the question_key as the Metsights field."""
    if answer is None:
        return {}
    return {key: answer}


def push_choice_remap(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Remap option values via ``choice_map`` before pushing."""
    if answer is None:
        return {}
    choice_map = params.get("choice_map") or {}
    s = str(answer).strip()
    if not s:
        return {}
    mapped = choice_map.get(s, s)
    return {key: mapped}


def push_bucket_to_scale(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Single-choice bucket -> Metsights ``{key: float, key_unit: str}``."""
    bucket_map = params.get("bucket_map") or {}
    bucket = str(answer).strip() if answer is not None else ""
    mapping = bucket_map.get(bucket)
    if mapping is None or not isinstance(mapping, dict):
        return {}
    val = mapping.get("value")
    unit = mapping.get("unit")
    if val is None or unit is None:
        return {}
    return {key: val, f"{key}_unit": str(unit)}


def push_boolean_string(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Boolean-like answer -> Metsights native boolean."""
    if answer is None:
        return {}
    if isinstance(answer, bool):
        return {key: answer}
    low = str(answer).strip().lower()
    if low in ("true", "1", "yes"):
        return {key: True}
    if low in ("false", "0", "no"):
        return {key: False}
    return {}


def push_single_to_list(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Single selection -> padded list for Metsights.

    Uses ``fill_strategy`` (currently ``deterministic_next``) to pad the list
    to ``min_list_size`` using ``fill_from_option_values`` as the pool.
    """
    if answer is None:
        return {}
    primary = str(answer).strip()
    if not primary:
        return {}

    min_size = int(params.get("min_list_size", 1))
    max_size = int(params.get("max_list_size", min_size))
    fill_strategy = params.get("fill_strategy", "deterministic_next")
    pool = params.get("fill_from_option_values") or []

    result = [primary]

    if fill_strategy == "deterministic_next" and len(result) < min_size and pool:
        try:
            idx = pool.index(primary)
        except ValueError:
            idx = 0
        while len(result) < min_size:
            idx = (idx + 1) % len(pool)
            candidate = pool[idx]
            if candidate not in result:
                result.append(candidate)
            if len(result) >= max_size:
                break
            if idx == (pool.index(primary) if primary in pool else 0):
                break

    return {key: result}


def push_list_to_single(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Collapse multi-select answer to single value for Metsights."""
    if answer is None:
        return {}
    pick_strategy = params.get("pick_strategy", "first_selected")
    if isinstance(answer, list):
        cleaned = [str(x).strip() for x in answer if x is not None and str(x).strip()]
        if not cleaned:
            return {}
        if pick_strategy == "first_selected":
            return {key: cleaned[0]}
        return {key: cleaned[0]}
    s = str(answer).strip()
    return {key: s} if s else {}


def push_skip_if_only(key: str, answer: Any, params: dict) -> dict[str, Any]:
    """Pre-filter: if the only selections are sentinel values, emit nothing.

    Otherwise strip sentinels and pass remaining values through.
    """
    skip_values = set(params.get("skip_values") or ["none"])
    if answer is None:
        return {}
    if isinstance(answer, list):
        cleaned = [str(x).strip() for x in answer if x is not None and str(x).strip().lower() not in skip_values]
        if not cleaned:
            return {}
        return {key: cleaned}
    s = str(answer).strip()
    if s.lower() in skip_values:
        return {}
    return {key: s}


# ---------------------------------------------------------------------------
# Pull strategies: Metsights API response -> local answer format
# ---------------------------------------------------------------------------

def pull_scale_ingest(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Metsights ``{key: float, key_unit: str}`` -> ``{value, unit}``."""
    raw_val = payload.get(key)
    unit_raw = payload.get(f"{key}_unit")
    if raw_val is None or unit_raw is None:
        return None
    if isinstance(raw_val, bool):
        return None
    try:
        val = float(raw_val)
    except (TypeError, ValueError):
        return None
    return {"value": val, "unit": str(unit_raw).strip()}


def pull_scale_ingest_unitless(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Metsights ``{key: float}`` -> ``{value, unit: "0"}`` for unitless scale fields."""
    raw_val = payload.get(key)
    if raw_val is None:
        return None
    if isinstance(raw_val, bool):
        return None
    try:
        val = float(raw_val)
    except (TypeError, ValueError):
        return None
    return {"value": val, "unit": "0"}


def pull_passthrough(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Return the value as-is from the Metsights response."""
    val = payload.get(key)
    if val is None or val == "" or val == []:
        return None
    return val


def pull_choice_ingest(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Pull choice value. Same as passthrough for now -- reserved for future mapping."""
    val = payload.get(key)
    if val is None or val == "" or val == []:
        return None
    return val


def pull_scale_to_bucket(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Metsights float + unit -> our single-choice bucket option_value.

    Algorithm:
    1. Read value and unit from payload
    2. Convert to minutes using ``unit_codes``
    3. Walk ``buckets`` in order, return first matching ``option_value``
    """
    raw_val = payload.get(key)
    unit_raw = payload.get(f"{key}_unit")
    if raw_val is None:
        return None
    if isinstance(raw_val, bool):
        return None
    try:
        val = float(raw_val)
    except (TypeError, ValueError):
        return None

    unit_str = str(unit_raw).strip() if unit_raw is not None else "0"
    unit_codes = params.get("unit_codes") or {}

    minutes: float
    if unit_str in unit_codes.values():
        if unit_str == unit_codes.get("hours", "1"):
            minutes = val * 60
        else:
            minutes = val
    else:
        minutes = val

    buckets = params.get("buckets") or []
    for bucket in buckets:
        max_minutes = bucket.get("max_minutes")
        if max_minutes is not None and minutes <= max_minutes:
            return bucket.get("option_value")
        if max_minutes is None:
            return bucket.get("option_value")

    return None


def pull_string_boolean(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Metsights boolean -> our string ``"true"``/``"false"``."""
    val = payload.get(key)
    if val is None:
        return None
    if isinstance(val, bool):
        return "true" if val else "false"
    low = str(val).strip().lower()
    if low in ("true", "1"):
        return "true"
    if low in ("false", "0"):
        return "false"
    return None


def pull_list_to_single(key: str, payload: dict[str, Any], params: dict) -> Any:
    """Metsights list -> our single value (first item)."""
    val = payload.get(key)
    if val is None:
        return None
    if isinstance(val, list):
        for item in val:
            if item is not None and str(item).strip():
                return str(item).strip()
        return None
    return str(val).strip() if str(val).strip() else None


# ---------------------------------------------------------------------------
# Strategy dispatchers
# ---------------------------------------------------------------------------

PUSH_STRATEGIES: dict[str, Any] = {
    "scale_emit": push_scale_emit,
    "scale_emit_unitless": push_scale_emit_unitless,
    "passthrough": push_passthrough,
    "choice_remap": push_choice_remap,
    "bucket_to_scale": push_bucket_to_scale,
    "boolean_string": push_boolean_string,
    "single_to_list": push_single_to_list,
    "list_to_single": push_list_to_single,
    "skip_if_only": push_skip_if_only,
}

PULL_STRATEGIES: dict[str, Any] = {
    "scale_ingest": pull_scale_ingest,
    "scale_ingest_unitless": pull_scale_ingest_unitless,
    "passthrough": pull_passthrough,
    "choice_ingest": pull_choice_ingest,
    "scale_to_bucket": pull_scale_to_bucket,
    "string_boolean": pull_string_boolean,
    "list_to_single": pull_list_to_single,
}


def apply_push_strategy(
    question_key: str,
    answer: Any,
    sync_config: dict[str, Any],
) -> dict[str, Any]:
    """Apply the configured push strategy to transform one answer into Metsights fields."""
    push_cfg = sync_config.get("push") or {}
    strategy_name = push_cfg.get("strategy", "passthrough")
    strategy_fn = PUSH_STRATEGIES.get(strategy_name)
    if strategy_fn is None:
        logger.warning("Unknown push strategy %r for key %s, falling back to passthrough", strategy_name, question_key)
        strategy_fn = push_passthrough

    params = {k: v for k, v in push_cfg.items() if k not in ("enabled", "strategy")}
    return strategy_fn(question_key, answer, params)


def apply_pull_strategy(
    question_key: str,
    metsights_payload: dict[str, Any],
    sync_config: dict[str, Any],
) -> Any:
    """Apply the configured pull strategy to transform a Metsights field into a local answer."""
    pull_cfg = sync_config.get("pull") or {}
    strategy_name = pull_cfg.get("strategy", "passthrough")
    strategy_fn = PULL_STRATEGIES.get(strategy_name)
    if strategy_fn is None:
        logger.warning("Unknown pull strategy %r for key %s, falling back to passthrough", strategy_name, question_key)
        strategy_fn = pull_passthrough

    params = {k: v for k, v in pull_cfg.items() if k not in ("enabled", "strategy")}
    return strategy_fn(question_key, metsights_payload, params)
