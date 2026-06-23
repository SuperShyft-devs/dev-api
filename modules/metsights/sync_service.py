"""Metsights record sync and questionnaire answer import."""

from __future__ import annotations

import logging
import random
import re
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from db.seed.questionnaire_field_config import (
    CHOICE_TO_METSIGHTS_VALUE,
    DAILY_ACTIVE_DURATION_PUSH_MAP,
    HEALTH_PRIORITIES_LABEL_TO_VALUE,
    HEALTH_PRIORITIES_OPTION_VALUES,
    METSIGHTS_PUSH_AS_LIST,
    NONE_CLEARS_MULTISELECT_FIELDS,
    RANDOM_SINGLE_FROM_MULTISELECT_FIELDS,
    SCALE_TO_CHOICE_CONVERTERS,
)
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository
from modules.diagnostics.models import DiagnosticPackage
from modules.assessments.service import AssessmentsService
from modules.engagements.models import EngagementKind
from modules.engagements.service import EngagementsService
from modules.metsights.service import MetsightsService
from modules.metsights.strategies import apply_pull_strategy, apply_push_strategy
from modules.platform_settings.service import PlatformSettingsService
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository

_CATEGORY_KEY_TO_API_PATH: dict[str, str] = {
    "physical-measurement": "physical-measurement",
    "vitals": "vitals",
    "diet-lifestyle-parameters": "diet-lifestyle-parameters",
    "fitness-parameters": "fitness-parameters",
}

logger = logging.getLogger(__name__)


async def _resolve_active_diagnostic_package_id(db: AsyncSession, preferred_id: int) -> int:
    """Use ``preferred_id`` when active; otherwise first active diagnostic package."""

    row = (
        await db.execute(select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == preferred_id).limit(1))
    ).scalar_one_or_none()
    if row is not None and (row.status or "").lower() == "active":
        return int(preferred_id)
    alt = (
        await db.execute(
            select(DiagnosticPackage.diagnostic_package_id)
            .where(func.lower(DiagnosticPackage.status) == "active")
            .order_by(DiagnosticPackage.diagnostic_package_id.asc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if alt is None:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="No active diagnostic package available for engagement creation",
        )
    return int(alt)

_ASSESSMENT_CODE_TO_TYPE_CODE: dict[str, str] = {
    "MET_BASIC": "1",
    "MET_PRO": "2",
    "MY_FITNESS_PRINT": "7",
}

_METADATA_FIELDS = frozenset(
    {
        "id",
        "created_at",
        "updated_at",
        "is_complete",
    }
)

# (measurement_field_name, metsights_unit_choice_code) -> ``questionnaire_options.option_value`` (Metsights OPTIONS codes).
_METSIGHTS_UNIT_CODE_TO_DB_OPTION: dict[tuple[str, str], str] = {
    ("height", "0"): "0",
    ("height", "2"): "2",
    ("weight", "0"): "0",
    ("weight", "1"): "1",
    ("waist_circumference", "0"): "0",
    ("waist_circumference", "1"): "1",
    ("hip_circumference", "0"): "0",
    ("hip_circumference", "1"): "1",
    ("body_fat", "0"): "0",
    ("bmi", "0"): "0",
    ("systolic_blood_pressure", "0"): "0",
    ("diastolic_blood_pressure", "0"): "0",
    ("heart_rate", "0"): "0",
    ("respiratory_rate", "0"): "0",
    ("hrv_sdnn", "0"): "0",
    ("daily_active_duration", "0"): "0",  # kept for reverse-push only; import uses SCALE_TO_CHOICE_CONVERTERS
    ("daily_active_duration", "1"): "1",  # kept for reverse-push only; import uses SCALE_TO_CHOICE_CONVERTERS
    ("weight_loss_goal", "0"): "0",
    ("weight_loss_goal", "1"): "1",
}

# Keys accepted by Metsights record PATCH payloads (aligned with OPTIONS / diet-lifestyle-parameters / fitness-parameters).
_METSIGHTS_SUBMIT_PHYSICAL_KEYS = frozenset({"height", "weight", "waist_circumference", "hip_circumference", "body_fat"})
_METSIGHTS_SUBMIT_VITALS_KEYS = frozenset({"systolic_blood_pressure", "diastolic_blood_pressure"})
_METSIGHTS_SUBMIT_DIET_LIFESTYLE_KEYS = frozenset(
    {
        "living_region",
        "diet_preference",
        "food_groups",
        "healthy_breakfast_frequency",
        "fresh_fruit_frequency",
        "fresh_vegetable_frequency",
        "baked_goods_frequency",
        "red_meat_frequency",
        "butter_dish_frequency",
        "dessert_frequency",
        "caffeine_frequency",
        "caffeine_type",
        "iodized_salt_status",
        "extra_salt_frequency",
        "sitting_hours",
        "physical_activity_frequency",
        "sleeping_hours",
        "alcohol_frequency",
        "tobacco_frequency",
        "family_health_history",
        "family_health_history_other",
        "diagnosed_diseases",
        "diagnosed_diseases_other",
        "diagnosed_diseases_medications",
        "diagnosed_diseases_medications_other",
    }
)
_METSIGHTS_SUBMIT_FITNESS_ONLY_KEYS = frozenset(
    {
        "exercise_frequency_week",
        "exercise_level",
        "daily_active_duration",
        "water_intake_frequency",
        "sickness_frequency",
        "health_priorities",
        "goal_preference",
        "weight_loss_goal",
    }
)

# Questionnaire sub-resources that should be marked complete when pushing (Metsights UI sections).
_METSIGHTS_QUESTIONNAIRE_RESOURCES = frozenset({
    "physical-measurement",
    "vitals",
    "diet-lifestyle-parameters",
    "fitness-parameters",
})


def _question_type_for_submit(raw: str | None) -> str:
    t = (raw or "").strip().lower()
    return {"multi_choice": "multiple_choice"}.get(t, t)


def _expand_health_priorities_for_metsights(selected: str) -> list[str]:
    """Keep the user's single choice and add a second distinct Metsights option."""

    primary = str(selected).strip()
    if not primary or primary not in HEALTH_PRIORITIES_OPTION_VALUES:
        return []
    others = sorted(v for v in HEALTH_PRIORITIES_OPTION_VALUES if v != primary)
    if not others:
        return [primary]
    secondary = random.choice(others)
    return [primary, secondary]


def _resolve_health_priority_option_code(raw: Any) -> str | None:
    """Normalize a stored answer fragment to a Metsights option_value (0–5)."""

    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() == "none":
        return None
    if s in HEALTH_PRIORITIES_OPTION_VALUES:
        return s
    by_label = HEALTH_PRIORITIES_LABEL_TO_VALUE.get(_normalize_label(s))
    if by_label:
        return by_label
    fl = _label_fingerprint(s)
    for label, code in HEALTH_PRIORITIES_LABEL_TO_VALUE.items():
        if _label_fingerprint(label) == fl:
            return code
    return None


def _health_priorities_to_metsights_fields(answer: Any) -> dict[str, Any]:
    """Always send two distinct codes: user's selection plus a random other."""

    primary: str | None = None
    if isinstance(answer, list):
        for item in answer:
            primary = _resolve_health_priority_option_code(item)
            if primary:
                break
    else:
        primary = _resolve_health_priority_option_code(answer)

    if not primary:
        return {}
    expanded = _expand_health_priorities_for_metsights(primary)
    return {"health_priorities": expanded} if expanded else {}


def _answer_to_metsights_fields(question_key: str, question_type: str, answer: Any) -> dict[str, Any]:
    """Map one DB answer to Metsights JSON keys (may include sibling ``*_unit`` for scales)."""

    qkey = (question_key or "").strip()
    if not qkey:
        return {}
    qtype = _question_type_for_submit(question_type)

    # UI may still store multiple_choice (array) or labels; expand before Metsights push.
    if qkey == "health_priorities":
        return _health_priorities_to_metsights_fields(answer)

    if qtype == "scale":
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
        return {qkey: val, f"{qkey}_unit": str(unit_raw).strip()}

    if qtype == "text":
        s = str(answer).strip() if answer is not None else ""
        if not s:
            return {}
        return {qkey: s}

    if qtype == "multiple_choice":
        if not isinstance(answer, list):
            return {}

        # Change 5: if the only selection is "none", skip the field entirely.
        if qkey in NONE_CLEARS_MULTISELECT_FIELDS:
            cleaned = [str(x).strip() for x in answer if x is not None and str(x).strip() not in ("", "none")]
            if not cleaned:
                # User chose "None" — send nothing to metsights.
                return {}
            return {qkey: cleaned}

        # Change 7: fields where Metsights only accepts a single value but our DB
        # stores all the user's selections (multiple_choice).  Pick one at random
        # so every valid selection has an equal chance of being pushed.
        if qkey in RANDOM_SINGLE_FROM_MULTISELECT_FIELDS:
            seq = [
                str(x).strip()
                for x in answer
                if x is not None
                and str(x).strip() != ""
                and str(x).strip().lower() != "none"
            ]
            if not seq:
                return {}
            return {qkey: random.choice(seq)}

        seq = [
            str(x).strip()
            for x in answer
            if x is not None
            and str(x).strip() != ""
            and str(x).strip().lower() != "none"
        ]
        return {qkey: seq} if seq else {}

    if qtype == "single_choice":
        if answer is None:
            return {}

        # Change 1: daily_active_duration — our bucket option_value → metsights float+unit.
        if qkey == "daily_active_duration":
            bucket = str(answer).strip()
            mapping = DAILY_ACTIVE_DURATION_PUSH_MAP.get(bucket)
            if mapping is None:
                return {}
            mets_val, mets_unit = mapping
            return {qkey: mets_val, f"{qkey}_unit": mets_unit}

        if qkey == "iodized_salt_status":
            if isinstance(answer, bool):
                return {qkey: "true" if answer else "false"}
            low = str(answer).strip().lower()
            if low in ("true", "1", "yes"):
                return {qkey: "true"}
            if low in ("false", "0", "no"):
                return {qkey: "false"}
            return {}

        s = str(answer).strip()
        if not s:
            return {}

        # Changes 2/3/4: remap new option values to their metsights equivalents.
        field_map = CHOICE_TO_METSIGHTS_VALUE.get(qkey)
        if field_map and s in field_map:
            s = field_map[s]

        if qkey in METSIGHTS_PUSH_AS_LIST:
            return {qkey: [s]}

        return {qkey: s}

    return {}


def _pick_metsights_payload_for_bases(merged: dict[str, Any], bases: frozenset[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for base in bases:
        if base in merged:
            out[base] = merged[base]
        uk = f"{base}_unit"
        if uk in merged:
            out[uk] = merged[uk]
    return out


class _FieldMeta:
    """Metadata for a single Metsights OPTIONS field."""
    __slots__ = ("valid_choices", "required")

    def __init__(self, valid_choices: set[str], required: bool):
        self.valid_choices = valid_choices
        self.required = required


def _extract_field_metadata_from_options(options_envelope: dict[str, Any]) -> dict[str, _FieldMeta]:
    """Parse an OPTIONS response to extract valid choice values and required flag per field.

    Returns ``{field_name: _FieldMeta}`` for every field that has a ``choices``
    list (including ``child.choices`` for list fields).
    """
    result: dict[str, _FieldMeta] = {}
    actions = options_envelope.get("actions") if isinstance(options_envelope, dict) else None
    if not isinstance(actions, dict):
        return result
    for method_actions in actions.values():
        if not isinstance(method_actions, dict):
            continue
        for field_key, field_info in method_actions.items():
            if not isinstance(field_info, dict):
                continue
            choices = field_info.get("choices")
            child = field_info.get("child")
            if isinstance(child, dict) and child.get("choices"):
                choices = child["choices"]
            if not isinstance(choices, list):
                continue
            valid: set[str] = set()
            for c in choices:
                if isinstance(c, dict):
                    v = c.get("value")
                    if v is not None:
                        valid.add(str(v).strip())
                else:
                    valid.add(str(c).strip())
            if valid:
                required = bool(field_info.get("required", False))
                if field_key not in result:
                    result[field_key] = _FieldMeta(valid, required)
                else:
                    result[field_key].valid_choices |= valid
                    result[field_key].required = result[field_key].required or required
    return result


def _validate_payload_against_options(
    payload: dict[str, Any],
    field_meta: dict[str, _FieldMeta],
) -> dict[str, Any]:
    """Remove or remap fields whose values are not in valid Metsights choices.

    For **required** fields with invalid values, the closest numeric choice is
    substituted so the POST does not fail on a missing required field.
    For optional fields, invalid values are silently dropped.
    """
    cleaned: dict[str, Any] = {}
    for key, value in payload.items():
        if key in _METADATA_FIELDS:
            continue
        meta = field_meta.get(key)
        if meta is None:
            cleaned[key] = value
            continue
        allowed = meta.valid_choices
        if isinstance(value, list):
            filtered = [v for v in value if str(v).strip() in allowed]
            cleaned[key] = filtered
        elif isinstance(value, bool):
            bool_str = "true" if value else "false"
            if bool_str in allowed:
                cleaned[key] = bool_str
            elif str(value) in allowed:
                cleaned[key] = value
            elif meta.required:
                fallback = sorted(allowed)[0]
                logger.warning("Remapping required field %s: bool %r -> %r", key, value, fallback)
                cleaned[key] = fallback
            else:
                logger.warning("Skipping optional field %s: bool value %r not in valid choices", key, value)
        else:
            sv = str(value).strip()
            if sv in allowed:
                cleaned[key] = value
            elif meta.required:
                fallback = _find_closest_choice(sv, allowed)
                logger.warning("Remapping required field %s: %r -> %r (closest valid)", key, sv, fallback)
                cleaned[key] = fallback
            else:
                logger.warning("Skipping optional field %s: value %r not in valid choices %s", key, sv, allowed)
    return cleaned


def _find_closest_choice(invalid_value: str, allowed: set[str]) -> str:
    """Pick the numerically closest valid choice, falling back to the first sorted value."""
    try:
        target = int(invalid_value)
    except (ValueError, TypeError):
        return sorted(allowed)[0]
    best: str | None = None
    best_dist = float("inf")
    for v in sorted(allowed):
        try:
            dist = abs(int(v) - target)
        except (ValueError, TypeError):
            continue
        if dist < best_dist:
            best_dist = dist
            best = v
    return best if best is not None else sorted(allowed)[0]


def _normalize_metsights_type_code(record_row: dict[str, Any]) -> str | None:
    code = str(record_row.get("assessment_code") or "").strip().upper()
    if code in _ASSESSMENT_CODE_TO_TYPE_CODE:
        return _ASSESSMENT_CODE_TO_TYPE_CODE[code]
    at = str(record_row.get("assessment_type") or "").strip()
    if at in ("1", "2", "7"):
        return at
    low = at.lower()
    if "fitprint" in low or "fitness print" in low or "my_fitness" in low:
        return "7"
    if "met pro" in low or "metsights pro" in low:
        return "2"
    if "basic" in low:
        return "1"
    return None


def _parse_iso_date(raw: Any) -> date:
    if raw is None:
        return date.today()
    s = str(raw).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return date.today()


def _normalize_label(s: str) -> str:
    t = (s or "").strip().lower()
    t = t.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", t)


def _label_fingerprint(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalize_label(s))


def _merge_choices_into(target: dict[str, dict[str, str]], field_key: str, choices_raw: Any) -> None:
    fk = (field_key or "").strip()
    if not fk or choices_raw is None:
        return
    bucket = target.setdefault(fk, {})
    if isinstance(choices_raw, dict):
        for code, lab in choices_raw.items():
            bucket[str(code).strip()] = str(lab).strip() if lab is not None else ""
        return
    if isinstance(choices_raw, list):
        for item in choices_raw:
            if not isinstance(item, dict):
                continue
            c = item.get("value")
            if c is None:
                c = item.get("code") or item.get("id")
            lab = item.get("label") or item.get("display_name") or item.get("name")
            if c is None:
                continue
            bucket[str(c).strip()] = str(lab).strip() if lab is not None else ""


def _build_field_choice_maps(options_envelope: dict[str, Any]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    data = options_envelope.get("data") if isinstance(options_envelope, dict) else None
    if not isinstance(data, dict):
        return out
    qc = data.get("questions_config")
    if isinstance(qc, list):
        for item in qc:
            if not isinstance(item, dict):
                continue
            key = str(
                item.get("key") or item.get("field") or item.get("name") or item.get("question_key") or ""
            ).strip()
            if key:
                _merge_choices_into(out, key, item.get("choices") or item.get("options"))
    for k, v in data.items():
        if k in _METADATA_FIELDS or not isinstance(v, dict):
            continue
        ch = v.get("choices") or v.get("options")
        if ch is not None:
            _merge_choices_into(out, str(k).strip(), ch)
    return out


def _option_value_display_pairs(db_options: Any) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for o in db_options:
        ov = getattr(o, "option_value", None)
        if ov is None:
            continue
        dn = getattr(o, "display_name", None)
        rows.append((str(ov), str(dn or "")))
    return rows


def _match_option_value_for_label(label: str, db_options: Any) -> str | None:
    nl = _normalize_label(label)
    if not nl:
        return None
    fl = _label_fingerprint(label)
    best: str | None = None
    for ov, dn in _option_value_display_pairs(db_options):
        nv = _normalize_label(ov)
        nd = _normalize_label(dn)
        if nl == nv or nl == nd:
            return ov
        if fl and fl == _label_fingerprint(dn):
            return ov
        if nd and (nl in nd or nd in nl):
            best = ov
    return best


def _map_metsights_choice_to_option_value(
    *,
    raw_code: Any,
    code_to_label: dict[str, str],
    db_options: Any,
) -> str | None:
    if raw_code is None:
        return None
    code_s = str(raw_code).strip()
    if not code_s:
        return None
    for ov, _dn in _option_value_display_pairs(db_options):
        if _normalize_label(ov) == _normalize_label(code_s):
            return ov
    label = code_to_label.get(code_s)
    if label:
        hit = _match_option_value_for_label(str(label), db_options)
        if hit:
            return hit
    return _match_option_value_for_label(code_s, db_options)


def _pick_scale_unit_string(canonical_candidate: str | None, db_unit_options: Any) -> str | None:
    if not canonical_candidate:
        return None
    nc = _normalize_label(canonical_candidate)
    for ov, _dn in _option_value_display_pairs(db_unit_options):
        if _normalize_label(ov) == nc:
            return ov
    return None


def _scale_answer_mapped(
    field: str,
    payload: dict[str, Any],
    value: Any,
    choice_maps: dict[str, dict[str, str]],
    db_unit_options: Any,
) -> dict[str, Any] | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    unit_key = f"{field}_unit"
    unit_raw = payload.get(unit_key)
    if unit_raw is None:
        return None
    ukey = str(unit_raw).strip()
    unit_codes = choice_maps.get(unit_key) or {}
    canonical = _METSIGHTS_UNIT_CODE_TO_DB_OPTION.get((field, ukey))
    if canonical is None:
        canonical = _METSIGHTS_UNIT_CODE_TO_DB_OPTION.get((field, ukey.lower()))
    if canonical is None:
        ulab = unit_codes.get(ukey)
        if ulab:
            picked = _match_option_value_for_label(str(ulab), db_unit_options)
            if picked:
                canonical = picked
    if canonical is None:
        canonical = _map_metsights_choice_to_option_value(
            raw_code=ukey,
            code_to_label=unit_codes,
            db_options=db_unit_options,
        )
    final_unit = _pick_scale_unit_string(canonical, db_unit_options)
    if final_unit is None:
        return None
    return {"value": float(value), "unit": final_unit}


def _resources_for_assessment_type(type_code: str) -> list[str]:
    tc = (type_code or "").strip()
    if tc in ("1", "2"):
        return ["diet-lifestyle-parameters", "physical-measurement", "vitals"]
    if tc == "7":
        return ["fitness-parameters"]
    return []


# Metsights returns ``405 Method Not Allowed`` for ``GET`` on some sub-resources
# (e.g. ``/vitals/`` and ``/fitness-parameters/``). Those endpoints are still readable
# via ``GET /records/:id/`` under these nested keys, so we fall back to the record
# detail payload when the sub-resource GET is unavailable or empty.
_RESOURCE_TO_DETAIL_FIELD: dict[str, str] = {
    "physical-measurement": "physical_measurement",
    "vitals": "vital_parameter",
    "diet-lifestyle-parameters": "diet_lifestyle_parameter",
    "fitness-parameters": "fitness_parameter",
}


class MetsightsSyncService:
    def __init__(
        self,
        *,
        metsights_service: MetsightsService,
        users_repository: UsersRepository,
        engagements_service: EngagementsService,
        assessments_service: AssessmentsService,
        platform_settings_service: PlatformSettingsService,
        questionnaire_repository: QuestionnaireRepository,
    ):
        self._metsights = metsights_service
        self._users = users_repository
        self._engagements = engagements_service
        self._assessments = assessments_service
        self._platform = platform_settings_service
        self._questionnaire = questionnaire_repository

    def _ensure_sync_access(self, *, current_user_id: int, target_user_id: int, employee_ok: bool) -> None:
        if int(target_user_id) == int(current_user_id):
            return
        if employee_ok:
            return
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    @staticmethod
    def _push_placeholder_url(*, record_id: str | None = None, resource: str = "push") -> str:
        mrid = (record_id or "unknown").strip() or "unknown"
        return f"/records/{mrid}/{resource}/"

    async def _create_pending_sync_log(
        self,
        db: AsyncSession,
        *,
        engagement_id: int | None,
        user_id: int | None,
        api_url: str,
        request_payload: dict | None = None,
    ) -> IntegrationSyncLog:
        audit_repo = AuditRepository()
        return await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider="metsights",
                api_endpoint_url=api_url,
                request_payload=request_payload,
                status="pending",
            ),
        )

    async def _finalize_sync_log(
        self,
        db: AsyncSession,
        *,
        sync_log_id: int,
        status: str,
        response_payload: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        audit_repo = AuditRepository()
        await audit_repo.update_sync_log_status(
            db,
            sync_log_id=sync_log_id,
            status=status,
            response_payload=response_payload,
            error_message=error_message,
        )

    async def _log_skipped_metsights_sync(
        self,
        db: AsyncSession,
        *,
        engagement_id: int | None,
        user_id: int | None,
        api_url: str,
        reason: str,
        request_payload: dict | None = None,
    ) -> None:
        audit_repo = AuditRepository()
        await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider="metsights",
                api_endpoint_url=api_url,
                request_payload=request_payload,
                response_payload={"skipped": True, "reason": reason},
                status="skipped",
            ),
        )

    async def log_skipped_push(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        user_id: int,
        reason: str,
        assessment_instance_id: int | None = None,
    ) -> None:
        """Log a skipped bulk/employee push attempt (no Metsights API call)."""
        response_payload: dict[str, Any] = {"skipped": True, "reason": reason}
        if assessment_instance_id is not None:
            response_payload["assessment_instance_id"] = assessment_instance_id
        audit_repo = AuditRepository()
        await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider="metsights",
                api_endpoint_url=self._push_placeholder_url(),
                response_payload=response_payload,
                status="skipped",
            ),
        )

    async def sync_completed_metsights_records(
        self,
        db: AsyncSession,
        *,
        target_user_id: int,
        current_user_id: int,
        employee_ok: bool,
        engagement_code: str | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        self._ensure_sync_access(
            current_user_id=current_user_id,
            target_user_id=target_user_id,
            employee_ok=employee_ok,
        )

        user = await self._users.get_user_by_id(db, target_user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        profile_id = (user.metsights_profile_id or "").strip()
        if not profile_id:
            raise AppError(
                status_code=422,
                error_code="METSIGHTS_PROFILE_REQUIRED",
                message="User has no Metsights profile id; complete profile sync first",
            )

        raw_list = await self._metsights.list_profile_records(profile_id=profile_id, completed="1")
        if not isinstance(raw_list, list):
            raw_list = []

        fixed_engagement = None
        if engagement_code and str(engagement_code).strip():
            fixed_engagement = await self._engagements.get_by_code(db, str(engagement_code).strip())
            if fixed_engagement is None:
                raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        created: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        default_slot = time(10, 0)

        for row in raw_list:
            if not isinstance(row, dict):
                continue
            mrid = str(row.get("id") or "").strip()
            if not mrid:
                errors.append({"record": row, "reason": "missing record id"})
                continue

            type_code = _normalize_metsights_type_code(row)
            if not type_code:
                skipped.append({"metsights_record_id": mrid, "reason": "unknown assessment type"})
                continue

            package = await self._assessments.get_package_by_assessment_type_code(
                db,
                assessment_type_code=type_code,
            )
            if package is None:
                skipped.append({"metsights_record_id": mrid, "reason": f"no active package for type {type_code}"})
                continue

            existing = await self._assessments.get_instance_by_metsights_record_id(db, metsights_record_id=mrid)
            if existing is not None:
                skipped.append({"metsights_record_id": mrid, "reason": "already imported"})
                continue

            engagement = fixed_engagement
            if engagement is None:
                eng_date = _parse_iso_date(row.get("date"))
                ap_id = int(package.package_id)
                try:
                    _ap, diag_id = await self._platform.resolve_b2c_default_package_ids(db)
                    default_diag = int(diag_id)
                except Exception:
                    default_diag = 1
                default_diag = await _resolve_active_diagnostic_package_id(db, default_diag)
                await self._platform.ensure_active_b2c_packages(db, ap_id, default_diag)
                engagement = await self._engagements.create_b2c_engagement(
                    db,
                    user_first_name=user.first_name,
                    engagement_date=eng_date,
                    city=user.city,
                    assessment_package_id=ap_id,
                    diagnostic_package_id=default_diag,
                    engagement_type=EngagementKind.bio_ai,
                    address=user.address,
                    pincode=user.pin_code,
                )
            else:
                if engagement.assessment_package_id is None or int(engagement.assessment_package_id) != int(package.package_id):
                    skipped.append(
                        {
                            "metsights_record_id": mrid,
                            "reason": "engagement package does not match record assessment type",
                        }
                    )
                    continue

            if not await self._engagements.user_has_slot_for_engagement(
                db, user_id=target_user_id, engagement_id=engagement.engagement_id
            ):
                eng_date = _parse_iso_date(row.get("date"))
                try:
                    await self._engagements.enroll_user_in_engagement(
                        db,
                        engagement=engagement,
                        user_id=target_user_id,
                        engagement_date=eng_date,
                        slot_start_time=default_slot,
                        increment_participant_count=True,
                        is_profile_created_on_metsights=True,
                    )
                except AppError as exc:
                    errors.append({"metsights_record_id": mrid, "reason": str(exc.message)})
                    continue
                except Exception as exc:
                    logger.exception("enroll failed for metsights sync")
                    errors.append({"metsights_record_id": mrid, "reason": str(exc)})
                    continue

                target_user = await self._users.get_user_by_id(db, target_user_id)
                if target_user is not None:
                    await self._engagements.notify_onboarding_assistants_after_enrollment(
                        db,
                        engagement=engagement,
                        user=target_user,
                        source=engagement.engagement_code or "metsights-sync",
                        collection_date=eng_date.isoformat(),
                        collection_time=default_slot.isoformat(),
                    )

            ms_complete = bool(row.get("is_complete"))

            try:
                instance = await self._assessments.create_instance_for_metsights_record(
                    db,
                    user_id=target_user_id,
                    engagement_id=engagement.engagement_id,
                    package_id=int(package.package_id),
                    metsights_record_id=mrid,
                    metsights_is_complete=ms_complete,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
            except AppError as exc:
                errors.append({"metsights_record_id": mrid, "reason": str(exc.message)})
                continue
            except Exception as exc:
                logger.exception("create_instance_for_metsights_record failed")
                errors.append({"metsights_record_id": mrid, "reason": str(exc)})
                continue

            created.append(
                {
                    "metsights_record_id": mrid,
                    "assessment_instance_id": instance.assessment_instance_id,
                    "engagement_id": engagement.engagement_id,
                }
            )

        return {"created": created, "skipped": skipped, "errors": errors}

    async def import_questionnaire_answers_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        current_user_id: int,
        employee_ok: bool,
    ) -> dict[str, Any]:
        row = await self._assessments.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        instance = row
        self._ensure_sync_access(
            current_user_id=current_user_id,
            target_user_id=int(instance.user_id),
            employee_ok=employee_ok,
        )

        mrid = (instance.metsights_record_id or "").strip()
        if not mrid:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment instance has no Metsights record id",
            )

        package = await self._assessments.get_package_by_id(db, int(instance.package_id))
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Assessment package not found")

        type_code = (package.assessment_type_code or "").strip()
        resources = _resources_for_assessment_type(type_code)
        if not resources:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message=f"No Metsights questionnaire resources mapped for assessment type {type_code!r}",
            )

        engagement_id = int(instance.engagement_id) if instance.engagement_id else None
        participant_user_id = int(instance.user_id)

        imported = 0
        skipped_questions: list[str] = []
        record_detail_cache: dict[str, Any] | None = None
        record_detail_fetched = False

        async def _get_record_detail_cached() -> dict[str, Any] | None:
            nonlocal record_detail_cache, record_detail_fetched
            if record_detail_fetched:
                return record_detail_cache
            record_detail_fetched = True
            try:
                detail = await self._metsights.get_record_detail(record_id=mrid)
            except AppError:
                record_detail_cache = None
                return None
            record_detail_cache = detail if isinstance(detail, dict) else None
            return record_detail_cache

        async def _ingest_payload(
            resource: str,
            payload: dict[str, Any],
            choice_maps: dict[str, dict[str, str]],
        ) -> int:
            nonlocal imported
            resource_imported = 0
            for field_name, raw_val in payload.items():
                if field_name in _METADATA_FIELDS:
                    continue
                if str(field_name).endswith("_unit"):
                    continue
                if raw_val is None:
                    continue
                if raw_val == []:
                    continue

                qdef = await self._questionnaire.get_definition_by_key(db, question_key=str(field_name))
                if qdef is None:
                    skipped_questions.append(f"{resource}.{field_name}")
                    continue

                category_id = await self._assessments.get_first_category_id_for_question_in_package(
                    db,
                    package_id=int(instance.package_id),
                    question_id=int(qdef.question_id),
                )
                if category_id is None:
                    skipped_questions.append(f"{resource}.{field_name}.no_category")
                    continue

                qtype = (qdef.question_type or "").strip().lower()
                qtype = {"multi_choice": "multiple_choice"}.get(qtype, qtype)
                db_opts = await self._questionnaire.list_options_for_question(db, question_id=int(qdef.question_id))

                answer: Any = None

                if qtype == "scale":
                    answer = _scale_answer_mapped(str(field_name), payload, raw_val, choice_maps, db_opts)
                    if answer is None:
                        skipped_questions.append(f"{resource}.{field_name}")
                        continue
                elif qtype == "single_choice" and field_name in SCALE_TO_CHOICE_CONVERTERS and isinstance(raw_val, (int, float)) and not isinstance(raw_val, bool):
                    # Field was previously a scale type in metsights but is now a
                    # single_choice in our DB.  Use the converter from the config.
                    unit_key = f"{field_name}_unit"
                    unit_raw = payload.get(unit_key, "0")
                    converted = SCALE_TO_CHOICE_CONVERTERS[field_name](float(raw_val), str(unit_raw).strip())
                    if converted is None:
                        skipped_questions.append(f"{resource}.{field_name}:no_bucket")
                        continue
                    answer = converted
                elif qtype == "text":
                    answer = str(raw_val).strip() if raw_val is not None else None
                    if not answer:
                        continue
                elif qtype == "multiple_choice":
                    seq = raw_val if isinstance(raw_val, list) else [raw_val]
                    code_map = choice_maps.get(str(field_name)) or {}
                    mapped: list[str] = []
                    bad = False
                    for item in seq:
                        if item is None or str(item).strip() == "":
                            continue
                        mv = _map_metsights_choice_to_option_value(
                            raw_code=item,
                            code_to_label=code_map,
                            db_options=db_opts,
                        )
                        if mv is None:
                            skipped_questions.append(f"{resource}.{field_name}:{item!r}")
                            bad = True
                            break
                        if mv not in mapped:
                            mapped.append(mv)
                    if bad or not mapped:
                        continue
                    answer = mapped
                elif qtype == "single_choice":
                    code_map = choice_maps.get(str(field_name)) or {}
                    mv = _map_metsights_choice_to_option_value(
                        raw_code=raw_val,
                        code_to_label=code_map,
                        db_options=db_opts,
                    )
                    if mv is None:
                        skipped_questions.append(f"{resource}.{field_name}:{raw_val!r}")
                        continue
                    answer = mv
                else:
                    skipped_questions.append(f"{resource}.{field_name}.type={qtype}")
                    continue

                existing = await self._questionnaire.get_response_by_instance_and_question_id(
                    db,
                    assessment_instance_id=assessment_instance_id,
                    question_id=int(qdef.question_id),
                )
                now = datetime.now(timezone.utc)
                if existing is not None:
                    existing.answer = answer
                    existing.category_id = int(category_id)
                    existing.submitted_at = now
                    await self._questionnaire.update_response(db, existing)
                else:
                    await self._questionnaire.create_response(
                        db,
                        QuestionnaireResponse(
                            assessment_instance_id=assessment_instance_id,
                            question_id=int(qdef.question_id),
                            category_id=int(category_id),
                            answer=answer,
                            submitted_at=now,
                        ),
                    )
                imported += 1
                resource_imported += 1
            return resource_imported

        def _payload_has_answer(payload: dict[str, Any]) -> bool:
            for fn, rv in payload.items():
                if fn in _METADATA_FIELDS or str(fn).endswith("_unit"):
                    continue
                if rv is None or rv == [] or rv == "":
                    continue
                return True
            return False

        for resource in resources:
            api_url = f"/records/{mrid}/{resource}/"
            sync_log = await self._create_pending_sync_log(
                db,
                engagement_id=engagement_id,
                user_id=participant_user_id,
                api_url=api_url,
            )

            try:
                payload = await self._metsights.get_record_subresource_or_none(record_id=mrid, resource=resource)
                source = resource

                # Fallback: Metsights returns 405 for GET on some sub-resources
                # (e.g. ``/fitness-parameters/`` and ``/vitals/``). Read the same data
                # from the record detail envelope in that case, or when the sub-resource
                # simply has no usable answers yet (e.g. FitPrint records always go here
                # because their fitness parameters are only exposed via record detail).
                if not isinstance(payload, dict) or not _payload_has_answer(payload):
                    detail = await _get_record_detail_cached()
                    if isinstance(detail, dict):
                        nested_key = _RESOURCE_TO_DETAIL_FIELD.get(resource)
                        nested = detail.get(nested_key) if nested_key else None
                        if isinstance(nested, dict) and _payload_has_answer(nested):
                            payload = nested
                            source = f"record_detail.{nested_key}"

                if not isinstance(payload, dict) or not _payload_has_answer(payload):
                    await self._finalize_sync_log(
                        db,
                        sync_log_id=sync_log.sync_log_id,
                        status="skipped",
                        response_payload={"skipped": True, "reason": "no_ingestable_data"},
                    )
                    continue

                skipped_before = len(skipped_questions)
                opt_env = await self._metsights.options_record_subresource(record_id=mrid, resource=resource)
                choice_maps = _build_field_choice_maps(opt_env if isinstance(opt_env, dict) else {})
                resource_imported = await _ingest_payload(source, payload, choice_maps)
                resource_skipped = skipped_questions[skipped_before:]
                await self._finalize_sync_log(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="success",
                    response_payload={"imported": resource_imported, "skipped": resource_skipped},
                )
            except Exception as exc:
                await self._finalize_sync_log(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="failed",
                    error_message=str(exc),
                )
                raise

        return {
            "assessment_instance_id": assessment_instance_id,
            "metsights_record_id": mrid,
            "responses_upserted": imported,
            "skipped_categories": [],
            "skipped_questions": skipped_questions,
        }

    async def _find_fitprint_instance_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> AssessmentInstance | None:
        """Return the user's FitPrint (type ``7``) instance in an engagement, if any."""

        instances = await self._assessments.list_instances_for_engagement(db, engagement_id=int(engagement_id))
        for inst in instances:
            if int(inst.user_id) != int(user_id):
                continue
            package = await self._assessments.get_package_by_id(db, int(inst.package_id))
            if package is None or (package.assessment_type_code or "").strip() != "7":
                continue
            if (inst.metsights_record_id or "").strip():
                return inst
        return None

    async def _patch_metsights_sections(
        self,
        *,
        db: AsyncSession,
        engagement_id: int | None,
        user_id: int | None,
        record_id: str,
        type_code: str,
        merged: dict[str, Any],
        mark_complete: bool,
        options_cache: dict[str, dict[str, _FieldMeta]] | None,
        patched: list[str],
        skipped_sections: list[str],
        section_errors: list[str] | None = None,
    ) -> None:
        """Push merged questionnaire fields to Metsights sub-resources for one record."""

        mrid = (record_id or "").strip()
        if not mrid:
            return

        async def _patch_section(resource: str, body: dict[str, Any]) -> None:
            api_url = f"/records/{mrid}/{resource}/"
            payload = {k: v for k, v in body.items() if k not in _METADATA_FIELDS}
            if not payload:
                logger.info(
                    "Skipping Metsights %s for record %s: empty payload (no matching questionnaire keys)",
                    resource,
                    mrid,
                )
                skipped_sections.append(resource)
                await self._log_skipped_metsights_sync(
                    db,
                    engagement_id=engagement_id,
                    user_id=user_id,
                    api_url=api_url,
                    reason="empty_payload",
                )
                return
            field_meta = await self._fetch_field_metadata_for_resource(mrid, resource, cache=options_cache)
            if field_meta:
                payload = _validate_payload_against_options(payload, field_meta)
                if not payload:
                    logger.warning(
                        "Metsights %s for record %s: all fields invalid after validation",
                        resource,
                        mrid,
                    )
                    skipped_sections.append(resource)
                    if section_errors is not None:
                        section_errors.append(resource)
                    await self._log_skipped_metsights_sync(
                        db,
                        engagement_id=engagement_id,
                        user_id=user_id,
                        api_url=api_url,
                        reason="all_fields_invalid",
                    )
                    return
            if mark_complete and resource in _METSIGHTS_QUESTIONNAIRE_RESOURCES:
                payload["is_complete"] = True
            logger.info("Pushing to Metsights %s for record %s: %s", resource, mrid, payload)

            sync_log = await self._create_pending_sync_log(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                api_url=api_url,
                request_payload=dict(payload),
            )
            try:
                await self._metsights.upsert_record_subresource(record_id=mrid, resource=resource, body=payload)
                patched.append(resource)
                await self._finalize_sync_log(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="success",
                    response_payload={"pushed": True},
                )
            except Exception as exc:
                await self._finalize_sync_log(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="failed",
                    error_message=str(exc),
                )
                logger.warning(
                    "Metsights push %s for record %s failed: %s — payload was: %s",
                    resource,
                    mrid,
                    exc,
                    payload,
                )
                if section_errors is not None:
                    section_errors.append(resource)
                else:
                    raise

        tc = (type_code or "").strip()
        if tc in ("1", "2"):
            phys = _pick_metsights_payload_for_bases(merged, _METSIGHTS_SUBMIT_PHYSICAL_KEYS)
            vit = _pick_metsights_payload_for_bases(merged, _METSIGHTS_SUBMIT_VITALS_KEYS)
            diet = _pick_metsights_payload_for_bases(merged, _METSIGHTS_SUBMIT_DIET_LIFESTYLE_KEYS)
            await _patch_section("physical-measurement", phys)
            await _patch_section("vitals", vit)
            await _patch_section("diet-lifestyle-parameters", diet)
        elif tc == "7":
            bases = (
                _METSIGHTS_SUBMIT_PHYSICAL_KEYS
                | _METSIGHTS_SUBMIT_DIET_LIFESTYLE_KEYS
                | _METSIGHTS_SUBMIT_FITNESS_ONLY_KEYS
            )
            fit = _pick_metsights_payload_for_bases(merged, bases)
            await _patch_section("fitness-parameters", fit)

    async def _fetch_field_metadata_for_resource(
        self,
        record_id: str,
        resource: str,
        cache: dict[str, dict[str, _FieldMeta]] | None = None,
    ) -> dict[str, _FieldMeta]:
        """Fetch OPTIONS for a Metsights resource and return field metadata.

        When *cache* is provided, the result is stored under ``resource`` and
        reused on subsequent calls with the same *resource* key.  This avoids
        redundant OPTIONS calls when pushing multiple participants.
        """
        if cache is not None and resource in cache:
            return cache[resource]
        try:
            opts = await self._metsights.options_record_subresource(record_id=record_id, resource=resource)
            result = _extract_field_metadata_from_options(opts)
        except Exception:
            result = {}
        if cache is not None:
            cache[resource] = result
        return result

    async def push_questionnaire_to_metsights_for_submit(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        current_user_id: int,
        source_assessment_instance_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """POST or PATCH Metsights record subresources from local ``questionnaire_responses`` (submit path).

        When *source_assessment_instance_ids* is provided, responses are aggregated
        from all listed instances (order matters -- later entries win on duplicate
        question keys).  The target instance's ``metsights_record_id`` and package
        type determine *where* the data is pushed.
        """

        instance = await self._assessments.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if instance is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")
        if int(instance.user_id) != int(current_user_id):
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        mrid = (instance.metsights_record_id or "").strip()
        package = await self._assessments.get_package_by_id(db, int(instance.package_id))
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Assessment package not found")

        type_code = (package.assessment_type_code or "").strip()
        if type_code not in ("1", "2", "7"):
            return {"pushed": False, "reason": "assessment_type_has_no_metsights_questionnaire_resources"}

        if not mrid:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment has no Metsights record id; cannot push questionnaire",
            )

        # Determine which instance ids to pull responses from.
        effective_source_ids: list[int]
        if source_assessment_instance_ids:
            for src_id in source_assessment_instance_ids:
                if int(src_id) == int(assessment_instance_id):
                    continue
                src = await self._assessments.get_instance_by_id(db, assessment_instance_id=int(src_id))
                if src is None:
                    raise AppError(
                        status_code=422,
                        error_code="INVALID_STATE",
                        message=f"Source assessment instance {src_id} does not exist",
                    )
                if int(src.user_id) != int(current_user_id):
                    raise AppError(
                        status_code=422,
                        error_code="INVALID_STATE",
                        message=f"Source assessment instance {src_id} belongs to a different user",
                    )
            effective_source_ids = list(source_assessment_instance_ids)
        else:
            effective_source_ids = [assessment_instance_id]

        responses = await self._questionnaire.list_responses_for_instances(
            db,
            assessment_instance_ids=effective_source_ids,
        )

        # Build an ordered list so that responses from later source ids overwrite earlier ones.
        source_order = {sid: idx for idx, sid in enumerate(effective_source_ids)}
        responses.sort(key=lambda r: (source_order.get(int(r.assessment_instance_id), 0), int(r.response_id)))

        qids = list({int(r.question_id) for r in responses})
        defs_map = await self._questionnaire.get_definitions_by_ids(db, question_ids=qids)

        merged: dict[str, Any] = {}
        for resp in responses:
            qdef = defs_map.get(int(resp.question_id))
            if qdef is None:
                continue
            key = (qdef.question_key or "").strip()
            if not key:
                continue
            merged.update(_answer_to_metsights_fields(key, str(qdef.question_type or ""), resp.answer))

        patched: list[str] = []
        skipped_sections: list[str] = []

        engagement_id = int(instance.engagement_id) if instance.engagement_id else None
        participant_user_id = int(current_user_id)

        await self._patch_metsights_sections(
            db=db,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            record_id=mrid,
            type_code=type_code,
            merged=merged,
            mark_complete=True,
            options_cache=None,
            patched=patched,
            skipped_sections=skipped_sections,
        )

        if type_code in ("1", "2") and instance.engagement_id is not None:
            fitprint_inst = await self._find_fitprint_instance_for_user_engagement(
                db,
                user_id=int(current_user_id),
                engagement_id=int(instance.engagement_id),
            )
            if fitprint_inst is not None:
                fp_rid = (fitprint_inst.metsights_record_id or "").strip()
                if fp_rid:
                    await self._patch_metsights_sections(
                        db=db,
                        engagement_id=engagement_id,
                        user_id=participant_user_id,
                        record_id=fp_rid,
                        type_code="7",
                        merged=merged,
                        mark_complete=True,
                        options_cache=None,
                        patched=patched,
                        skipped_sections=skipped_sections,
                    )

        if not patched and skipped_sections:
            logger.warning(
                "Metsights push for instance %s (record %s) produced no data — "
                "all sections skipped. effective_source_ids=%s, response_count=%s, merged_keys=%s",
                assessment_instance_id, mrid, effective_source_ids, len(responses), list(merged.keys()),
            )

        return {
            "assessment_instance_id": assessment_instance_id,
            "metsights_record_id": mrid,
            "resources_patched": patched,
        }

    async def push_questionnaire_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        source_assessment_instance_ids: list[int] | None = None,
        options_cache: dict[str, dict[str, _FieldMeta]] | None = None,
    ) -> dict[str, Any]:
        """Push local questionnaire answers to Metsights for a single instance (employee path).

        Unlike ``push_questionnaire_to_metsights_for_submit`` this skips user
        ownership checks so employees can push on behalf of participants.
        Does NOT set ``is_complete`` since this is a data push, not a final submit.
        Each section is pushed independently so one failure does not block others.
        Returns a summary dict; never raises on empty responses (just reports skipped).

        Pass *options_cache* (a mutable dict) to reuse OPTIONS metadata across
        multiple calls, avoiding redundant Metsights OPTIONS requests when
        pushing an entire engagement.
        """

        instance = await self._assessments.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if instance is None:
            return {"assessment_instance_id": assessment_instance_id, "pushed": False, "reason": "instance_not_found"}

        engagement_id = int(instance.engagement_id) if instance.engagement_id else None
        participant_user_id = int(instance.user_id)

        mrid = (instance.metsights_record_id or "").strip()
        if not mrid:
            await self._log_skipped_metsights_sync(
                db,
                engagement_id=engagement_id,
                user_id=participant_user_id,
                api_url=self._push_placeholder_url(),
                reason="no_metsights_record_id",
            )
            return {"assessment_instance_id": assessment_instance_id, "pushed": False, "reason": "no_metsights_record_id"}

        package = await self._assessments.get_package_by_id(db, int(instance.package_id))
        if package is None:
            return {"assessment_instance_id": assessment_instance_id, "pushed": False, "reason": "package_not_found"}

        type_code = (package.assessment_type_code or "").strip()
        if type_code not in ("1", "2", "7"):
            return {"assessment_instance_id": assessment_instance_id, "pushed": False, "reason": "unsupported_assessment_type"}

        effective_source_ids: list[int]
        if source_assessment_instance_ids:
            effective_source_ids = list(source_assessment_instance_ids)
        else:
            effective_source_ids = [assessment_instance_id]

        responses = await self._questionnaire.list_responses_for_instances(
            db,
            assessment_instance_ids=effective_source_ids,
        )
        if not responses:
            await self._log_skipped_metsights_sync(
                db,
                engagement_id=engagement_id,
                user_id=participant_user_id,
                api_url=self._push_placeholder_url(record_id=mrid),
                reason="no_responses",
            )
            return {"assessment_instance_id": assessment_instance_id, "pushed": False, "reason": "no_responses"}

        source_order = {sid: idx for idx, sid in enumerate(effective_source_ids)}
        responses.sort(key=lambda r: (source_order.get(int(r.assessment_instance_id), 0), int(r.response_id)))

        qids = list({int(r.question_id) for r in responses})
        defs_map = await self._questionnaire.get_definitions_by_ids(db, question_ids=qids)

        merged: dict[str, Any] = {}
        for resp in responses:
            qdef = defs_map.get(int(resp.question_id))
            if qdef is None:
                continue
            key = (qdef.question_key or "").strip()
            if not key:
                continue
            merged.update(_answer_to_metsights_fields(key, str(qdef.question_type or ""), resp.answer))

        if not merged:
            await self._log_skipped_metsights_sync(
                db,
                engagement_id=engagement_id,
                user_id=participant_user_id,
                api_url=self._push_placeholder_url(record_id=mrid),
                reason="no_mappable_answers",
            )
            return {"assessment_instance_id": assessment_instance_id, "pushed": False, "reason": "no_mappable_answers"}

        patched: list[str] = []
        skipped_sections: list[str] = []
        section_errors: list[str] = []

        await self._patch_metsights_sections(
            db=db,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            record_id=mrid,
            type_code=type_code,
            merged=merged,
            mark_complete=True,
            options_cache=options_cache,
            patched=patched,
            skipped_sections=skipped_sections,
            section_errors=section_errors,
        )

        if type_code in ("1", "2") and instance.engagement_id is not None:
            fitprint_inst = await self._find_fitprint_instance_for_user_engagement(
                db,
                user_id=int(instance.user_id),
                engagement_id=int(instance.engagement_id),
            )
            if fitprint_inst is not None:
                fp_rid = (fitprint_inst.metsights_record_id or "").strip()
                if fp_rid:
                    await self._patch_metsights_sections(
                        db=db,
                        engagement_id=engagement_id,
                        user_id=participant_user_id,
                        record_id=fp_rid,
                        type_code="7",
                        merged=merged,
                        mark_complete=True,
                        options_cache=options_cache,
                        patched=patched,
                        skipped_sections=skipped_sections,
                        section_errors=section_errors,
                    )

        if patched:
            now = datetime.now(timezone.utc)
            for resp in responses:
                if resp.submitted_at is None:
                    resp.submitted_at = now
                    await self._questionnaire.update_response(db, resp)

        return {
            "assessment_instance_id": assessment_instance_id,
            "metsights_record_id": mrid,
            "pushed": len(patched) > 0,
            "resources_patched": patched,
            "section_errors": section_errors,
        }

    # ------------------------------------------------------------------
    # Strategy-based push (category-level submit)
    # ------------------------------------------------------------------

    async def submit_category_to_metsights(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
        category_key: str,
        category_of: str = "metsights",
    ) -> dict[str, Any]:
        """Push answers for a single Metsights category using the strategy engine."""
        from modules.assessments.repository import AssessmentsRepository

        assessments_repo = AssessmentsRepository()
        audit_repo = AuditRepository()

        category = await self._questionnaire.get_category_by_key_and_category_of(
            db, category_key=category_key, category_of=category_of,
        )
        if category is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Category '{category_key}' with category_of='{category_of}' does not exist",
            )

        instance = await self._assessments.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if instance is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")
        if int(instance.user_id) != int(user_id):
            raise AppError(status_code=403, error_code="FORBIDDEN", message="You do not have permission to perform this action")

        mrid = (instance.metsights_record_id or "").strip()
        if not mrid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment has no Metsights record id")

        engagement_id = int(instance.engagement_id) if instance.engagement_id else None

        all_instances = await assessments_repo.list_all_instances_for_engagement(db, engagement_id=engagement_id) if engagement_id else [instance]
        source_ids = [int(inst.assessment_instance_id) for inst in all_instances if int(inst.user_id) == int(user_id)]

        questions = await self._questionnaire.list_questions_by_category(db, category_id=int(category.category_id))
        if not questions:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="No questions found for this category")

        not_enabled = []
        for q in questions:
            sync_cfg = q.metsights_sync or {}
            push_cfg = sync_cfg.get("push") or {}
            if not push_cfg.get("enabled", False):
                not_enabled.append(q.question_key)
        if not_enabled:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message=f"Push not enabled for questions: {', '.join(not_enabled)}",
            )

        question_ids = [int(q.question_id) for q in questions]
        responses = await self._questionnaire.list_responses_for_instances(db, assessment_instance_ids=source_ids)
        responses_map: dict[int, Any] = {}
        for r in responses:
            if int(r.question_id) in question_ids:
                responses_map[int(r.question_id)] = r.answer

        api_path = _CATEGORY_KEY_TO_API_PATH.get(category_key)
        if api_path is None:
            raise AppError(status_code=422, error_code="INVALID_STATE", message=f"No Metsights API path for category '{category_key}'")

        metsights_payload: dict[str, Any] = {}
        for q in questions:
            answer = responses_map.get(int(q.question_id))
            if answer is None:
                continue
            sync_cfg = q.metsights_sync or {}
            fields = apply_push_strategy(q.question_key, answer, sync_cfg)
            metsights_payload.update(fields)

        api_url = f"/records/{mrid}/{api_path}/"

        sync_log = await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider="metsights",
                api_endpoint_url=api_url,
                request_payload=metsights_payload,
                status="pending",
            ),
        )

        try:
            field_meta = await self._fetch_field_metadata_for_resource(mrid, api_path, cache=None)
            if field_meta:
                metsights_payload = _validate_payload_against_options(metsights_payload, field_meta)
            metsights_payload["is_complete"] = True
            await self._metsights.upsert_record_subresource(record_id=mrid, resource=api_path, body=metsights_payload)
            await audit_repo.update_sync_log_status(
                db, sync_log_id=sync_log.sync_log_id, status="success", response_payload={"pushed": True},
            )
        except Exception as exc:
            await audit_repo.update_sync_log_status(
                db, sync_log_id=sync_log.sync_log_id, status="failed", error_message=str(exc),
            )
            raise AppError(
                status_code=502,
                error_code="METSIGHTS_PUSH_FAILED",
                message=f"Failed to push to Metsights: {exc}",
            ) from exc

        now = datetime.now(timezone.utc)

        category_responses = await self._questionnaire.list_responses_for_instance(
            db, assessment_instance_id=assessment_instance_id,
            category_id=int(category.category_id),
        )
        for resp in category_responses:
            if resp.submitted_at is None:
                resp.submitted_at = now
                await self._questionnaire.update_response(db, resp)

        from modules.questionnaire.service import QuestionnaireService

        q_service = QuestionnaireService(
            repository=self._questionnaire,
            users_repository=self._users,
        )
        is_complete = await q_service.is_category_complete(
            db,
            assessment_instance_id=assessment_instance_id,
            category_id=int(category.category_id),
            user_id=user_id,
        )

        progress = await assessments_repo.get_category_progress(
            db,
            assessment_instance_id=assessment_instance_id,
            category_id=int(category.category_id),
        )
        if is_complete:
            if progress is None:
                await assessments_repo.create_category_progress(
                    db,
                    AssessmentCategoryProgress(
                        assessment_instance_id=assessment_instance_id,
                        category_id=int(category.category_id),
                        status="complete",
                        completed_at=now,
                    ),
                )
            elif (progress.status or "").strip().lower() != "complete":
                progress.status = "complete"
                progress.completed_at = now
                await assessments_repo.update_category_progress(db, progress)
        else:
            if progress is not None and (progress.status or "").strip().lower() == "complete":
                progress.status = "incomplete"
                progress.completed_at = None
                await assessments_repo.update_category_progress(db, progress)

        return {
            "assessment_instance_id": assessment_instance_id,
            "category": category_key,
            "metsights_record_id": mrid,
            "status": "success",
            "fields_pushed": list(metsights_payload.keys()),
        }

    # ------------------------------------------------------------------
    # Strategy-based pull (category-level import)
    # ------------------------------------------------------------------

    async def import_category_from_metsights(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
        category_key: str,
        category_of: str = "metsights",
        reload: int = 0,
        employee_ok: bool = False,
    ) -> dict[str, Any]:
        """Pull answers for a single Metsights category using the strategy engine."""
        from modules.assessments.repository import AssessmentsRepository

        assessments_repo = AssessmentsRepository()
        audit_repo = AuditRepository()

        category = await self._questionnaire.get_category_by_key_and_category_of(
            db, category_key=category_key, category_of=category_of,
        )
        if category is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Category '{category_key}' with category_of='{category_of}' does not exist",
            )

        instance = await self._assessments.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if instance is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        self._ensure_sync_access(
            current_user_id=user_id,
            target_user_id=int(instance.user_id),
            employee_ok=employee_ok,
        )

        mrid = (instance.metsights_record_id or "").strip()
        if not mrid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment has no Metsights record id")

        engagement_id = int(instance.engagement_id) if instance.engagement_id else None
        api_path = _CATEGORY_KEY_TO_API_PATH.get(category_key)
        if api_path is None:
            raise AppError(status_code=422, error_code="INVALID_STATE", message=f"No Metsights API path for category '{category_key}'")

        if reload == 0:
            existing_responses = await self._questionnaire.list_responses_for_instance(
                db, assessment_instance_id=assessment_instance_id, category_id=int(category.category_id),
            )
            if existing_responses:
                await self._update_all_category_progress_for_instance(
                    db, assessments_repo=assessments_repo, instance=instance,
                )
                api_url = f"/records/{mrid}/{api_path}/"
                await self._log_skipped_metsights_sync(
                    db,
                    engagement_id=engagement_id,
                    user_id=int(instance.user_id),
                    api_url=api_url,
                    reason="responses already exist, use reload=1 to overwrite",
                )
                return {
                    "assessment_instance_id": assessment_instance_id,
                    "category": category_key,
                    "status": "skipped",
                    "reason": "responses already exist, use reload=1 to overwrite",
                }

        api_url = f"/records/{mrid}/{api_path}/"
        sync_log = await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=int(instance.user_id),
                provider="metsights",
                api_endpoint_url=api_url,
                status="pending",
            ),
        )

        try:
            metsights_payload = await self._metsights.get_record_subresource_or_none(record_id=mrid, resource=api_path)
            if not isinstance(metsights_payload, dict):
                detail = await self._metsights.get_record_detail(record_id=mrid)
                if isinstance(detail, dict):
                    nested_key = _RESOURCE_TO_DETAIL_FIELD.get(api_path)
                    nested = detail.get(nested_key) if nested_key else None
                    if isinstance(nested, dict):
                        metsights_payload = nested
                    else:
                        metsights_payload = {}
                else:
                    metsights_payload = {}
        except Exception as exc:
            await audit_repo.update_sync_log_status(
                db, sync_log_id=sync_log.sync_log_id, status="failed", error_message=str(exc),
            )
            raise AppError(
                status_code=502,
                error_code="METSIGHTS_PULL_FAILED",
                message=f"Failed to fetch from Metsights: {exc}",
            ) from exc

        imported = 0
        skipped: list[str] = []
        now = datetime.now(timezone.utc)

        for field_name, raw_val in metsights_payload.items():
            if field_name in _METADATA_FIELDS or str(field_name).endswith("_unit"):
                continue
            if raw_val is None or raw_val == [] or raw_val == "":
                continue

            qdef = await self._questionnaire.get_definition_by_key(db, question_key=str(field_name))
            if qdef is None:
                skipped.append(f"{field_name}:no_definition")
                continue

            sync_cfg = qdef.metsights_sync or {}
            pull_cfg = sync_cfg.get("pull") or {}
            if not pull_cfg.get("enabled", False):
                skipped.append(f"{field_name}:pull_not_enabled")
                continue

            answer = apply_pull_strategy(str(field_name), metsights_payload, sync_cfg)
            if answer is None:
                skipped.append(f"{field_name}:strategy_returned_none")
                continue

            existing = await self._questionnaire.get_response_by_instance_and_question_id(
                db, assessment_instance_id=assessment_instance_id, question_id=int(qdef.question_id),
            )
            if existing is not None:
                existing.answer = answer
                existing.category_id = int(category.category_id)
                existing.submitted_at = now
                await self._questionnaire.update_response(db, existing)
            else:
                await self._questionnaire.create_response(
                    db,
                    QuestionnaireResponse(
                        assessment_instance_id=assessment_instance_id,
                        question_id=int(qdef.question_id),
                        category_id=int(category.category_id),
                        answer=answer,
                        submitted_at=now,
                    ),
                )
            imported += 1

        await audit_repo.update_sync_log_status(
            db, sync_log_id=sync_log.sync_log_id, status="success",
            response_payload={"imported": imported, "skipped": skipped},
        )

        await self._update_all_category_progress_for_instance(
            db, assessments_repo=assessments_repo, instance=instance,
        )

        return {
            "assessment_instance_id": assessment_instance_id,
            "category": category_key,
            "metsights_record_id": mrid,
            "responses_imported": imported,
            "skipped": skipped,
        }

    async def _update_all_category_progress_for_instance(
        self,
        db: AsyncSession,
        *,
        assessments_repo: "AssessmentsRepository",
        instance: AssessmentInstance,
    ) -> None:
        """Check all categories for an instance and update progress status.

        Delegates to QuestionnaireService.is_category_complete so that
        visibility rules, prefill, and full answer validation are applied
        consistently with the user-facing upsert path.
        """
        from modules.questionnaire.service import QuestionnaireService

        q_service = QuestionnaireService(
            repository=self._questionnaire,
            users_repository=self._users,
        )

        package_categories = await assessments_repo.list_package_categories(
            db, package_id=int(instance.package_id),
        )
        now = datetime.now(timezone.utc)

        for link in package_categories:
            cat = await self._questionnaire.get_category_by_id(db, link.category_id)
            if cat is None:
                continue

            all_required_answered = await q_service.is_category_complete(
                db,
                assessment_instance_id=int(instance.assessment_instance_id),
                category_id=int(cat.category_id),
                user_id=int(instance.user_id),
            )

            progress = await assessments_repo.get_category_progress(
                db,
                assessment_instance_id=int(instance.assessment_instance_id),
                category_id=int(cat.category_id),
            )

            if all_required_answered:
                if progress is None:
                    await assessments_repo.create_category_progress(
                        db,
                        AssessmentCategoryProgress(
                            assessment_instance_id=int(instance.assessment_instance_id),
                            category_id=int(cat.category_id),
                            status="complete",
                            completed_at=now,
                        ),
                    )
                elif (progress.status or "").strip().lower() != "complete":
                    progress.status = "complete"
                    progress.completed_at = now
                    await assessments_repo.update_category_progress(db, progress)
            else:
                if progress is not None and (progress.status or "").strip().lower() == "complete":
                    progress.status = "incomplete"
                    progress.completed_at = None
                    await assessments_repo.update_category_progress(db, progress)
