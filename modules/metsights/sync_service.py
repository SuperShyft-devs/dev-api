"""Metsights record sync and questionnaire answer import."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.diagnostics.models import DiagnosticPackage
from modules.assessments.service import AssessmentsService
from modules.engagements.models import EngagementKind
from modules.engagements.service import EngagementsService
from modules.metsights.service import MetsightsService
from modules.platform_settings.service import PlatformSettingsService
from modules.questionnaire.models import QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository

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

# (measurement_field_name, metsights_unit_code) -> unit aligned with ``questionnaire_options.option_value`` when possible.
_METSIGHTS_UNIT_TO_CANONICAL: dict[tuple[str, str], str] = {
    ("height", "0"): "cm",
    ("height", "1"): "ft_in",
    ("weight", "0"): "kg",
    ("weight", "1"): "lb",
    ("waist_circumference", "0"): "cm",
    ("waist_circumference", "1"): "in",
    ("hip_circumference", "0"): "cm",
    ("hip_circumference", "1"): "in",
    ("body_fat", "0"): "%",
    ("bmi", "0"): "kg/m²",
    ("systolic_blood_pressure", "0"): "mmhg",
    ("diastolic_blood_pressure", "0"): "mmhg",
    ("heart_rate", "0"): "bpm",
    ("resting_heart_rate", "0"): "bpm",
    ("respiratory_rate", "0"): "breaths/min",
    ("hrv_sdnn", "0"): "ms",
    ("daily_active_duration", "0"): "min",
    ("weight_loss_goal", "0"): "kg",
}


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
    canonical = _METSIGHTS_UNIT_TO_CANONICAL.get((field, ukey))
    if canonical is None:
        canonical = _METSIGHTS_UNIT_TO_CANONICAL.get((field, ukey.lower()))
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
                    )
                except AppError as exc:
                    errors.append({"metsights_record_id": mrid, "reason": str(exc.message)})
                    continue
                except Exception as exc:
                    logger.exception("enroll failed for metsights sync")
                    errors.append({"metsights_record_id": mrid, "reason": str(exc)})
                    continue

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

        imported = 0
        skipped_questions: list[str] = []
        got_vitals_payload = False

        async def _ingest_payload(
            resource: str,
            payload: dict[str, Any],
            choice_maps: dict[str, dict[str, str]],
        ) -> None:
            nonlocal imported
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

        for resource in resources:
            payload = await self._metsights.get_record_subresource_or_none(record_id=mrid, resource=resource)
            if not isinstance(payload, dict):
                continue
            if resource == "vitals":
                for fn, rv in payload.items():
                    if fn in _METADATA_FIELDS or str(fn).endswith("_unit"):
                        continue
                    if rv is not None and rv != []:
                        got_vitals_payload = True
                        break

            opt_env = await self._metsights.options_record_subresource(record_id=mrid, resource=resource)
            choice_maps = _build_field_choice_maps(opt_env if isinstance(opt_env, dict) else {})
            await _ingest_payload(resource, payload, choice_maps)

        if type_code in ("1", "2") and not got_vitals_payload:
            detail = await self._metsights.get_record_detail(record_id=mrid)
            if isinstance(detail, dict):
                vp = detail.get("vital_parameter")
                if isinstance(vp, dict) and vp:
                    opt_env = await self._metsights.options_record_subresource(record_id=mrid, resource="vitals")
                    cm = _build_field_choice_maps(opt_env if isinstance(opt_env, dict) else {})
                    await _ingest_payload("record_detail.vital_parameter", vp, cm)

        return {
            "assessment_instance_id": assessment_instance_id,
            "metsights_record_id": mrid,
            "responses_upserted": imported,
            "skipped_categories": [],
            "skipped_questions": skipped_questions,
        }
