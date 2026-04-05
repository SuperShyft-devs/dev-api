"""Metsights record sync and questionnaire answer import."""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.diagnostics.models import DiagnosticPackage
from modules.assessments.service import AssessmentsService
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

# Top-level keys on Metsights record detail whose nested objects map 1:1 to our category_key.
_RECORD_DETAIL_CATEGORY_KEYS: tuple[str, ...] = (
    "physical_measurement",
    "vital_parameter",
    "diet_lifestyle_parameter",
    "fitness_parameter",
)

_METADATA_FIELDS = frozenset(
    {
        "id",
        "created_at",
        "updated_at",
        "is_complete",
    }
)

# (measurement_field_name, metsights_unit_code) -> canonical unit string stored in DB (scale option_value).
_METSIGHTS_UNIT_TO_CANONICAL: dict[tuple[str, str], str] = {
    ("height", "0"): "cm",
    ("weight", "0"): "kg",
    ("waist_circumference", "0"): "cm",
    ("hip_circumference", "0"): "cm",
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


def _scale_answer(field: str, obj: dict[str, Any], value: Any) -> dict[str, Any] | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    unit_raw = obj.get(f"{field}_unit")
    if unit_raw is None:
        return None
    ukey = str(unit_raw).strip()
    canonical = _METSIGHTS_UNIT_TO_CANONICAL.get((field, ukey))
    if canonical is None:
        canonical = _METSIGHTS_UNIT_TO_CANONICAL.get((field, ukey.lower()))
    if canonical is None:
        return None
    return {"value": float(value), "unit": canonical}


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
                )
            else:
                if int(engagement.assessment_package_id) != int(package.package_id):
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

        detail = await self._metsights.get_record_detail(record_id=mrid)
        if not isinstance(detail, dict):
            raise AppError(status_code=503, error_code="EXTERNAL_SERVICE_UNAVAILABLE", message="Unexpected Metsights payload")

        imported = 0
        skipped_categories: list[str] = []
        skipped_questions: list[str] = []

        for cat_key in _RECORD_DETAIL_CATEGORY_KEYS:
            category = await self._questionnaire.get_category_by_key(db, category_key=cat_key)
            if category is None:
                skipped_categories.append(cat_key)
                continue

            payload = detail.get(cat_key)
            if not isinstance(payload, dict):
                continue

            questions = await self._questionnaire.list_questions_by_category(db, category_id=category.category_id)
            by_key = {q.question_key: q for q in questions if q.question_key}

            for field_name, raw_val in payload.items():
                if field_name in _METADATA_FIELDS:
                    continue
                if str(field_name).endswith("_unit"):
                    continue
                if raw_val is None:
                    continue

                qdef = by_key.get(field_name)
                if qdef is None:
                    skipped_questions.append(f"{cat_key}.{field_name}")
                    continue

                qtype = (qdef.question_type or "").strip().lower()
                answer: Any = None

                unit_field = f"{field_name}_unit"
                if unit_field in payload and qtype == "scale":
                    answer = _scale_answer(field_name, payload, raw_val)
                    if answer is None:
                        skipped_questions.append(f"{cat_key}.{field_name}")
                        continue
                elif qtype in ("multi_choice", "multiple_choice"):
                    if not isinstance(raw_val, list):
                        skipped_questions.append(f"{cat_key}.{field_name}")
                        continue
                    answer = [str(x).strip() for x in raw_val if x is not None and str(x).strip() != ""]
                elif qtype in ("single_choice", "text", "string"):
                    answer = str(raw_val).strip() if raw_val is not None else None
                    if answer == "":
                        continue
                else:
                    # Fallback: store primitives as string codes
                    if isinstance(raw_val, (int, float)) and not isinstance(raw_val, bool):
                        answer = str(raw_val)
                    elif isinstance(raw_val, str):
                        answer = raw_val.strip()
                    else:
                        skipped_questions.append(f"{cat_key}.{field_name}")
                        continue

                existing = await self._questionnaire.get_response_by_instance_and_question(
                    db,
                    assessment_instance_id=assessment_instance_id,
                    category_id=category.category_id,
                    question_id=qdef.question_id,
                )
                now = datetime.now(timezone.utc)
                if existing is not None:
                    existing.answer = answer
                    existing.submitted_at = now
                    await self._questionnaire.update_response(db, existing)
                else:
                    await self._questionnaire.create_response(
                        db,
                        QuestionnaireResponse(
                            assessment_instance_id=assessment_instance_id,
                            question_id=qdef.question_id,
                            category_id=category.category_id,
                            answer=answer,
                            submitted_at=now,
                        ),
                    )
                imported += 1

        return {
            "assessment_instance_id": assessment_instance_id,
            "metsights_record_id": mrid,
            "responses_upserted": imported,
            "skipped_categories": skipped_categories,
            "skipped_questions": skipped_questions,
        }
