"""Aurae Face Scan orchestration."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.aurae import client as aurae_client
from modules.aurae.units import (
    extract_scale_answer,
    height_to_cm,
    normalize_aurae_gender,
    waist_to_inches,
    weight_to_kg,
)
from modules.engagements.repository import EngagementsRepository
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository

logger = logging.getLogger(__name__)

_PROVIDER = "aurae"
_VIFC = "vifc"
_ANTHROPOMETRY_KEYS = ("height", "weight", "waist_circumference")


class AuraeService:
    """Start Aurae VIFC face scans for assessment instances."""

    def __init__(
        self,
        *,
        assessments_repository: AssessmentsRepository | None = None,
        engagements_repository: EngagementsRepository | None = None,
        users_repository: UsersRepository | None = None,
        questionnaire_repository: QuestionnaireRepository | None = None,
    ):
        self._assessments_repo = assessments_repository or AssessmentsRepository()
        self._engagements_repo = engagements_repository or EngagementsRepository()
        self._users_repo = users_repository or UsersRepository()
        self._questionnaire_repo = questionnaire_repository or QuestionnaireRepository()

    async def start_face_scan(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
    ) -> dict[str, str]:
        """Return the Aurae face-scan link for a VIFC assessment owned by ``user_id``.

        Reuses ``engagement_participants.face_scan_link`` when already set;
        otherwise calls Aurae token + onboard, persists the link, and logs both
        external calls under ``integration_sync_logs`` with ``provider=aurae``.
        """
        row = await self._assessments_repo.get_instance_for_user(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )

        instance, package = row
        self._validate_vifc_package(instance=instance, package=package)

        if instance.engagement_id is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Assessment is not linked to an engagement",
            )

        participant = await self._engagements_repo.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=int(instance.engagement_id),
        )
        if participant is None:
            raise AppError(
                status_code=400,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Engagement participant not found for this assessment",
            )

        existing_link = (participant.face_scan_link or "").strip()
        if existing_link:
            return {"link": existing_link}

        user = await self._users_repo.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        gender = normalize_aurae_gender(user.gender)
        if gender is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="User gender is required for face scan (male or female)",
            )

        height_cm, weight_kg, waist_in = await self._resolve_anthropometry(
            db, assessment_instance_id=int(instance.assessment_instance_id)
        )

        email = (user.email or "").strip()
        phone = (user.phone or "").strip()
        first_name = (user.first_name or "").strip()
        last_name = (user.last_name or "").strip()
        if not email or not phone or not first_name or not last_name:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="User email, phone, first_name, and last_name are required for face scan",
            )

        dob = user.date_of_birth.isoformat() if user.date_of_birth is not None else None
        if not dob:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="User date of birth is required for face scan",
            )

        org_code = (settings.AURAE_ORG_CODE or "").strip()
        onboard_payload: dict[str, Any] = {
            "email": email,
            "phone_number": phone,
            "first_name": first_name,
            "last_name": last_name,
            "org_code": org_code,
            "api_customer_id": str(instance.assessment_instance_id),
            "dob": dob,
            "components": ["VIFC"],
            "height": height_cm,
            "weight": weight_kg,
            "waist": waist_in,
            "gender": gender,
        }

        engagement_id = int(instance.engagement_id)

        async def _get_token() -> str:
            return await aurae_client.get_token()

        token = await tracked_integration_call(
            db,
            provider=_PROVIDER,
            api_url=aurae_client.token_url(),
            engagement_id=engagement_id,
            user_id=user_id,
            request_payload={"org_code": org_code},
            operation=_get_token,
            persist=False,
        )
        if not token:
            raise AppError(
                status_code=502,
                error_code="AURAE_TOKEN_FAILED",
                message="Failed to obtain Aurae auth token",
            )

        async def _onboard() -> dict[str, Any]:
            return await aurae_client.onboard_user(token=token, payload=onboard_payload)

        onboard_result = await tracked_integration_call(
            db,
            provider=_PROVIDER,
            api_url=aurae_client.onboard_url(),
            engagement_id=engagement_id,
            user_id=user_id,
            request_payload=onboard_payload,
            operation=_onboard,
            persist=False,
        )
        if not onboard_result or not isinstance(onboard_result, dict):
            raise AppError(
                status_code=502,
                error_code="AURAE_ONBOARD_FAILED",
                message="Failed to onboard user with Aurae",
            )

        link = (onboard_result.get("link") or "").strip()
        if not link:
            raise AppError(
                status_code=502,
                error_code="AURAE_ONBOARD_FAILED",
                message="Aurae onboard response did not include a link",
            )

        participant.face_scan_link = link
        await self._engagements_repo.update_participant(db, participant)

        return {"link": link}

    @staticmethod
    def _validate_vifc_package(*, instance, package) -> None:
        if package is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Assessment package is missing",
            )
        package_code = (getattr(package, "package_code", None) or "").strip().lower()
        type_code = (getattr(package, "assessment_type_code", None) or "").strip().lower()
        package_status = (getattr(package, "status", None) or "").strip().lower()
        instance_status = (getattr(instance, "status", None) or "").strip().lower()

        if package_code != _VIFC:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Assessment package code must be vifc",
            )
        if type_code != _VIFC:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Assessment type code must be vifc",
            )
        if package_status != "active":
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Assessment package must be Active",
            )
        if instance_status != "active":
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Assessment instance must be Active",
            )

    async def _resolve_anthropometry(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> tuple[int, int, int]:
        defs_by_key: dict[str, int] = {}
        for key in _ANTHROPOMETRY_KEYS:
            definition = await self._questionnaire_repo.get_definition_by_key(db, question_key=key)
            if definition is None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=f"Questionnaire definition for {key} is missing",
                )
            defs_by_key[key] = int(definition.question_id)

        responses = await self._questionnaire_repo.list_responses_for_instance(
            db, assessment_instance_id=assessment_instance_id
        )
        answers_by_qid = {int(r.question_id): r.answer for r in responses}

        missing: list[str] = []
        height_cm: int | None = None
        weight_kg: int | None = None
        waist_in: int | None = None

        height_answer = answers_by_qid.get(defs_by_key["height"])
        height_val, height_unit = extract_scale_answer(height_answer)
        if height_val is None:
            missing.append("height")
        else:
            try:
                height_cm = height_to_cm(height_val, height_unit)
            except ValueError:
                missing.append("height")

        weight_answer = answers_by_qid.get(defs_by_key["weight"])
        weight_val, weight_unit = extract_scale_answer(weight_answer)
        if weight_val is None:
            missing.append("weight")
        else:
            try:
                weight_kg = weight_to_kg(weight_val, weight_unit)
            except ValueError:
                missing.append("weight")

        waist_answer = answers_by_qid.get(defs_by_key["waist_circumference"])
        waist_val, waist_unit = extract_scale_answer(waist_answer)
        if waist_val is None:
            missing.append("waist")
        else:
            try:
                waist_in = waist_to_inches(waist_val, waist_unit)
            except ValueError:
                missing.append("waist")

        if missing or height_cm is None or weight_kg is None or waist_in is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Missing or invalid anthropometry for face scan: {', '.join(missing) or 'unknown'}",
            )

        return height_cm, weight_kg, waist_in
