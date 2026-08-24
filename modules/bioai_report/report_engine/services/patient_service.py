"""Fetch patient / profile demographics for BioReport enrichment.

Runs automatically before report assembly for a given MetSights ``record_id``.
Sources (local first, then MetSights):

- Local User + questionnaire (height / weight / BMI) when a DB session is available
- MetSights ``GET /records/{record_id}/``
- MetSights ``GET /profiles/{profile_id}/`` when identity fields are still missing
- MetSights ``physical-measurement`` when anthropometrics are still missing

Missing fields never abort report generation — available values are merged and
the pipeline continues.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.bioai_report.report_engine.utils.patient_enrichment import (
    extract_demographics_from_physical,
    extract_demographics_from_profile,
    extract_demographics_from_questionnaire,
    extract_demographics_from_record,
    extract_demographics_from_user,
    extract_patient_identifiers,
    merge_patient_into_assessment,
    missing_demographic_fields,
    needs_patient_enrichment,
    resolve_profile_id,
)
from modules.metsights.service import MetsightsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository

logger = logging.getLogger(__name__)

EventReporter = Callable[[str, str, str], None]

_PHYSICAL_QUESTION_KEYS = ("height", "weight", "bmi", "gender", "sex")


class PatientProfileService:
    """Load patient demographics from local User/questionnaire APIs, with MetSights fallback."""

    def __init__(
        self,
        metsights_service: MetsightsService,
        *,
        assessments_repository: AssessmentsRepository | None = None,
        users_repository: UsersRepository | None = None,
        questionnaire_repository: QuestionnaireRepository | None = None,
        event_reporter: EventReporter | None = None,
    ) -> None:
        self._metsights = metsights_service
        self._assessments = assessments_repository
        self._users = users_repository
        self._questionnaire = questionnaire_repository
        self._event_reporter = event_reporter

    def _emit(self, stage: str, status: str, detail: str) -> None:
        if self._event_reporter is not None:
            self._event_reporter(stage, status, detail)
        elif status in {"failed", "empty", "skipped"}:
            logger.warning("Patient enrichment [%s] %s: %s", stage, status, detail)
        else:
            logger.debug("Patient enrichment [%s] %s: %s", stage, status, detail)

    async def _fetch_record_detail(self, *, record_id: str) -> dict[str, Any] | None:
        try:
            detail = await self._metsights.get_record_detail(record_id=record_id)
        except AppError as exc:
            self._emit(
                "record_detail",
                "failed",
                f"record_id={record_id} error_code={exc.error_code} message={exc.message}",
            )
            return None
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "record_detail",
                "failed",
                f"record_id={record_id} unexpected={type(exc).__name__}: {exc}",
            )
            return None
        if not isinstance(detail, dict) or not detail:
            self._emit(
                "record_detail",
                "empty",
                f"record_id={record_id} response was empty or not an object: {detail!r}",
            )
            return None
        self._emit("record_detail", "ok", f"record_id={record_id} keys={sorted(detail.keys())}")
        return detail

    async def _fetch_profile_detail(self, *, profile_id: str) -> dict[str, Any] | None:
        try:
            detail = await self._metsights.get_profile_detail(profile_id=profile_id)
        except AppError as exc:
            self._emit(
                "profile_detail",
                "failed",
                f"profile_id={profile_id} error_code={exc.error_code} message={exc.message}",
            )
            return None
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "profile_detail",
                "failed",
                f"profile_id={profile_id} unexpected={type(exc).__name__}: {exc}",
            )
            return None
        if not isinstance(detail, dict) or not detail:
            self._emit(
                "profile_detail",
                "empty",
                f"profile_id={profile_id} response was empty or not an object: {detail!r}",
            )
            return None
        self._emit("profile_detail", "ok", f"profile_id={profile_id} keys={sorted(detail.keys())}")
        return detail

    async def _fetch_physical_measurement(self, *, record_id: str) -> dict[str, Any] | None:
        try:
            payload = await self._metsights.get_record_subresource_or_none(
                record_id=record_id,
                resource="physical-measurement",
            )
        except AppError as exc:
            self._emit(
                "physical_measurement",
                "failed",
                f"record_id={record_id} error_code={exc.error_code} message={exc.message}",
            )
            return None
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "physical_measurement",
                "failed",
                f"record_id={record_id} unexpected={type(exc).__name__}: {exc}",
            )
            return None
        if not isinstance(payload, dict) or not payload:
            self._emit(
                "physical_measurement",
                "empty",
                f"record_id={record_id} API returned no physical-measurement data",
            )
            return None
        self._emit(
            "physical_measurement",
            "ok",
            f"record_id={record_id} keys={sorted(payload.keys())}",
        )
        return payload

    async def _resolve_local_user(
        self,
        db: AsyncSession,
        *,
        record_id: str,
        assessment: dict[str, Any] | None,
    ) -> tuple[Any | None, int | None]:
        """Return (User, assessment_instance_id) using local Patient/User tables."""
        if self._assessments is None or self._users is None:
            self._emit("user_api", "skipped", "local user repositories not configured")
            return None, None

        ids = extract_patient_identifiers(assessment, record_id=record_id)
        self._emit("user_api", "identifiers", f"{ids}")

        instance = None
        if record_id:
            try:
                instance = await self._assessments.get_instance_by_metsights_record_id(
                    db, metsights_record_id=record_id
                )
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "user_api",
                    "failed",
                    f"assessment lookup by record_id failed: {type(exc).__name__}: {exc}",
                )
                instance = None

        user = None
        assessment_instance_id: int | None = None
        if instance is not None:
            assessment_instance_id = int(instance.assessment_instance_id)
            try:
                user = await self._users.get_user_by_id(db, int(instance.user_id))
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "user_api",
                    "failed",
                    f"get_user_by_id({instance.user_id}) failed: {type(exc).__name__}: {exc}",
                )

        if user is None and ids.get("user_id"):
            try:
                user = await self._users.get_user_by_id(db, int(ids["user_id"]))
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "user_api",
                    "failed",
                    f"get_user_by_id({ids['user_id']}) failed: {type(exc).__name__}: {exc}",
                )

        if user is None and ids.get("patient_id"):
            try:
                user = await self._users.get_user_by_id(db, int(ids["patient_id"]))
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "user_api",
                    "failed",
                    f"get_user_by_id(patient_id={ids['patient_id']}) failed: {type(exc).__name__}: {exc}",
                )

        if user is None and ids.get("profile_id"):
            try:
                user = await self._users.get_user_by_metsights_profile_id(db, ids["profile_id"])
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "user_api",
                    "failed",
                    f"get_user_by_metsights_profile_id({ids['profile_id']}) failed: "
                    f"{type(exc).__name__}: {exc}",
                )

        if user is None:
            self._emit(
                "user_api",
                "empty",
                f"no local user found for record_id={record_id} identifiers={ids}",
            )
        else:
            self._emit(
                "user_api",
                "ok",
                f"user_id={getattr(user, 'user_id', None)} "
                f"assessment_instance_id={assessment_instance_id}",
            )
        return user, assessment_instance_id

    async def _fetch_questionnaire_demographics(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> dict[str, Any]:
        if self._questionnaire is None:
            self._emit("questionnaire", "skipped", "questionnaire repository not configured")
            return {}
        try:
            responses = await self._questionnaire.list_responses_for_instance(
                db, assessment_instance_id=assessment_instance_id
            )
            question_ids = [int(r.question_id) for r in responses]
            def_by_id = await self._questionnaire.get_definitions_by_ids(
                db, question_ids=question_ids
            )
            lookup: dict[str, Any] = {}
            for response in responses:
                definition = def_by_id.get(int(response.question_id))
                if definition is None or not definition.question_key:
                    continue
                key = str(definition.question_key)
                if key in _PHYSICAL_QUESTION_KEYS:
                    lookup[key] = response.answer
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "questionnaire",
                "failed",
                f"assessment_instance_id={assessment_instance_id} "
                f"{type(exc).__name__}: {exc}",
            )
            return {}

        extracted = extract_demographics_from_questionnaire(lookup)
        if extracted:
            self._emit(
                "questionnaire",
                "ok",
                f"assessment_instance_id={assessment_instance_id} fields={sorted(extracted.keys())}",
            )
        else:
            self._emit(
                "questionnaire",
                "empty",
                f"assessment_instance_id={assessment_instance_id} no height/weight/bmi answers",
            )
        return extracted

    async def _fetch_local_demographics(
        self,
        db: AsyncSession,
        *,
        record_id: str,
        assessment: dict[str, Any] | None,
    ) -> dict[str, Any]:
        demographics: dict[str, Any] = {}
        user, assessment_instance_id = await self._resolve_local_user(
            db, record_id=record_id, assessment=assessment
        )
        if user is not None:
            extracted = extract_demographics_from_user(user)
            demographics.update(extracted)
            self._emit("user_api", "extracted", f"fields={sorted(extracted.keys()) or ['<none>']}")

        if assessment_instance_id is not None:
            physical = await self._fetch_questionnaire_demographics(
                db, assessment_instance_id=assessment_instance_id
            )
            for key, value in physical.items():
                demographics.setdefault(key, value)
        return demographics

    async def _fetch_metsights_demographics(
        self,
        *,
        record_id: str,
        assessment: dict[str, Any] | None,
        already: dict[str, Any],
    ) -> dict[str, Any]:
        demographics = dict(already)
        still_missing = missing_demographic_fields({**demographics})
        if not still_missing:
            self._emit("metsights_fallback", "skipped", "all demographic fields already present")
            return demographics

        record = await self._fetch_record_detail(record_id=record_id)
        if record:
            extracted = extract_demographics_from_record(record)
            for key, value in extracted.items():
                demographics.setdefault(key, value)
            self._emit(
                "record_detail",
                "extracted",
                f"fields={sorted(extracted.keys()) or ['<none>']}",
            )

        still_missing = missing_demographic_fields({**demographics})
        # Always pull full profile when name/DOB are incomplete and a profile_id is known.
        needs_profile = any(
            f in still_missing for f in ("name", "gender", "sex", "age", "date_of_birth")
        )
        if needs_profile:
            profile_id = (
                resolve_profile_id(record, assessment)
                or (
                    str(demographics["profile_id"]).strip()
                    if demographics.get("profile_id") not in (None, "")
                    else None
                )
                or extract_patient_identifiers(assessment, record_id=record_id).get("profile_id")
            )
            if not profile_id:
                self._emit(
                    "profile_detail",
                    "skipped",
                    "no profile_id found on record detail or assessment JSON",
                )
            else:
                profile = await self._fetch_profile_detail(profile_id=profile_id)
                if profile:
                    extracted = extract_demographics_from_profile(profile)
                    for key, value in extracted.items():
                        demographics.setdefault(key, value)
                    self._emit(
                        "profile_detail",
                        "extracted",
                        f"fields={sorted(extracted.keys()) or ['<none>']}",
                    )
        else:
            self._emit(
                "profile_detail",
                "skipped",
                "identity fields already available",
            )

        still_missing = missing_demographic_fields({**demographics})
        physical_missing = any(f in still_missing for f in ("height", "weight", "bmi"))
        if physical_missing:
            nested = None
            if isinstance(record, dict):
                nested = record.get("physical_measurement") or record.get("physical-measurement")
            if isinstance(nested, dict) and nested:
                extracted = extract_demographics_from_physical(nested)
                for key, value in extracted.items():
                    demographics.setdefault(key, value)
                self._emit(
                    "physical_measurement",
                    "extracted",
                    f"source=record_detail fields={sorted(extracted.keys()) or ['<none>']}",
                )
            else:
                physical = await self._fetch_physical_measurement(record_id=record_id)
                if physical:
                    extracted = extract_demographics_from_physical(physical)
                    for key, value in extracted.items():
                        demographics.setdefault(key, value)
                    self._emit(
                        "physical_measurement",
                        "extracted",
                        f"source=subresource fields={sorted(extracted.keys()) or ['<none>']}",
                    )
        else:
            self._emit(
                "physical_measurement",
                "skipped",
                "height/weight/bmi already available",
            )
        return demographics

    async def fetch_demographics(
        self,
        *,
        record_id: str,
        assessment: dict[str, Any] | None = None,
        db: AsyncSession | None = None,
    ) -> dict[str, Any]:
        """Fetch demographics from local User API first, then MetSights fallbacks."""
        demographics: dict[str, Any] = {}

        if db is not None:
            try:
                local = await self._fetch_local_demographics(
                    db, record_id=record_id, assessment=assessment
                )
                demographics.update(local)
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "user_api",
                    "failed",
                    f"local patient enrichment failed: {type(exc).__name__}: {exc}",
                )
        else:
            self._emit(
                "user_api",
                "skipped",
                "no database session provided; using MetSights enrichment only",
            )

        return await self._fetch_metsights_demographics(
            record_id=record_id,
            assessment=assessment,
            already=demographics,
        )

    async def enrich_assessment_if_needed(
        self,
        assessment: dict[str, Any],
        *,
        record_id: str,
        db: AsyncSession | None = None,
    ) -> dict[str, Any]:
        """Merge available patient demographics into assessment; never raise on missing data."""
        if not needs_patient_enrichment(assessment):
            self._emit(
                "enrichment",
                "skipped",
                f"assessment already has demographics; missing={missing_demographic_fields(assessment)}",
            )
            return assessment
        missing = missing_demographic_fields(assessment)
        self._emit("enrichment", "started", f"missing_fields={missing}")
        try:
            patient = await self.fetch_demographics(
                record_id=record_id,
                assessment=assessment,
                db=db,
            )
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "enrichment",
                "failed",
                f"continuing without enrichment: {type(exc).__name__}: {exc}",
            )
            return assessment
        if not patient:
            self._emit(
                "enrichment",
                "failed",
                "no demographic fields could be resolved from user/profile/physical sources",
            )
            return assessment
        merged = merge_patient_into_assessment(assessment, patient)
        self._emit("enrichment", "merged", f"patient_fields={sorted(patient.keys())}")
        return merged
