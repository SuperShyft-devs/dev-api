"""Read Metsights blood values from ``questionnaire_responses``."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    ALL_BLOOD_PARAMETER_KEYS,
    BLOOD_PARAMETER_CATEGORY_KEY,
    UNITLESS_BLOOD_PARAMETER_KEYS,
)
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.questionnaire.repository import QuestionnaireRepository

_BLOOD_CATEGORY_KEYS = (
    BLOOD_PARAMETER_CATEGORY_KEY,
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
)


class BloodParametersQuestionnaireReader:
    """Build Metsights-compatible flat blood dicts from questionnaire answers."""

    def __init__(self, repository: QuestionnaireRepository | None = None) -> None:
        self._repository = repository or QuestionnaireRepository()

    async def build_flat_from_questionnaire_responses(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> dict[str, Any]:
        """Produce flat shape ``{haemoglobin: 13.2, haemoglobin_unit: "g/dL", ...}``."""
        category_ids = await self._blood_category_ids(db)
        if not category_ids:
            return {}

        result = await db.execute(
            select(QuestionnaireResponse, QuestionnaireDefinition)
            .join(
                QuestionnaireDefinition,
                QuestionnaireDefinition.question_id == QuestionnaireResponse.question_id,
            )
            .where(QuestionnaireResponse.assessment_instance_id == assessment_instance_id)
            .where(QuestionnaireResponse.category_id.in_(category_ids))
        )
        rows = result.all()
        if not rows:
            return {}

        question_ids = [int(defn.question_id) for _, defn in rows]
        all_options = await self._repository.list_options_for_question_ids(
            db,
            question_ids=question_ids,
        )
        options_by_question: dict[int, list] = {}
        for option in all_options:
            options_by_question.setdefault(int(option.question_id), []).append(option)

        flat: dict[str, Any] = {}
        for response, definition in rows:
            question_key = (definition.question_key or "").strip()
            if not question_key or question_key not in ALL_BLOOD_PARAMETER_KEYS:
                continue
            answer = response.answer
            if not isinstance(answer, dict):
                continue
            raw_value = answer.get("value")
            if raw_value is None:
                continue
            try:
                flat[question_key] = float(raw_value)
            except (TypeError, ValueError):
                continue

            unit_code = answer.get("unit")
            if question_key in UNITLESS_BLOOD_PARAMETER_KEYS:
                continue
            if unit_code is None:
                continue
            display_unit = self._resolve_unit_display(
                str(unit_code).strip(),
                options_by_question.get(int(definition.question_id), []),
            )
            if display_unit:
                flat[f"{question_key}_unit"] = display_unit

        return flat

    async def extract_parameter_for_assessment(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        parameter_key: str,
    ) -> tuple[float | None, str | None]:
        flat = await self.build_flat_from_questionnaire_responses(
            db,
            assessment_instance_id=assessment_instance_id,
        )
        raw_value = flat.get(parameter_key)
        if raw_value is None:
            return None, None
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return None, None
        unit_key = f"{parameter_key}_unit"
        raw_unit = flat.get(unit_key)
        unit = raw_unit.strip() if isinstance(raw_unit, str) and raw_unit.strip() else None
        return value, unit

    async def _blood_category_ids(self, db: AsyncSession) -> list[int]:
        result = await db.execute(
            select(QuestionnaireCategory.category_id).where(
                QuestionnaireCategory.category_key.in_(_BLOOD_CATEGORY_KEYS),
                QuestionnaireCategory.category_of == "metsights",
            )
        )
        return [int(row[0]) for row in result.all()]

    @staticmethod
    def _resolve_unit_display(unit_code: str, options: list[QuestionnaireOption]) -> str | None:
        for option in options:
            if str(option.option_value).strip() == unit_code:
                display = str(option.display_name or "").strip()
                return display or None
        # Already a display string (legacy migrated rows)
        return unit_code or None
