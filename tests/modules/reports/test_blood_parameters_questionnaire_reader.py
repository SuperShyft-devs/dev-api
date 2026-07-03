"""Unit tests for questionnaire blood parameter reader."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.reports.blood_parameters_questionnaire_reader import BloodParametersQuestionnaireReader


@pytest.mark.asyncio
async def test_build_flat_from_questionnaire_responses(test_db_session):
    test_db_session.add(
        QuestionnaireCategory(
            category_id=88001,
            category_key="blood-parameters",
            display_name="Blood Parameters",
            category_of="metsights",
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=88001,
            question_key="glucose_fasting",
            question_text="Glucose fasting",
            question_type="scale",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        QuestionnaireOption(
            option_id=88001,
            question_id=88001,
            option_value="0",
            display_name="mg/dL",
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            response_id=88001,
            assessment_instance_id=99002,
            question_id=88001,
            category_id=88001,
            answer={"value": 91.0, "unit": "0"},
            submitted_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    reader = BloodParametersQuestionnaireReader()
    flat = await reader.build_flat_from_questionnaire_responses(
        test_db_session,
        assessment_instance_id=99002,
    )

    assert flat["glucose_fasting"] == 91.0
    assert flat["glucose_fasting_unit"] == "mg/dL"
