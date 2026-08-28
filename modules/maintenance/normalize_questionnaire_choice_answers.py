"""Normalize questionnaire choice answers from display labels to option_value codes.

Metsights pull historically stored display names (e.g. ``\"Between 7 to 9 hours\"``)
in ``questionnaire_responses.answer``. Healthy habit rules and UI validation expect
``questionnaire_options.option_value`` codes (e.g. ``\"2\"``).

This job rewrites matching label-style answers in place. Unmatched values are
reported and left unchanged (no destructive nulling).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance
from modules.metsights.sync_service import (
    _CHOICE_QUESTION_TYPES,
    _normalize_pulled_choice_answer,
    _question_type_for_submit,
)
from modules.questionnaire.models import (
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.users.models import User


def _phone_digits(phone: str | None) -> str | None:
    digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    if len(digits) < 10:
        return None
    return digits[-10:]


@dataclass
class NormalizeChoiceAnswersReport:
    dry_run: bool
    scanned: int = 0
    updated: int = 0
    unchanged: int = 0
    unmatched: int = 0
    assessment_instance_id: int | None = None
    phone: str | None = None
    changes: list[dict[str, Any]] = field(default_factory=list)
    unmatched_samples: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "scanned": self.scanned,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "unmatched": self.unmatched,
            "assessment_instance_id": self.assessment_instance_id,
            "phone": self.phone,
            "changes": self.changes,
            "unmatched_samples": self.unmatched_samples,
        }


async def normalize_questionnaire_choice_answers(
    db: AsyncSession,
    *,
    dry_run: bool = True,
    assessment_instance_id: int | None = None,
    phone: str | None = None,
    max_change_samples: int = 200,
    max_unmatched_samples: int = 100,
) -> NormalizeChoiceAnswersReport:
    """Scan choice responses and rewrite display labels to option_value codes."""
    report = NormalizeChoiceAnswersReport(
        dry_run=dry_run,
        assessment_instance_id=assessment_instance_id,
        phone=phone,
    )

    stmt = (
        select(QuestionnaireResponse, QuestionnaireDefinition)
        .join(
            QuestionnaireDefinition,
            QuestionnaireDefinition.question_id == QuestionnaireResponse.question_id,
        )
        .where(
            QuestionnaireDefinition.question_type.in_(
                ("single_choice", "multiple_choice", "multi_choice")
            )
        )
        .order_by(
            QuestionnaireResponse.assessment_instance_id.asc(),
            QuestionnaireResponse.response_id.asc(),
        )
    )

    if assessment_instance_id is not None:
        stmt = stmt.where(
            QuestionnaireResponse.assessment_instance_id == int(assessment_instance_id)
        )

    if phone is not None:
        phone_key = _phone_digits(phone)
        if phone_key is None:
            raise ValueError(f"Invalid phone filter: {phone!r}")
        # Match last-10 digits regardless of +91 / 91 prefix stored on users.phone.
        user_ids_result = await db.execute(select(User.user_id, User.phone))
        matching_user_ids = [
            int(uid)
            for uid, stored in user_ids_result.all()
            if _phone_digits(stored) == phone_key
        ]
        if not matching_user_ids:
            return report
        instance_ids_result = await db.execute(
            select(AssessmentInstance.assessment_instance_id).where(
                AssessmentInstance.user_id.in_(matching_user_ids)
            )
        )
        instance_ids = [int(row[0]) for row in instance_ids_result.all()]
        if not instance_ids:
            return report
        stmt = stmt.where(QuestionnaireResponse.assessment_instance_id.in_(instance_ids))

    rows = list((await db.execute(stmt)).all())
    if not rows:
        return report

    question_ids = sorted({int(defn.question_id) for _resp, defn in rows})
    options_result = await db.execute(
        select(QuestionnaireOption)
        .where(QuestionnaireOption.question_id.in_(question_ids))
        .order_by(QuestionnaireOption.question_id.asc(), QuestionnaireOption.option_id.asc())
    )
    options_by_qid: dict[int, list[QuestionnaireOption]] = {}
    for opt in options_result.scalars().all():
        options_by_qid.setdefault(int(opt.question_id), []).append(opt)

    for response, definition in rows:
        report.scanned += 1
        qtype = _question_type_for_submit(definition.question_type)
        if qtype not in _CHOICE_QUESTION_TYPES:
            report.unchanged += 1
            continue

        db_opts = options_by_qid.get(int(definition.question_id)) or []
        original = response.answer
        normalized, skip_reason = _normalize_pulled_choice_answer(
            original,
            question_type=definition.question_type,
            db_options=db_opts,
        )

        if skip_reason is not None:
            report.unmatched += 1
            if len(report.unmatched_samples) < max_unmatched_samples:
                report.unmatched_samples.append(
                    {
                        "response_id": int(response.response_id),
                        "assessment_instance_id": int(response.assessment_instance_id),
                        "question_id": int(definition.question_id),
                        "question_key": definition.question_key,
                        "answer": original,
                        "reason": skip_reason,
                    }
                )
            continue

        if normalized == original:
            report.unchanged += 1
            continue

        report.updated += 1
        if len(report.changes) < max_change_samples:
            report.changes.append(
                {
                    "response_id": int(response.response_id),
                    "assessment_instance_id": int(response.assessment_instance_id),
                    "question_id": int(definition.question_id),
                    "question_key": definition.question_key,
                    "from": original,
                    "to": normalized,
                }
            )

        if not dry_run:
            response.answer = normalized

    if not dry_run:
        await db.flush()

    return report
