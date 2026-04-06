from __future__ import annotations

from typing import Iterable

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentPackage, AssessmentPackageCategory
from modules.employee.models import Employee
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
)
from modules.platform_settings.models import PlatformSettings
from modules.users.models import User

from db.seed.seed_dataclasses import (
    SeedAssessmentPackage,
    SeedCategory,
    SeedCategoryQuestion,
    SeedEmployee,
    SeedOption,
    SeedPackageCategory,
    SeedQuestion,
    SeedUser,
)

# B2C singleton defaults (diagnostic id 1 is inactive in CSV; use active Supershyft Basic).
DEFAULT_B2C_ASSESSMENT_PACKAGE_ID = 1
DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID = 6


async def upsert_users(session: AsyncSession, users: Iterable[SeedUser]) -> None:
    for seed in users:
        existing = await session.get(User, seed.user_id)
        if existing is None:
            session.add(
                User(
                    user_id=seed.user_id,
                    first_name=seed.first_name,
                    last_name=seed.last_name,
                    age=seed.age,
                    phone=seed.phone,
                    email=seed.email,
                    date_of_birth=seed.date_of_birth,
                    gender=seed.gender,
                    address=seed.address,
                    pin_code=seed.pin_code,
                    city=seed.city,
                    state=seed.state,
                    country=seed.country,
                    referred_by=seed.referred_by,
                    is_participant=seed.is_participant,
                    status=seed.status,
                )
            )
        else:
            existing.first_name = seed.first_name
            existing.last_name = seed.last_name
            existing.age = seed.age
            existing.phone = seed.phone
            existing.email = seed.email
            existing.date_of_birth = seed.date_of_birth
            existing.gender = seed.gender
            existing.address = seed.address
            existing.pin_code = seed.pin_code
            existing.city = seed.city
            existing.state = seed.state
            existing.country = seed.country
            existing.referred_by = seed.referred_by
            existing.is_participant = seed.is_participant
            existing.status = seed.status


async def upsert_employees(
    session: AsyncSession, employees: Iterable[SeedEmployee]
) -> None:
    for seed in employees:
        existing = await session.get(Employee, seed.employee_id)
        if existing is None:
            session.add(
                Employee(
                    employee_id=seed.employee_id,
                    user_id=seed.user_id,
                    role=seed.role,
                    status=seed.status,
                )
            )
        else:
            existing.user_id = seed.user_id
            existing.role = seed.role
            existing.status = seed.status


async def upsert_assessment_packages(
    session: AsyncSession, packages: Iterable[SeedAssessmentPackage]
) -> None:
    for seed in packages:
        existing = await session.get(AssessmentPackage, seed.package_id)
        if existing is None:
            session.add(
                AssessmentPackage(
                    package_id=seed.package_id,
                    package_code=seed.package_code,
                    display_name=seed.display_name,
                    assessment_type_code=seed.assessment_type_code,
                    status=seed.status,
                )
            )
        else:
            existing.package_code = seed.package_code
            existing.display_name = seed.display_name
            existing.assessment_type_code = seed.assessment_type_code
            existing.status = seed.status


async def upsert_categories(
    session: AsyncSession, categories: Iterable[SeedCategory]
) -> None:
    for seed in categories:
        existing = await session.get(QuestionnaireCategory, seed.category_id)
        if existing is None:
            session.add(
                QuestionnaireCategory(
                    category_id=seed.category_id,
                    category_key=seed.category_key,
                    display_name=seed.display_name,
                    status=seed.status,
                )
            )
        else:
            existing.category_key = seed.category_key
            existing.display_name = seed.display_name
            existing.status = seed.status


async def delete_options_for_question_ids(session: AsyncSession, question_ids: Iterable[int]) -> None:
    qids = sorted({int(x) for x in question_ids})
    if not qids:
        return
    await session.execute(delete(QuestionnaireOption).where(QuestionnaireOption.question_id.in_(qids)))
    await session.flush()


async def upsert_questions(
    session: AsyncSession, questions: Iterable[SeedQuestion]
) -> None:
    for seed in questions:
        # Avoid unique(question_key) violations when seed keys are renamed but older
        # rows (or duplicates) still hold the target key.
        key_conflict = await session.execute(
            select(QuestionnaireDefinition).where(
                QuestionnaireDefinition.question_key == seed.question_key,
                QuestionnaireDefinition.question_id != seed.question_id,
            )
        )
        for other in key_conflict.scalars().all():
            other.question_key = f"_superseded_{other.question_id}"
        await session.flush()

        existing = await session.get(QuestionnaireDefinition, seed.question_id)
        if existing is None:
            session.add(
                QuestionnaireDefinition(
                    question_id=seed.question_id,
                    question_key=seed.question_key,
                    question_text=seed.question_text,
                    question_type=seed.question_type,
                    is_required=seed.is_required,
                    is_read_only=seed.is_read_only,
                    help_text=seed.help_text,
                    status=seed.status,
                )
            )
        else:
            existing.question_key = seed.question_key
            existing.question_text = seed.question_text
            existing.question_type = seed.question_type
            existing.is_required = seed.is_required
            existing.is_read_only = seed.is_read_only
            existing.help_text = seed.help_text
            existing.status = seed.status


async def upsert_category_questions(
    session: AsyncSession,
    category_questions: Iterable[SeedCategoryQuestion],
) -> None:
    for seed in category_questions:
        by_pair = (
            await session.execute(
                select(QuestionnaireCategoryQuestion).where(
                    QuestionnaireCategoryQuestion.category_id == seed.category_id,
                    QuestionnaireCategoryQuestion.question_id == seed.question_id,
                )
            )
        ).scalar_one_or_none()
        if by_pair is not None and by_pair.id != seed.id:
            await session.delete(by_pair)
            await session.flush()
        by_id = await session.get(QuestionnaireCategoryQuestion, seed.id)
        if by_id is not None and (
            by_id.category_id != seed.category_id or by_id.question_id != seed.question_id
        ):
            await session.delete(by_id)
            await session.flush()

        existing = await session.get(QuestionnaireCategoryQuestion, seed.id)
        if existing is None:
            session.add(
                QuestionnaireCategoryQuestion(
                    id=seed.id,
                    category_id=seed.category_id,
                    question_id=seed.question_id,
                )
            )
        else:
            existing.category_id = seed.category_id
            existing.question_id = seed.question_id


async def upsert_options(session: AsyncSession, options: Iterable[SeedOption]) -> None:
    for seed in options:
        existing = await session.get(QuestionnaireOption, seed.option_id)
        if existing is None:
            session.add(
                QuestionnaireOption(
                    option_id=seed.option_id,
                    question_id=seed.question_id,
                    option_value=seed.option_value,
                    display_name=seed.display_name,
                    tooltip_text=seed.tooltip_text,
                )
            )
        else:
            existing.question_id = seed.question_id
            existing.option_value = seed.option_value
            existing.display_name = seed.display_name
            existing.tooltip_text = seed.tooltip_text


async def upsert_package_categories(
    session: AsyncSession,
    package_categories: Iterable[SeedPackageCategory],
) -> None:
    for seed in package_categories:
        by_pair = (
            await session.execute(
                select(AssessmentPackageCategory).where(
                    AssessmentPackageCategory.package_id == seed.package_id,
                    AssessmentPackageCategory.category_id == seed.category_id,
                )
            )
        ).scalar_one_or_none()
        if by_pair is not None and by_pair.id != seed.id:
            await session.delete(by_pair)
            await session.flush()
        by_id = await session.get(AssessmentPackageCategory, seed.id)
        if by_id is not None and (
            by_id.package_id != seed.package_id or by_id.category_id != seed.category_id
        ):
            await session.delete(by_id)
            await session.flush()

        existing = await session.get(AssessmentPackageCategory, seed.id)
        if existing is None:
            session.add(
                AssessmentPackageCategory(
                    id=seed.id,
                    package_id=seed.package_id,
                    category_id=seed.category_id,
                )
            )
        else:
            existing.package_id = seed.package_id
            existing.category_id = seed.category_id


async def reset_sequences(session: AsyncSession) -> None:
    """Reset PostgreSQL sequences after manual ID insertion."""
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('users', 'user_id'),
            COALESCE((SELECT MAX(user_id) FROM users), 1),
            true
        )
        """
        )
    )
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('employee', 'employee_id'),
            COALESCE((SELECT MAX(employee_id) FROM employee), 1),
            true
        )
        """
        )
    )
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('assessment_packages', 'package_id'),
            COALESCE((SELECT MAX(package_id) FROM assessment_packages), 1),
            true
        )
        """
        )
    )
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('questionnaire_categories', 'category_id'),
            COALESCE((SELECT MAX(category_id) FROM questionnaire_categories), 1),
            true
        )
        """
        )
    )
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('questionnaire_definitions', 'question_id'),
            COALESCE((SELECT MAX(question_id) FROM questionnaire_definitions), 1),
            true
        )
        """
        )
    )
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('questionnaire_options', 'option_id'),
            COALESCE((SELECT MAX(option_id) FROM questionnaire_options), 1),
            true
        )
        """
        )
    )
    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('questionnaire_category_questions', 'id'),
            COALESCE((SELECT MAX(id) FROM questionnaire_category_questions), 1),
            true
        )
        """
        )
    )

    await session.execute(
        text(
            """
        SELECT setval(
            pg_get_serial_sequence('assessment_package_categories', 'id'),
            COALESCE((SELECT MAX(id) FROM assessment_package_categories), 1),
            true
        )
        """
        )
    )


async def upsert_default_platform_settings(session: AsyncSession) -> None:
    """Ensure row settings_id=1 exists so B2C onboarding does not fall back to inactive diagnostic package 1."""
    existing = await session.get(PlatformSettings, 1)
    if existing is not None:
        return
    session.add(
        PlatformSettings(
            settings_id=1,
            b2c_default_assessment_package_id=DEFAULT_B2C_ASSESSMENT_PACKAGE_ID,
            b2c_default_diagnostic_package_id=DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID,
            updated_by_user_id=None,
        )
    )
    await session.flush()
