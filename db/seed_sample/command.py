"""Database sample seeding command for admin dashboard development.

Entrypoint: `python -m db.seed_sample --yes`
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, time
from typing import Iterable

from sqlalchemy import and_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings
from modules.assessments.models import AssessmentPackage, AssessmentPackageCategory
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant, OnboardingAssistantAssignment
from modules.organizations.models import Organization
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
)
from modules.users.models import User


@dataclass(frozen=True)
class SeedUser:
    user_id: int
    first_name: str
    last_name: str
    age: int
    phone: str
    email: str
    date_of_birth: date
    gender: str
    address: str
    pin_code: str
    city: str
    state: str
    country: str
    referred_by: str | None
    is_participant: bool
    status: str


@dataclass(frozen=True)
class SeedEmployee:
    employee_id: int
    user_id: int
    role: str
    status: str


@dataclass(frozen=True)
class SeedOrganization:
    organization_id: int
    name: str
    organization_type: str
    website_url: str
    address: str
    pin_code: str
    city: str
    state: str
    country: str
    contact_name: str
    contact_email: str
    contact_phone: str
    contact_designation: str
    bd_employee_id: int | None
    status: str
    created_employee_id: int | None
    updated_employee_id: int | None


@dataclass(frozen=True)
class SeedAssessmentPackage:
    package_id: int
    package_code: str
    display_name: str
    status: str


@dataclass(frozen=True)
class SeedDiagnosticPackage:
    diagnostic_package_id: int
    reference_id: str
    package_name: str
    diagnostic_provider: str
    status: str


@dataclass(frozen=True)
class SeedEngagement:
    engagement_id: int
    engagement_name: str
    metsights_engagement_id: str | None
    organization_id: int | None
    engagement_code: str
    engagement_type: str
    assessment_package_id: int
    diagnostic_package_id: int | None
    city: str
    slot_duration: int
    start_date: date
    end_date: date
    status: str
    participant_count: int


@dataclass(frozen=True)
class SeedQuestion:
    question_id: int
    question_key: str
    question_text: str
    question_type: str
    is_required: bool
    is_read_only: bool
    help_text: str | None
    status: str


@dataclass(frozen=True)
class SeedQuestionOption:
    option_id: int
    question_id: int
    option_value: str
    display_name: str
    tooltip_text: str | None


@dataclass(frozen=True)
class SeedCategory:
    category_id: int
    category_key: str
    display_name: str
    status: str


@dataclass(frozen=True)
class SeedCategoryQuestion:
    id: int
    category_id: int
    question_id: int


@dataclass(frozen=True)
class SeedPackageCategory:
    id: int
    package_id: int
    category_id: int


@dataclass(frozen=True)
class SeedOnboardingAssignment:
    onboarding_assistant_id: int
    employee_id: int
    engagement_id: int


@dataclass(frozen=True)
class SeedParticipant:
    engagement_participant_id: int
    engagement_id: int
    user_id: int
    slot_start_time: time
    engagement_date: date
    participants_employee_id: str | None = None
    want_doctor_consultation: bool | None = None
    want_nutritionist_consultation: bool | None = None
    want_doctor_and_nutritionist_consultation: bool | None = None


SAMPLE_USERS: tuple[SeedUser, ...] = (
    SeedUser(1001, "Aarav", "Shah", 33, "9000001001", "aarav.shah@example.com", date(1992, 5, 15), "male", "Andheri East", "400059", "Mumbai", "Maharashtra", "India", None, False, "active"),
    SeedUser(1002, "Mira", "Kapoor", 34, "9000001002", "mira.kapoor@example.com", date(1991, 8, 9), "female", "Banjara Hills", "500034", "Hyderabad", "Telangana", "India", None, False, "active"),
    SeedUser(1003, "Kabir", "Mehta", 35, "9000001003", "kabir.mehta@example.com", date(1990, 2, 19), "male", "Koramangala", "560034", "Bengaluru", "Karnataka", "India", None, False, "active"),
    SeedUser(1004, "Nisha", "Rao", 36, "9000001004", "nisha.rao@example.com", date(1989, 11, 27), "female", "Kondapur", "500084", "Hyderabad", "Telangana", "India", None, False, "inactive"),
    SeedUser(1005, "Rohan", "Das", 32, "9000001005", "rohan.das@example.com", date(1993, 3, 4), "male", "Salt Lake", "700091", "Kolkata", "West Bengal", "India", None, False, "active"),
    SeedUser(1101, "Pooja", "Iyer", 30, "9000001101", "pooja.iyer@example.com", date(1995, 1, 11), "female", "HSR Layout", "560102", "Bengaluru", "Karnataka", "India", "HC-MUM-2026", True, "active"),
    SeedUser(1102, "Arjun", "Nair", 31, "9000001102", "arjun.nair@example.com", date(1994, 6, 2), "male", "Powai", "400076", "Mumbai", "Maharashtra", "India", "HC-MUM-2026", True, "active"),
    SeedUser(1103, "Sara", "Khan", 29, "9000001103", "sara.khan@example.com", date(1996, 9, 18), "female", "Whitefield", "560066", "Bengaluru", "Karnataka", "India", "FIN-BLR-2026", True, "inactive"),
    SeedUser(1104, "Dev", "Patel", 28, "9000001104", "dev.patel@example.com", date(1997, 4, 25), "male", "Thane West", "400601", "Mumbai", "Maharashtra", "India", "FIN-BLR-2026", True, "active"),
    SeedUser(1105, "Isha", "Verma", 27, "9000001105", "isha.verma@example.com", date(1998, 12, 7), "female", "Indiranagar", "560038", "Bengaluru", "Karnataka", "India", "OPEN-CARE-2026", True, "active"),
)

SAMPLE_EMPLOYEES: tuple[SeedEmployee, ...] = (
    SeedEmployee(201, 1001, "admin", "active"),
    SeedEmployee(202, 1002, "bd", "active"),
    SeedEmployee(203, 1003, "onboarding_assistant", "active"),
    SeedEmployee(204, 1004, "onboarding_assistant", "inactive"),
    SeedEmployee(205, 1005, "operations", "active"),
)

SAMPLE_ORGANIZATIONS: tuple[SeedOrganization, ...] = (
    SeedOrganization(301, "Zenith Corp", "enterprise", "https://zenith.example.com", "BKC, Bandra East", "400051", "Mumbai", "Maharashtra", "India", "Anita Menon", "anita.menon@zenith.example.com", "9876501001", "HR Director", 202, "active", 201, 201),
    SeedOrganization(302, "Apex Retail", "retail", "https://apex.example.com", "Hitech City", "500081", "Hyderabad", "Telangana", "India", "Pranav Reddy", "pranav.reddy@apex.example.com", "9876501002", "People Ops Lead", 202, "active", 201, 202),
    SeedOrganization(303, "Nimbus Logistics", "logistics", "https://nimbus.example.com", "Electronic City", "560100", "Bengaluru", "Karnataka", "India", "Kritika Jain", "kritika.jain@nimbus.example.com", "9876501003", "Admin Head", 205, "inactive", 205, 205),
    SeedOrganization(304, "Legacy Foods", "manufacturing", "https://legacy.example.com", "Sector V", "700091", "Kolkata", "West Bengal", "India", "Ravi Sinha", "ravi.sinha@legacy.example.com", "9876501004", "HR Manager", 205, "archived", 205, 205),
)

SAMPLE_ASSESSMENT_PACKAGES: tuple[SeedAssessmentPackage, ...] = (
    SeedAssessmentPackage(1, "METSIGHTS_BASIC", "Metsights Basic", "active"),
    SeedAssessmentPackage(11, "METSIGHTS_CORE", "Metsights Core", "active"),
    SeedAssessmentPackage(12, "METSIGHTS_LEADERSHIP", "Metsights Leadership", "active"),
    SeedAssessmentPackage(13, "METSIGHTS_WELLBEING", "Metsights Wellbeing", "inactive"),
)

SAMPLE_DIAGNOSTIC_PACKAGES: tuple[SeedDiagnosticPackage, ...] = (
    SeedDiagnosticPackage(1, "DX-CORE-00", "Default Diagnostics", "HealthLabs", "active"),
    SeedDiagnosticPackage(21, "DX-CORE-01", "Core Diagnostics", "HealthLabs", "active"),
    SeedDiagnosticPackage(22, "DX-PRO-01", "Advanced Diagnostics", "HealthLabs", "active"),
)

SAMPLE_ENGAGEMENTS: tuple[SeedEngagement, ...] = (
    SeedEngagement(401, "Zenith Annual Healthcamp", "METS-401", 301, "HC-MUM-2026", "doctor", 11, 21, "Mumbai", 30, date(2026, 3, 10), date(2026, 3, 15), "active", 3),
    SeedEngagement(402, "Apex Leadership Drive", "METS-402", 302, "FIN-BLR-2026", "doctor", 12, 22, "Hyderabad", 45, date(2026, 3, 20), date(2026, 3, 27), "active", 2),
    SeedEngagement(403, "Nimbus Wellness Week", "METS-403", 303, "WELL-NMB-2026", "doctor", 13, 21, "Bengaluru", 30, date(2026, 2, 10), date(2026, 2, 14), "inactive", 1),
    SeedEngagement(404, "Open Community Camp", "METS-404", None, "OPEN-CARE-2026", "bio_ai", 11, 1, "Mumbai", 20, date(2026, 3, 5), date(2026, 3, 25), "active", 2),
    SeedEngagement(405, "City Pop-up Screening", "METS-405", None, "CITY-POP-2026", "bio_ai", 11, 1, "Hyderabad", 20, date(2026, 1, 15), date(2026, 1, 20), "inactive", 1),
    SeedEngagement(406, "Legacy Recall Camp", "METS-406", 304, "LEG-RECALL-2026", "doctor", 11, 21, "Kolkata", 30, date(2025, 12, 1), date(2025, 12, 3), "archived", 0),
)

SAMPLE_CATEGORIES: tuple[SeedCategory, ...] = (
    SeedCategory(701, "wellbeing", "Wellbeing", "active"),
    SeedCategory(702, "fitness", "Fitness", "active"),
    SeedCategory(703, "lifestyle", "Lifestyle", "active"),
)


SAMPLE_QUESTIONS: tuple[SeedQuestion, ...] = (
    SeedQuestion(501, "energy_level_weekly", "How would you rate your energy levels this week?", "scale", True, False, "Use your weekly average", "active"),
    SeedQuestion(502, "exercise_days_weekly", "How many days did you exercise in the last 7 days?", "single_choice", True, False, None, "active"),
    SeedQuestion(503, "health_goals", "What health goals are most important to you?", "multiple_choice", False, False, None, "active"),
    SeedQuestion(504, "health_concerns", "Any health concerns you want to share?", "text", False, False, None, "inactive"),
)

SAMPLE_CATEGORY_QUESTIONS: tuple[SeedCategoryQuestion, ...] = (
    SeedCategoryQuestion(901, 701, 501),
    SeedCategoryQuestion(902, 702, 502),
    SeedCategoryQuestion(903, 703, 503),
    SeedCategoryQuestion(904, 701, 504),
)

SAMPLE_QUESTION_OPTIONS: tuple[SeedQuestionOption, ...] = (
    SeedQuestionOption(801, 502, "0", "0", None),
    SeedQuestionOption(802, 502, "1-2", "1-2", None),
    SeedQuestionOption(803, 502, "3-4", "3-4", None),
    SeedQuestionOption(804, 502, "5+", "5+", None),
    SeedQuestionOption(805, 503, "Weight loss", "Weight loss", None),
    SeedQuestionOption(806, 503, "Better sleep", "Better sleep", None),
    SeedQuestionOption(807, 503, "Stress management", "Stress management", None),
    SeedQuestionOption(808, 503, "Fitness", "Fitness", None),
)

SAMPLE_PACKAGE_CATEGORIES: tuple[SeedPackageCategory, ...] = (
    SeedPackageCategory(600, 1, 701),
    SeedPackageCategory(606, 1, 703),
    SeedPackageCategory(601, 11, 701),
    SeedPackageCategory(602, 11, 702),
    SeedPackageCategory(603, 12, 701),
    SeedPackageCategory(604, 12, 703),
    SeedPackageCategory(605, 13, 701),
)

SAMPLE_ASSIGNMENTS: tuple[SeedOnboardingAssignment, ...] = (
    SeedOnboardingAssignment(701, 203, 401),
    SeedOnboardingAssignment(702, 205, 401),
    SeedOnboardingAssignment(703, 203, 402),
    SeedOnboardingAssignment(704, 205, 404),
)

SAMPLE_PARTICIPANTS: tuple[SeedParticipant, ...] = (
    SeedParticipant(801, 401, 1101, time(10, 0), date(2026, 3, 11)),
    SeedParticipant(802, 401, 1102, time(10, 30), date(2026, 3, 11)),
    SeedParticipant(803, 401, 1104, time(11, 0), date(2026, 3, 12)),
    SeedParticipant(804, 402, 1103, time(15, 0), date(2026, 3, 22)),
    SeedParticipant(805, 402, 1104, time(15, 45), date(2026, 3, 22)),
    SeedParticipant(806, 404, 1105, time(9, 30), date(2026, 3, 8)),
    SeedParticipant(807, 404, 1102, time(9, 50), date(2026, 3, 8)),
    SeedParticipant(808, 405, 1101, time(10, 20), date(2026, 1, 17)),
    SeedParticipant(809, 403, 1103, time(14, 0), date(2026, 2, 12)),
)


async def _upsert_users(session: AsyncSession, users: Iterable[SeedUser]) -> None:
    for seed in users:
        row = await session.get(User, seed.user_id)
        if row is None:
            row = User(user_id=seed.user_id)
            session.add(row)
        row.first_name = seed.first_name
        row.last_name = seed.last_name
        row.age = seed.age
        row.phone = seed.phone
        row.email = seed.email
        row.date_of_birth = seed.date_of_birth
        row.gender = seed.gender
        row.address = seed.address
        row.pin_code = seed.pin_code
        row.city = seed.city
        row.state = seed.state
        row.country = seed.country
        row.referred_by = seed.referred_by
        row.is_participant = seed.is_participant
        row.status = seed.status


async def _upsert_employees(session: AsyncSession, employees: Iterable[SeedEmployee]) -> None:
    for seed in employees:
        row = await session.get(Employee, seed.employee_id)
        if row is None:
            row = Employee(employee_id=seed.employee_id)
            session.add(row)
        row.user_id = seed.user_id
        row.role = seed.role
        row.status = seed.status


async def _upsert_organizations(session: AsyncSession, organizations: Iterable[SeedOrganization]) -> None:
    for seed in organizations:
        row = await session.get(Organization, seed.organization_id)
        if row is None:
            row = Organization(organization_id=seed.organization_id)
            session.add(row)
        row.name = seed.name
        row.organization_type = seed.organization_type
        row.website_url = seed.website_url
        row.address = seed.address
        row.pin_code = seed.pin_code
        row.city = seed.city
        row.state = seed.state
        row.country = seed.country
        row.contact_name = seed.contact_name
        row.contact_email = seed.contact_email
        row.contact_phone = seed.contact_phone
        row.contact_designation = seed.contact_designation
        row.bd_employee_id = seed.bd_employee_id
        row.status = seed.status
        row.created_employee_id = seed.created_employee_id
        row.updated_employee_id = seed.updated_employee_id


async def _upsert_assessment_packages(
    session: AsyncSession, packages: Iterable[SeedAssessmentPackage]
) -> None:
    for seed in packages:
        row = await session.get(AssessmentPackage, seed.package_id)
        if row is None:
            row = AssessmentPackage(package_id=seed.package_id)
            session.add(row)
        row.package_code = seed.package_code
        row.display_name = seed.display_name
        row.status = seed.status


async def _upsert_diagnostic_packages(
    session: AsyncSession, packages: Iterable[SeedDiagnosticPackage]
) -> None:
    for seed in packages:
        row = await session.get(DiagnosticPackage, seed.diagnostic_package_id)
        if row is None:
            row = DiagnosticPackage(diagnostic_package_id=seed.diagnostic_package_id)
            session.add(row)
        row.reference_id = seed.reference_id
        row.package_name = seed.package_name
        row.diagnostic_provider = seed.diagnostic_provider
        row.status = seed.status


async def _upsert_engagements(session: AsyncSession, engagements: Iterable[SeedEngagement]) -> None:
    for seed in engagements:
        row = await session.get(Engagement, seed.engagement_id)
        if row is None:
            row = Engagement(engagement_id=seed.engagement_id)
            session.add(row)
        row.engagement_name = seed.engagement_name
        row.metsights_engagement_id = seed.metsights_engagement_id
        row.organization_id = seed.organization_id
        row.engagement_code = seed.engagement_code
        row.engagement_type = seed.engagement_type
        row.assessment_package_id = seed.assessment_package_id
        row.diagnostic_package_id = seed.diagnostic_package_id
        row.city = seed.city
        row.slot_duration = seed.slot_duration
        row.start_date = seed.start_date
        row.end_date = seed.end_date
        row.status = seed.status
        row.participant_count = seed.participant_count


async def _upsert_categories(session: AsyncSession, categories: Iterable[SeedCategory]) -> None:
    for seed in categories:
        row = await session.get(QuestionnaireCategory, seed.category_id)
        if row is None:
            row = QuestionnaireCategory(category_id=seed.category_id)
            session.add(row)
        row.category_key = seed.category_key
        row.display_name = seed.display_name
        row.status = seed.status


async def _upsert_questions(
    session: AsyncSession, questions: Iterable[SeedQuestion]
) -> dict[int, int]:
    id_map: dict[int, int] = {}
    for seed in questions:
        row = await session.get(QuestionnaireDefinition, seed.question_id)
        if row is None:
            with session.no_autoflush:
                existing = await session.execute(
                    select(QuestionnaireDefinition).where(
                        QuestionnaireDefinition.question_key == seed.question_key
                    )
                )
                row = existing.scalar_one_or_none()
            if row is None:
                row = QuestionnaireDefinition(question_id=seed.question_id)
                session.add(row)
        row.question_key = seed.question_key
        row.question_text = seed.question_text
        row.question_type = seed.question_type
        row.is_required = seed.is_required
        row.is_read_only = seed.is_read_only
        row.help_text = seed.help_text
        row.status = seed.status
        await session.flush()
        id_map[seed.question_id] = int(row.question_id)
    return id_map


async def _upsert_category_questions(
    session: AsyncSession,
    category_questions: Iterable[SeedCategoryQuestion],
    question_id_map: dict[int, int],
) -> None:
    for seed in category_questions:
        resolved_question_id = question_id_map.get(seed.question_id, seed.question_id)
        row = await session.get(QuestionnaireCategoryQuestion, seed.id)
        if row is None:
            existing = await session.execute(
                select(QuestionnaireCategoryQuestion).where(
                    and_(
                        QuestionnaireCategoryQuestion.category_id == seed.category_id,
                        QuestionnaireCategoryQuestion.question_id == resolved_question_id,
                    )
                )
            )
            row = existing.scalar_one_or_none()
        if row is None:
            session.add(
                QuestionnaireCategoryQuestion(
                    id=seed.id,
                    category_id=seed.category_id,
                    question_id=resolved_question_id,
                )
            )
        else:
            row.category_id = seed.category_id
            row.question_id = resolved_question_id


async def _upsert_question_options(
    session: AsyncSession,
    question_options: Iterable[SeedQuestionOption],
    question_id_map: dict[int, int],
) -> None:
    for seed in question_options:
        resolved_question_id = question_id_map.get(seed.question_id, seed.question_id)
        row = await session.get(QuestionnaireOption, seed.option_id)
        if row is None:
            existing = await session.execute(
                select(QuestionnaireOption).where(
                    and_(
                        QuestionnaireOption.question_id == resolved_question_id,
                        QuestionnaireOption.option_value == seed.option_value,
                    )
                )
            )
            row = existing.scalar_one_or_none()
        if row is None:
            session.add(
                QuestionnaireOption(
                    option_id=seed.option_id,
                    question_id=resolved_question_id,
                    option_value=seed.option_value,
                    display_name=seed.display_name,
                    tooltip_text=seed.tooltip_text,
                )
            )
        else:
            row.question_id = resolved_question_id
            row.option_value = seed.option_value
            row.display_name = seed.display_name
            row.tooltip_text = seed.tooltip_text


async def _upsert_package_categories(
    session: AsyncSession, package_categories: Iterable[SeedPackageCategory]
) -> None:
    for seed in package_categories:
        row = await session.get(AssessmentPackageCategory, seed.id)
        if row is None:
            existing = await session.execute(
                select(AssessmentPackageCategory).where(
                    and_(
                        AssessmentPackageCategory.package_id == seed.package_id,
                        AssessmentPackageCategory.category_id == seed.category_id,
                    )
                )
            )
            row = existing.scalar_one_or_none()

        if row is None:
            session.add(
                AssessmentPackageCategory(
                    id=seed.id,
                    package_id=seed.package_id,
                    category_id=seed.category_id,
                )
            )
        else:
            row.package_id = seed.package_id
            row.category_id = seed.category_id


async def _upsert_assignments(
    session: AsyncSession, assignments: Iterable[SeedOnboardingAssignment]
) -> None:
    for seed in assignments:
        row = await session.get(OnboardingAssistantAssignment, seed.onboarding_assistant_id)
        if row is None:
            row = OnboardingAssistantAssignment(
                onboarding_assistant_id=seed.onboarding_assistant_id
            )
            session.add(row)
        row.employee_id = seed.employee_id
        row.engagement_id = seed.engagement_id


async def _upsert_participants(session: AsyncSession, slots: Iterable[SeedParticipant]) -> None:
    for seed in slots:
        row = await session.get(EngagementParticipant, seed.engagement_participant_id)
        if row is None:
            row = EngagementParticipant(engagement_participant_id=seed.engagement_participant_id)
            session.add(row)
        row.engagement_id = seed.engagement_id
        row.user_id = seed.user_id
        row.slot_start_time = seed.slot_start_time
        row.engagement_date = seed.engagement_date
        row.participants_employee_id = seed.participants_employee_id
        row.want_doctor_consultation = seed.want_doctor_consultation
        row.want_nutritionist_consultation = seed.want_nutritionist_consultation
        row.want_doctor_and_nutritionist_consultation = seed.want_doctor_and_nutritionist_consultation


async def _reset_sequences(session: AsyncSession) -> None:
    sequence_specs = (
        ("users", "user_id"),
        ("employee", "employee_id"),
        ("organizations", "organization_id"),
        ("assessment_packages", "package_id"),
        ("diagnostic_package", "diagnostic_package_id"),
        ("engagements", "engagement_id"),
        ("questionnaire_categories", "category_id"),
        ("questionnaire_definitions", "question_id"),
        ("questionnaire_options", "option_id"),
        ("questionnaire_category_questions", "id"),
        ("assessment_package_categories", "id"),
        ("onboarding_assistant_assignment", "onboarding_assistant_id"),
        ("engagement_participants", "engagement_participant_id"),
    )

    for table_name, id_column in sequence_specs:
        await session.execute(
            text(
                f"""
                SELECT setval(
                    pg_get_serial_sequence('{table_name}', '{id_column}'),
                    COALESCE((SELECT MAX({id_column}) FROM {table_name}), 1),
                    true
                )
                """
            )
        )


async def seed_sample_data(*, yes: bool) -> None:
    settings.validate()

    if not yes:
        raise SystemExit(
            "Refusing to seed without explicit confirmation. Re-run with --yes to apply changes."
        )

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            await _upsert_users(session, SAMPLE_USERS)
            await _upsert_employees(session, SAMPLE_EMPLOYEES)
            await _upsert_assessment_packages(session, SAMPLE_ASSESSMENT_PACKAGES)
            await _upsert_diagnostic_packages(session, SAMPLE_DIAGNOSTIC_PACKAGES)
            await _upsert_organizations(session, SAMPLE_ORGANIZATIONS)
            await _upsert_engagements(session, SAMPLE_ENGAGEMENTS)
            await _upsert_categories(session, SAMPLE_CATEGORIES)
            question_id_map = await _upsert_questions(session, SAMPLE_QUESTIONS)
            await _upsert_category_questions(session, SAMPLE_CATEGORY_QUESTIONS, question_id_map)
            await _upsert_question_options(session, SAMPLE_QUESTION_OPTIONS, question_id_map)
            await _upsert_package_categories(session, SAMPLE_PACKAGE_CATEGORIES)
            await _upsert_assignments(session, SAMPLE_ASSIGNMENTS)
            await _upsert_participants(session, SAMPLE_PARTICIPANTS)
            await _reset_sequences(session)

            print("Seeded sample data")
            print("Reset PostgreSQL sequences for sample tables")

    await engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed Supershyft sample dashboard data (idempotent, ORM-based)."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag, the command exits without writing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    asyncio.run(seed_sample_data(yes=args.yes))
    return 0
