"""Database seeding command.

Production guidance (aligned with instructions/*):
- Seeding must be explicit (never on app startup).
- Seeds must be idempotent (safe to re-run).
- Use ORM, not raw SQL.
- Run after migrations.

Entrypoint: `python -m db.seed --yes`
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings
from modules.assessments.models import AssessmentPackage
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireDefinition, QuestionnaireOption
from modules.employee.models import Employee
from modules.users.models import User


@dataclass(frozen=True)
class SeedUser:
    user_id: int
    first_name: str
    last_name: str
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
class SeedAssessmentPackage:
    package_id: int
    package_code: str
    display_name: str
    status: str


@dataclass(frozen=True)
class SeedCategory:
    category_id: int
    category_key: str
    display_name: str


@dataclass(frozen=True)
class SeedQuestion:
    question_id: int
    question_key: str
    question_text: str
    question_type: str
    category_id: int
    is_required: bool
    is_read_only: bool
    help_text: str | None
    status: str


@dataclass(frozen=True)
class SeedOption:
    option_id: int
    question_id: int
    option_value: str
    display_name: str
    tooltip_text: str | None


DEFAULT_USERS: tuple[SeedUser, ...] = (
    SeedUser(
        user_id=1,
        first_name="Rishi",
        last_name="Nagar",
        phone="9898898912",
        email="rishi@supershyft.com",
        date_of_birth=date(1995, 1, 1),
        gender="male",
        address="Sher-E-Punjab, Pump House, Andheri East",
        pin_code="400059",
        city="Mumbai",
        state="Maharashtra",
        country="India",
        referred_by=None,
        is_participant=False,
        status="active",
    ),
)

DEFAULT_EMPLOYEES: tuple[SeedEmployee, ...] = (
    SeedEmployee(employee_id=1, user_id=1, role="admin", status="active"),
)

DEFAULT_ASSESSMENT_PACKAGES: tuple[SeedAssessmentPackage, ...] = (
    SeedAssessmentPackage(
        package_id=1,
        package_code="METSIGHTS_BASIC",
        display_name="Metsights Basic",
        status="active",
    ),
    SeedAssessmentPackage(
        package_id=2,
        package_code="METSIGHTS_PRO",
        display_name="Metsights Pro",
        status="active",
    ),
)

DEFAULT_CATEGORIES: tuple[SeedCategory, ...] = (
    SeedCategory(category_id=1, category_key="general", display_name="General"),
)

DEFAULT_QUESTIONS: tuple[SeedQuestion, ...] = (
    SeedQuestion(
        question_id=1,
        question_key="energy_level_weekly",
        question_text="How would you rate your energy levels this week?",
        question_type="scale",
        category_id=1,
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
)

DEFAULT_OPTIONS: tuple[SeedOption, ...] = ()


async def _upsert_users(session: AsyncSession, users: Iterable[SeedUser]) -> None:
    for seed in users:
        existing = await session.get(User, seed.user_id)
        if existing is None:
            session.add(
                User(
                    user_id=seed.user_id,
                    first_name=seed.first_name,
                    last_name=seed.last_name,
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


async def _upsert_employees(session: AsyncSession, employees: Iterable[SeedEmployee]) -> None:
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


async def _upsert_assessment_packages(
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
                    status=seed.status,
                )
            )
        else:
            existing.package_code = seed.package_code
            existing.display_name = seed.display_name
            existing.status = seed.status


async def _upsert_categories(session: AsyncSession, categories: Iterable[SeedCategory]) -> None:
    for seed in categories:
        existing = await session.get(QuestionnaireCategory, seed.category_id)
        if existing is None:
            session.add(
                QuestionnaireCategory(
                    category_id=seed.category_id,
                    category_key=seed.category_key,
                    display_name=seed.display_name,
                )
            )
        else:
            existing.category_key = seed.category_key
            existing.display_name = seed.display_name


async def _upsert_questions(session: AsyncSession, questions: Iterable[SeedQuestion]) -> None:
    for seed in questions:
        existing = await session.get(QuestionnaireDefinition, seed.question_id)
        if existing is None:
            session.add(
                QuestionnaireDefinition(
                    question_id=seed.question_id,
                    question_key=seed.question_key,
                    question_text=seed.question_text,
                    question_type=seed.question_type,
                    category_id=seed.category_id,
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
            existing.category_id = seed.category_id
            existing.is_required = seed.is_required
            existing.is_read_only = seed.is_read_only
            existing.help_text = seed.help_text
            existing.status = seed.status


async def _upsert_options(session: AsyncSession, options: Iterable[SeedOption]) -> None:
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


async def _reset_sequences(session: AsyncSession) -> None:
    """Reset PostgreSQL sequences after manual ID insertion.
    
    This ensures auto-increment starts from the correct value.
    When we manually insert records with specific IDs (like user_id=1),
    PostgreSQL's sequence doesn't automatically update. We must sync it.
    """
    # Reset user_id sequence to continue from max existing ID
    await session.execute(
        text("""
        SELECT setval(
            pg_get_serial_sequence('users', 'user_id'),
            COALESCE((SELECT MAX(user_id) FROM users), 1),
            true
        )
        """)
    )
    
    # Reset employee_id sequence
    await session.execute(
        text("""
        SELECT setval(
            pg_get_serial_sequence('employee', 'employee_id'),
            COALESCE((SELECT MAX(employee_id) FROM employee), 1),
            true
        )
        """)
    )
    
    # Reset package_id sequence
    await session.execute(
        text("""
        SELECT setval(
            pg_get_serial_sequence('assessment_packages', 'package_id'),
            COALESCE((SELECT MAX(package_id) FROM assessment_packages), 1),
            true
        )
        """)
    )

    # Reset questionnaire sequences
    await session.execute(
        text("""
        SELECT setval(
            pg_get_serial_sequence('questionnaire_categories', 'category_id'),
            COALESCE((SELECT MAX(category_id) FROM questionnaire_categories), 1),
            true
        )
        """)
    )
    await session.execute(
        text("""
        SELECT setval(
            pg_get_serial_sequence('questionnaire_definitions', 'question_id'),
            COALESCE((SELECT MAX(question_id) FROM questionnaire_definitions), 1),
            true
        )
        """)
    )
    await session.execute(
        text("""
        SELECT setval(
            pg_get_serial_sequence('questionnaire_options', 'option_id'),
            COALESCE((SELECT MAX(option_id) FROM questionnaire_options), 1),
            true
        )
        """)
    )


async def seed_reference_data(*, yes: bool) -> None:
    """Seed reference data.

    Intended to be run after migrations.

    Args:
        yes: Must be True to perform any writes.
    """

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
            await _upsert_users(session, DEFAULT_USERS)
            await _upsert_employees(session, DEFAULT_EMPLOYEES)
            await _upsert_assessment_packages(session, DEFAULT_ASSESSMENT_PACKAGES)
            await _upsert_categories(session, DEFAULT_CATEGORIES)
            await _upsert_questions(session, DEFAULT_QUESTIONS)
            await _upsert_options(session, DEFAULT_OPTIONS)
            
            # CRITICAL: Reset sequences after manual ID insertion
            await _reset_sequences(session)
            
            print("✓ Seeded users, employees, and assessment packages")
            print("✓ Reset PostgreSQL sequences for auto-increment")

    await engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed Supershyft reference data (idempotent, ORM-based).")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag, the command exits without writing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    asyncio.run(seed_reference_data(yes=args.yes))
    return 0
