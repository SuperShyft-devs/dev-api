"""Pytest configuration and shared fixtures.

Database behavior:

- If ``TEST_DATABASE_URL`` is set (recommended): it is copied to ``DATABASE_URL``
  before any app code imports the DB engine. Each session runs
  ``alembic downgrade base`` (ignored if it fails, e.g. empty DB), then
  ``alembic upgrade head`` and ``python -m db.seed --yes``. Tests use a
  session-scoped async engine (no per-test schema drop). Avoid ``DROP SCHEMA
  public`` here: it can orphan PostgreSQL types and break the next ``alembic`` run.

- If ``TEST_DATABASE_URL`` is unset (legacy): tests use ``DATABASE_URL`` and the
  ``test_engine`` fixture drops ``public`` and recreates tables with SQLAlchemy
  metadata for every test — dangerous on a shared dev database.

Tests must still clean up data they insert (see autouse cleanup fixture).
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env before reading TEST_DATABASE_URL (same as core.config).
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Must run before any import that reads DATABASE_URL (core.config, db.session).
_use_isolated_test_db = bool(os.getenv("TEST_DATABASE_URL"))
if _use_isolated_test_db:
    os.environ["DATABASE_URL"] = os.getenv("TEST_DATABASE_URL", "")

import subprocess
import sys
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.base import Base
from modules.audit import models as _audit_models  # noqa: F401
from modules.auth import models as _auth_models  # noqa: F401
from modules.users import models as _users_models  # noqa: F401
from modules.employee import models as _employee_models  # noqa: F401
from modules.engagements import models as _engagements_models  # noqa: F401
from modules.organizations import models as _organizations_models  # noqa: F401
from modules.assessments import models as _assessments_models  # noqa: F401
from modules.questionnaire import models as _questionnaire_models  # noqa: F401
from modules.diagnostics import models as _diagnostics_models  # noqa: F401
from modules.reports import models as _reports_models  # noqa: F401
from modules.platform_settings import models as _platform_settings_models  # noqa: F401

from core.config import settings
from core.exceptions import add_exception_handlers
from core.logging import request_id_middleware
from db.session import get_db
from modules.auth.dependencies import get_auth_service
from modules.auth.router import router as auth_router
from modules.engagements.router import router as engagements_router
from modules.checklists.router import router as checklists_router
from modules.organizations.router import router as organizations_router
from modules.users.router import router as users_router
from modules.employee.router import router as employees_router
from modules.assessments.router import router as assessments_router
from modules.assessments.packages_router import router as assessment_packages_router
from modules.questionnaire.router import router as questionnaire_router
from modules.reports.router import router as reports_router
from modules.platform_settings.router import router as platform_settings_router


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _run_subprocess(argv: list[str]) -> None:
    result = subprocess.run(
        argv,
        cwd=_project_root(),
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        out = (result.stdout or "") + (result.stderr or "")
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(argv)}\n{out}"
        )


def pytest_sessionstart(session: pytest.Session) -> None:
    if not _use_isolated_test_db:
        return

    os.environ.setdefault(
        "JWT_SECRET_KEY",
        os.getenv("JWT_SECRET_KEY", "test-pytest-jwt-secret-16+"),
    )

    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        raise RuntimeError("TEST_DATABASE_URL / DATABASE_URL is required for isolated test DB")

    # Tear down migration state without DROP SCHEMA (avoids orphaned pg types on Windows PG).
    subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "base"],
        cwd=_project_root(),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )
    _run_subprocess([sys.executable, "-m", "alembic", "upgrade", "head"])
    # Diagnostics CSVs live under dev-api/db/seed/csv (not workspace root).
    csv_dir = _project_root() / "db" / "seed" / "csv"
    env = os.environ.copy()
    env.setdefault("DIAGNOSTICS_CSV_DIR", str(csv_dir.resolve()))
    result = subprocess.run(
        [sys.executable, "-m", "db.seed", "--yes"],
        cwd=_project_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): db.seed\n"
            f"{(result.stdout or '') + (result.stderr or '')}"
        )


@pytest.fixture(autouse=True)
def _set_test_settings():
    """Set required settings for tests."""
    settings.JWT_SECRET_KEY = settings.JWT_SECRET_KEY or "test-secret"
    settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 5
    settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS = 7
    settings.ALLOW_BYPASS_OTP = False


class CapturingOtpSender:
    """OTP sender that captures OTP for tests."""

    def __init__(self):
        self.last_phone: str | None = None
        self.last_otp: str | None = None

    async def send_otp(self, phone: str, otp: str) -> None:
        self.last_phone = phone
        self.last_otp = otp


async def _ensure_required_tables(connection) -> None:
    """Recreate all tables for tests (legacy path only).

    Drops everything first so schema changes apply immediately without Alembic.
    """
    await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
    await connection.execute(text("CREATE SCHEMA public"))
    await connection.execute(text("GRANT ALL ON SCHEMA public TO public"))
    await connection.execute(text("GRANT ALL ON SCHEMA public TO CURRENT_USER"))
    await connection.run_sync(Base.metadata.create_all)


async def _create_test_engine_legacy():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required to run DB-backed tests")

    engine = create_async_engine(database_url, echo=False, pool_pre_ping=True)

    async with engine.begin() as conn:
        await _ensure_required_tables(conn)

    return engine


if _use_isolated_test_db:

    @pytest_asyncio.fixture(scope="session", loop_scope="session")
    async def test_engine():
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("TEST_DATABASE_URL is required for isolated test DB mode")

        engine = create_async_engine(database_url, echo=False, pool_pre_ping=True)
        yield engine
        await engine.dispose()

else:

    @pytest_asyncio.fixture(loop_scope="session")
    async def test_engine():
        engine = await _create_test_engine_legacy()
        yield engine
        await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def test_db_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create a new DB session for each test."""
    session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest.fixture
def otp_sender() -> CapturingOtpSender:
    return CapturingOtpSender()


@pytest.fixture
def auth_service(otp_sender: CapturingOtpSender):
    """Real AuthService wired with a capturing OTP sender."""

    from modules.audit.repository import AuditRepository
    from modules.audit.service import AuditService
    from modules.auth.repository import AuthRepository
    from modules.auth.service import AuthService
    from modules.users.repository import UsersRepository
    from modules.users.service import UsersService

    return AuthService(
        repository=AuthRepository(),
        users_service=UsersService(UsersRepository()),
        audit_service=AuditService(AuditRepository()),
        otp_sender=otp_sender,
    )


@pytest_asyncio.fixture(loop_scope="session")
async def fastapi_app(test_db_session: AsyncSession, auth_service, otp_sender: CapturingOtpSender) -> FastAPI:
    """FastAPI app wired to the real DB and a capturing OTP sender."""

    app = FastAPI()
    add_exception_handlers(app)
    app.middleware("http")(request_id_middleware)
    app.include_router(auth_router)
    app.include_router(users_router)
    app.include_router(organizations_router)
    app.include_router(engagements_router)
    app.include_router(checklists_router)
    app.include_router(employees_router)
    app.include_router(assessments_router)
    app.include_router(assessment_packages_router)
    app.include_router(questionnaire_router)
    app.include_router(reports_router)
    app.include_router(platform_settings_router)

    async def _get_test_db():
        yield test_db_session

    app.dependency_overrides[get_db] = _get_test_db
    app.dependency_overrides[get_auth_service] = lambda: auth_service

    app.state.otp_sender = otp_sender

    return app


@pytest_asyncio.fixture(loop_scope="session")
async def async_client(fastapi_app: FastAPI):
    """Async HTTP client for the FastAPI app."""
    transport = ASGITransport(app=fastapi_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _cleanup_auth_test_rows(test_db_session: AsyncSession):
    """Remove rows created by tests.

    Legacy mode also wipes reference-like tables because the next test rebuilds an
    empty schema. Isolated mode keeps ``db.seed`` reference data between tests.
    """
    yield

    # If a test failed mid-transaction, the connection can be left in an aborted state.
    # Rollback first so cleanup queries can run.
    await test_db_session.rollback()

    # Delete audit logs first - they reference auth_otp_sessions
    if _use_isolated_test_db:
        _non_seed_users = "user_id NOT IN (1, 2)"
    else:
        _non_seed_users = "user_id >= 1001 AND user_id < 10000"
    await test_db_session.execute(text(f"DELETE FROM data_audit_logs WHERE {_non_seed_users}"))
    await test_db_session.execute(text(f"DELETE FROM auth_otp_sessions WHERE {_non_seed_users}"))
    await test_db_session.execute(text(f"DELETE FROM auth_tokens WHERE {_non_seed_users}"))
    await test_db_session.execute(text("DELETE FROM questionnaire_responses"))
    await test_db_session.execute(text("DELETE FROM questionnaire_healthy_habit_rules"))
    await test_db_session.execute(text("DELETE FROM assessment_category_progress"))
    await test_db_session.execute(text("DELETE FROM individual_health_report"))
    await test_db_session.execute(text("DELETE FROM organization_health_report"))
    await test_db_session.execute(text("DELETE FROM reports_user_sync_state"))
    await test_db_session.execute(text("DELETE FROM engagement_time_slots"))
    await test_db_session.execute(text("DELETE FROM assessment_instances"))
    await test_db_session.execute(text("DELETE FROM onboarding_assistant_assignment"))
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    if _use_isolated_test_db:
        # Restore B2C defaults (matches db.seed); tests that customize settings delete/replace this row.
        await test_db_session.execute(
            text(
                "INSERT INTO platform_settings (settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id) "
                "VALUES (1, 1, 6)"
            )
        )
    await test_db_session.execute(text("DELETE FROM engagements"))
    if _use_isolated_test_db:
        # Tests insert ad-hoc assessment_packages; seed keeps MET_BASIC / MET_PRO / FitPrint as ids 1–3.
        await test_db_session.execute(
            text(
                "DELETE FROM assessment_package_categories WHERE package_id NOT IN (1, 2, 3)"
            )
        )
        await test_db_session.execute(
            text("DELETE FROM assessment_packages WHERE package_id NOT IN (1, 2, 3)")
        )
        # After package↔category links for ad-hoc packages are gone, drop test questionnaire rows.
        await test_db_session.execute(text("DELETE FROM questionnaire_options WHERE question_id > 44"))
        await test_db_session.execute(
            text(
                "DELETE FROM questionnaire_category_questions WHERE category_id > 4 OR question_id > 44"
            )
        )
        await test_db_session.execute(text("DELETE FROM questionnaire_definitions WHERE question_id > 44"))
        await test_db_session.execute(text("DELETE FROM questionnaire_categories WHERE category_id > 4"))
        await test_db_session.execute(text("DELETE FROM user_preferences WHERE user_id NOT IN (1, 2)"))
    await test_db_session.execute(text("DELETE FROM organizations"))

    if _use_isolated_test_db:
        await test_db_session.execute(text("DELETE FROM employee WHERE user_id NOT IN (1, 2)"))
    else:
        await test_db_session.execute(text("DELETE FROM assessment_package_categories"))
        await test_db_session.execute(text("DELETE FROM questionnaire_category_questions"))
        await test_db_session.execute(text("DELETE FROM assessment_packages"))
        await test_db_session.execute(text("DELETE FROM questionnaire_options"))
        await test_db_session.execute(text("DELETE FROM questionnaire_definitions"))
        await test_db_session.execute(text("DELETE FROM questionnaire_categories"))
        await test_db_session.execute(text("DELETE FROM diagnostic_package"))
        await test_db_session.execute(text("DELETE FROM employee"))

    if _use_isolated_test_db:
        await test_db_session.execute(text("DELETE FROM users WHERE user_id NOT IN (1, 2)"))
    else:
        await test_db_session.execute(
            text(
                "DELETE FROM users WHERE user_id >= 1001 AND user_id < 10000 "
                "OR phone IN ("
                "'9999999999','8888888888','7777777777','6666666666','5555555555',"
                "'1111111111','2222222222','3333333333','4444444444','7777700000',"
                "'1234567890','1234567891','8877665501','5550000998','9301000000','9411000000','9876543210',"
                "'6100000001','6100000002','6100000003','6100000004','6100000005',"
                "'6111111111','6111111112','6111111113','6111111114'"
                ")"
            )
        )

    await test_db_session.commit()
