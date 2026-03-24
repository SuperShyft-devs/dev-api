"""Pytest configuration and shared fixtures.

These tests use the database configured by `DATABASE_URL`.

Important: the application creates its SQLAlchemy engine at import time from
`DATABASE_URL` (via `core.config.settings`).

Tests must clean up data they insert.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from typing import AsyncGenerator

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
    """Recreate all tables for tests.

    We drop everything first so schema changes apply immediately.
    """
    await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
    await connection.execute(text("CREATE SCHEMA public"))
    await connection.execute(text("GRANT ALL ON SCHEMA public TO public"))
    await connection.execute(text("GRANT ALL ON SCHEMA public TO CURRENT_USER"))
    await connection.run_sync(Base.metadata.create_all)


@pytest_asyncio.fixture
async def test_engine():
    """Create an async engine within the current test event loop."""

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required to run DB-backed tests")

    engine = create_async_engine(database_url, echo=False, pool_pre_ping=True)

    async with engine.begin() as conn:
        await _ensure_required_tables(conn)

    yield engine

    await engine.dispose()


@pytest_asyncio.fixture
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


@pytest_asyncio.fixture
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

    async def _get_test_db():
        yield test_db_session

    app.dependency_overrides[get_db] = _get_test_db
    app.dependency_overrides[get_auth_service] = lambda: auth_service

    app.state.otp_sender = otp_sender

    return app


@pytest_asyncio.fixture
async def async_client(fastapi_app: FastAPI):
    """Async HTTP client for the FastAPI app."""
    transport = ASGITransport(app=fastapi_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest_asyncio.fixture(autouse=True)
async def _cleanup_auth_test_rows(test_db_session: AsyncSession):
    """Remove rows created by auth tests.

    We only delete rows that match the user_ids and phones used in auth tests.
    """
    yield

    # If a test failed mid-transaction, the connection can be left in an aborted state.
    # Rollback first so cleanup queries can run.
    await test_db_session.rollback()

    user_ids = (1001, 1002, 1003, 1004, 1010, 2001)
    phones = (
        "9999999999",
        "8888888888",
        "7777777777",
        "6666666666",
        "5555555555",
        "1111111111",
        "2222222222",
        "3333333333",
        '4444444444',
        "7777700000",
    )

    # Delete audit logs first - they reference auth_otp_sessions
    # Clean up test users in various ranges: 1001-1999, 2001-2999, 8001-8999
    await test_db_session.execute(
        text("DELETE FROM data_audit_logs WHERE user_id >= 1001 AND user_id < 10000")
    )
    await test_db_session.execute(
        text("DELETE FROM auth_otp_sessions WHERE user_id >= 1001 AND user_id < 10000")
    )
    await test_db_session.execute(
        text("DELETE FROM auth_tokens WHERE user_id >= 1001 AND user_id < 10000")
    )
    await test_db_session.execute(text("DELETE FROM questionnaire_responses"))
    await test_db_session.execute(text("DELETE FROM assessment_category_progress"))
    await test_db_session.execute(text("DELETE FROM individual_health_report"))
    await test_db_session.execute(text("DELETE FROM organization_health_report"))
    await test_db_session.execute(text("DELETE FROM engagement_time_slots"))
    await test_db_session.execute(text("DELETE FROM assessment_instances"))
    await test_db_session.execute(text("DELETE FROM onboarding_assistant_assignment"))
    await test_db_session.execute(text("DELETE FROM engagements"))
    await test_db_session.execute(text("DELETE FROM assessment_package_categories"))
    await test_db_session.execute(text("DELETE FROM questionnaire_category_questions"))
    await test_db_session.execute(text("DELETE FROM assessment_packages"))
    await test_db_session.execute(text("DELETE FROM questionnaire_options"))
    await test_db_session.execute(text("DELETE FROM questionnaire_definitions"))
    await test_db_session.execute(text("DELETE FROM questionnaire_categories"))
    await test_db_session.execute(text("DELETE FROM diagnostic_package"))
    await test_db_session.execute(text("DELETE FROM organizations"))
    await test_db_session.execute(text("DELETE FROM employee"))

    await test_db_session.execute(
        text(
            "DELETE FROM users WHERE user_id >= 1001 AND user_id < 10000 "
            "OR phone IN ('9999999999','8888888888','7777777777','6666666666','5555555555','1111111111','2222222222','3333333333','4444444444','7777700000')"
        )
    )

    await test_db_session.commit()
