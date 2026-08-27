"""Tests for idle-in-transaction fixes across audit helpers and related services."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text

from db.session import AsyncSessionLocal
from modules.audit import cron_sync_logging
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.diagnostics.healthians import sync_log as healthians_sync_log
from modules.reports import service as reports_service_module
from modules.reports.service import ReportsService


@pytest.mark.asyncio
async def test_tracked_integration_call_commits_pending_log_before_operation(test_db_session):
    """Pending audit rows must be visible on a separate session before outbound work runs."""
    pending_visible_before_operation = False

    async def operation() -> dict[str, str]:
        nonlocal pending_visible_before_operation
        async with AsyncSessionLocal() as check_db:
            result = await check_db.execute(
                text(
                    "SELECT status FROM integration_sync_logs "
                    "WHERE provider = 'test-tracked-provider' "
                    "ORDER BY sync_log_id DESC LIMIT 1"
                )
            )
            row = result.first()
            pending_visible_before_operation = row is not None and row.status == "pending"
        return {"ok": True}

    result = await tracked_integration_call(
        test_db_session,
        provider="test-tracked-provider",
        api_url="https://example.com/test",
        engagement_id=None,
        user_id=None,
        request_payload={"action": "test"},
        operation=operation,
        persist=False,
    )

    assert result == {"ok": True}
    assert pending_visible_before_operation

    finalized = (
        await test_db_session.execute(
            text(
                "SELECT status, response_payload FROM integration_sync_logs "
                "WHERE provider = 'test-tracked-provider' "
                "ORDER BY sync_log_id DESC LIMIT 1"
            )
        )
    ).first()
    assert finalized is not None
    assert finalized.status == "success"
    assert finalized.response_payload == {"ok": True}


@pytest.mark.asyncio
async def test_tracked_integration_call_persists_before_operation_via_mocks():
    """Unit test: persist/finalize helpers are invoked around operation, not only after."""
    call_order: list[str] = []

    async def fake_persist(**_kwargs) -> int:
        call_order.append("persist")
        return 42

    async def fake_finalize(**_kwargs) -> None:
        call_order.append("finalize")

    async def operation() -> dict[str, str]:
        call_order.append("operation")
        return {"done": True}

    db = AsyncMock()
    with (
        patch.object(cron_sync_logging, "persist_integration_sync_log_isolated", fake_persist),
        patch.object(cron_sync_logging, "finalize_integration_sync_log_isolated", fake_finalize),
    ):
        result = await tracked_integration_call(
            db,
            provider="test",
            api_url="https://example.com",
            engagement_id=None,
            user_id=None,
            request_payload=None,
            operation=operation,
            persist=False,
        )

    assert result == {"done": True}
    assert call_order == ["persist", "operation", "finalize"]


@pytest.mark.asyncio
async def test_tracked_integration_call_finalizes_failure_without_leaking_pending():
    """Failed operations must still finalize the isolated audit log."""
    call_order: list[str] = []

    async def fake_persist(**_kwargs) -> int:
        call_order.append("persist")
        return 99

    async def fake_finalize(**kwargs) -> None:
        call_order.append(f"finalize:{kwargs['status']}")

    async def operation() -> None:
        call_order.append("operation")
        raise RuntimeError("provider down")

    db = AsyncMock()
    with (
        patch.object(cron_sync_logging, "persist_integration_sync_log_isolated", fake_persist),
        patch.object(cron_sync_logging, "finalize_integration_sync_log_isolated", fake_finalize),
    ):
        with pytest.raises(RuntimeError, match="provider down"):
            await tracked_integration_call(
                db,
                provider="test",
                api_url="https://example.com",
                engagement_id=None,
                user_id=None,
                request_payload=None,
                operation=operation,
                persist=False,
            )

    assert call_order == ["persist", "operation", "finalize:failed"]


@pytest.mark.asyncio
async def test_healthians_isolated_logging_commits_before_finalize(monkeypatch):
    """Healthians isolated helpers must commit on their own sessions."""
    commit_calls = 0

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def commit(self):
            nonlocal commit_calls
            commit_calls += 1

    monkeypatch.setattr(healthians_sync_log, "AsyncSessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(
        healthians_sync_log.AuditRepository,
        "create_sync_log",
        AsyncMock(return_value=MagicMock(sync_log_id=7)),
    )
    monkeypatch.setattr(
        healthians_sync_log.AuditRepository,
        "update_sync_log_status",
        AsyncMock(),
    )

    sync_log_id = await healthians_sync_log.persist_healthians_sync_log_isolated(
        engagement_id=1,
        user_id=2,
        provider="healthians",
        api_url="https://example.com",
        request_payload={"x": 1},
        status="pending",
    )
    await healthians_sync_log.finalize_healthians_sync_log_isolated(
        sync_log_id=sync_log_id,
        status="success",
        response_payload={"ok": True},
    )

    assert sync_log_id == 7
    assert commit_calls == 2


def test_blood_parameter_refresh_dedupes_in_process_tasks(monkeypatch):
    """Repeated triggers for the same user must not stack background tasks."""
    reports_service_module._blood_parameter_refresh_in_progress.clear()
    created_tasks: list[int] = []

    def fake_create_task(coro):
        created_tasks.append(1)
        coro.close()
        return MagicMock()

    service = ReportsService(
        repository=MagicMock(),
        assessments_repository=MagicMock(),
        metsights_service=MagicMock(),
        diagnostics_service=MagicMock(),
    )
    monkeypatch.setattr(reports_service_module.asyncio, "create_task", fake_create_task)

    service.trigger_user_blood_parameters_refresh(user_id=123)
    service.trigger_user_blood_parameters_refresh(user_id=123)
    service.trigger_user_blood_parameters_refresh(user_id=456)

    assert len(created_tasks) == 2
    reports_service_module._blood_parameter_refresh_in_progress.clear()


@pytest.mark.asyncio
async def test_notifications_dispatch_uses_isolated_audit_before_webhook(monkeypatch):
    """Notification dispatch must not hold request txn open during n8n webhook."""
    from modules.notifications.service import NotificationsService

    call_order: list[str] = []

    async def fake_persist(**_kwargs) -> int:
        call_order.append("persist")
        return 55

    async def fake_finalize(**_kwargs) -> None:
        call_order.append("finalize")

    class _FakeHttpxResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeHttpxClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, *_args, **_kwargs):
            call_order.append("webhook")
            return _FakeHttpxResponse()

    monkeypatch.setattr(
        "modules.notifications.service.persist_integration_sync_log_isolated",
        fake_persist,
    )
    monkeypatch.setattr(
        "modules.notifications.service.finalize_integration_sync_log_isolated",
        fake_finalize,
    )
    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeHttpxClient)

    db = AsyncMock()
    repo = MagicMock()
    repo.create_notification = AsyncMock(
        return_value=MagicMock(notification_id=1, status="pending")
    )
    repo.update_notification = AsyncMock()
    repo.get_service_by_key = AsyncMock(
        return_value=MagicMock(
            service_key="test-svc",
            channel="email",
            webhook_path="hook",
            is_active=True,
            require_blood_report_url=False,
            require_bio_ai_report_url=False,
            require_participant_detail=False,
            require_external_link=False,
            require_otp=False,
            require_session_details=False,
        )
    )
    repo.get_users_by_ids = AsyncMock(return_value=[MagicMock(user_id=1, first_name="A", last_name="B")])

    service = NotificationsService(repo)
    result = await service.dispatch(
        db,
        payload=MagicMock(
            service_key="test-svc",
            user_ids=[1],
            engagement_id=None,
            assessment_instance_id=None,
            participant_details=None,
            otp=None,
            external_link=None,
            session_details=None,
        ),
        triggered_by_user_id=1,
    )

    assert call_order == ["persist", "webhook", "finalize"]


@pytest.mark.asyncio
async def test_tracked_metsights_call_uses_isolated_audit():
    from modules.metsights.integration_logging import MetsightsSyncContext, tracked_metsights_call

    call_order: list[str] = []

    async def fake_persist(**_kwargs) -> int:
        call_order.append("persist")
        return 7

    async def fake_finalize(**kwargs) -> None:
        call_order.append(f"finalize:{kwargs['status']}")

    async def operation() -> dict[str, str]:
        call_order.append("operation")
        return {"ok": True}

    ctx = MetsightsSyncContext(db=AsyncMock(), engagement_id=1, user_id=2)
    with (
        patch(
            "modules.metsights.integration_logging.persist_integration_sync_log_isolated",
            fake_persist,
        ),
        patch(
            "modules.metsights.integration_logging.finalize_integration_sync_log_isolated",
            fake_finalize,
        ),
    ):
        result = await tracked_metsights_call(
            ctx,
            api_url="https://metsights.example/records/1/",
            operation=operation,
        )

    assert result == {"ok": True}
    assert call_order == ["persist", "operation", "finalize:success"]


@pytest.mark.asyncio
async def test_release_request_transaction_commits_dirty_session(test_db_session):
    from db.transaction import release_request_transaction

    await test_db_session.execute(text("SELECT 1"))
    assert test_db_session.in_transaction()
    await release_request_transaction(test_db_session)
    assert not test_db_session.in_transaction()


@pytest.mark.asyncio
async def test_release_request_transaction_rolls_back_read_only(test_db_session):
    from db.transaction import release_request_transaction

    await test_db_session.execute(text("SELECT 1"))
    await release_request_transaction(test_db_session)
    assert not test_db_session.in_transaction()
