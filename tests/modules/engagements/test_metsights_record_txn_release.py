"""Unit tests: release DB transaction before Metsights record HTTP."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from db.transaction import release_request_transaction
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.engagements.assessment_packages_service import EngagementAssessmentPackagesService
from modules.engagements.repository import EngagementsRepository
from modules.metsights.service import MetsightsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.users.repository import UsersRepository


def _build_service(*, metsights: MetsightsService) -> EngagementAssessmentPackagesService:
    return EngagementAssessmentPackagesService(
        engagements_repository=EngagementsRepository(),
        assessments_repository=AssessmentsRepository(),
        reports_repository=ReportsRepository(),
        questionnaire_repository=QuestionnaireRepository(),
        users_repository=UsersRepository(),
        assessments_service=AssessmentsService(AssessmentsRepository()),
        metsights_service=metsights,
        audit_service=AuditService(AuditRepository()),
    )


@pytest.mark.asyncio
async def test_connect_metsights_records_releases_before_create(monkeypatch):
    call_order: list[str] = []

    async def _release(db):
        call_order.append("release")
        await release_request_transaction(db)

    async def _create(self, *, profile_id: str, assessment_type_code: str):
        call_order.append("create")
        assert profile_id == "profile-abc"
        assert assessment_type_code == "2"
        return "NEWREC01"

    monkeypatch.setattr(
        "modules.engagements.assessment_packages_service.release_request_transaction",
        _release,
    )
    monkeypatch.setattr(MetsightsService, "create_record_for_profile", _create)

    service = _build_service(metsights=MetsightsService(client=MagicMock()))

    mock_package = MagicMock()
    mock_package.package_code = "METSIGHTS_PRO"
    mock_package.assessment_type_code = "2"

    mock_instance = MagicMock()
    mock_instance.assessment_instance_id = 501
    mock_instance.user_id = 601
    mock_instance.metsights_record_id = None

    mock_user = MagicMock()
    mock_user.metsights_profile_id = "profile-abc"

    service._engagements.get_engagement_by_id = AsyncMock(return_value=MagicMock())
    service._assessments_repo.get_package_by_id = AsyncMock(return_value=mock_package)
    service._assessments_repo.list_instances_for_engagement_and_package = AsyncMock(
        return_value=[mock_instance]
    )
    service._users.get_user_by_id = AsyncMock(return_value=mock_user)
    service._assessments_repo.set_metsights_record_id = AsyncMock()
    service._audit.log_event = AsyncMock()

    db = MagicMock()
    db.in_transaction.return_value = False
    db.dirty = False
    db.new = False
    db.deleted = False
    employee = EmployeeContext(employee_id=701, user_id=701, role="admin")

    result = await service.connect_metsights_records_for_package(
        db,
        engagement_id=801,
        package_id=2,
        employee=employee,
        ip_address="127.0.0.1",
        user_agent="pytest",
        endpoint="/test/connect",
    )

    assert call_order == ["release", "create"]
    assert result["connected"] == 1
    service._assessments_repo.set_metsights_record_id.assert_awaited_once()


@pytest.mark.asyncio
async def test_add_package_backfill_releases_before_create(monkeypatch):
    call_order: list[str] = []

    async def _release(db):
        call_order.append("release")
        await release_request_transaction(db)

    async def _create(self, *, profile_id: str, assessment_type_code: str):
        call_order.append("create")
        return "BACKFILLREC"

    monkeypatch.setattr(
        "modules.engagements.assessment_packages_service.release_request_transaction",
        _release,
    )
    monkeypatch.setattr(MetsightsService, "create_record_for_profile", _create)

    service = _build_service(metsights=MetsightsService(client=MagicMock()))

    mock_package = MagicMock()
    mock_package.package_id = 2
    mock_package.package_code = "METSIGHTS_PRO"
    mock_package.assessment_type_code = "2"
    mock_package.status = "active"

    mock_existing = MagicMock()
    mock_existing.assessment_instance_id = 901
    mock_existing.metsights_record_id = None

    mock_user = MagicMock()
    mock_user.metsights_profile_id = "profile-backfill"

    service._engagements.get_engagement_by_id = AsyncMock(
        return_value=MagicMock(status="running")
    )
    service._assessments_repo.get_package_by_code = AsyncMock(return_value=mock_package)
    service._engagements.list_distinct_participant_ids_for_engagement = AsyncMock(
        return_value=[1001]
    )
    service._assessments_repo.get_instance_by_user_engagement_package = AsyncMock(
        return_value=mock_existing
    )
    service._users.get_user_by_id = AsyncMock(return_value=mock_user)
    service._assessments_repo.set_metsights_record_id = AsyncMock()
    service._audit.log_event = AsyncMock()

    db = MagicMock()
    db.in_transaction.return_value = False
    db.dirty = False
    db.new = False
    db.deleted = False
    employee = EmployeeContext(employee_id=701, user_id=701, role="admin")

    result = await service.add_package_to_engagement(
        db,
        engagement_id=802,
        package_code="METSIGHTS_PRO",
        current_user_id=701,
        employee=employee,
        ip_address="127.0.0.1",
        user_agent="pytest",
        endpoint="/test/add-package",
    )

    assert call_order == ["release", "create"]
    assert result["errors"] == []
    service._assessments_repo.set_metsights_record_id.assert_awaited_once()
