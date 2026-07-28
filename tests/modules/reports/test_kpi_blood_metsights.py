"""Unit tests for KPI Metsights blood collection status helper."""

import pytest

from core.exceptions import AppError
from modules.reports.camp_reports_service import CampReportsService


class _FakeMetsights:
    def __init__(self, *, behavior: str):
        self.behavior = behavior
        self.calls = 0

    async def get_fetch_collections(self, *, record_id: str):
        self.calls += 1
        if self.behavior == "collected":
            return {"reference_id": "123"}
        if self.behavior == "missing":
            raise AppError(
                status_code=422,
                error_code="BLOOD_SAMPLE_NOT_COLLECTED",
                message="Sample collection does not exist for this record",
            )
        raise AppError(
            status_code=503,
            error_code="EXTERNAL_SERVICE_UNAVAILABLE",
            message="Metsights request failed",
        )


class _FakeReportsService:
    def __init__(self, metsights):
        self._metsights_service = metsights


def _service_with_metsights(behavior: str) -> tuple[CampReportsService, _FakeMetsights]:
    metsights = _FakeMetsights(behavior=behavior)
    svc = CampReportsService.__new__(CampReportsService)
    svc._reports_service = _FakeReportsService(metsights)
    return svc, metsights


@pytest.mark.asyncio
async def test_check_metsights_sample_collection_collected():
    svc, metsights = _service_with_metsights("collected")
    assert await svc._check_metsights_sample_collection(record_id="abc") == "collected"
    assert metsights.calls == 1


@pytest.mark.asyncio
async def test_check_metsights_sample_collection_missing():
    svc, _ = _service_with_metsights("missing")
    assert await svc._check_metsights_sample_collection(record_id="abc") == "missing"


@pytest.mark.asyncio
async def test_check_metsights_sample_collection_failed():
    svc, _ = _service_with_metsights("failed")
    assert await svc._check_metsights_sample_collection(record_id="abc") == "failed"
