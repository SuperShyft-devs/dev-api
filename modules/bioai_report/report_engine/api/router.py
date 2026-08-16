"""HTTP routes that return assembled BioReport JSON (no PDF).

Frontend contract — single call, single input:

    GET /bioai-report/content/{record_id}

The backend fetches assessment + patient demographics, enriches, assembles,
and returns one self-contained BioReport JSON. The frontend only renders it.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.exceptions import AppError
from db.session import get_db
from modules.bioai_report.report_engine.api.dependencies import get_bioreport_service
from modules.bioai_report.report_engine.exceptions import KnowledgeBaseError, ReportEngineError
from modules.bioai_report.report_engine.services.report_service import BioReportService

router = APIRouter(prefix="/bioai-report", tags=["bioai-report"])


@router.get("/content/{record_id}")
async def get_bioreport_content(
    record_id: str,
    assessment_type_code: str | None = Query(
        default=None,
        description=(
            "Optional MetSights assessment type code (e.g. 1=Basic, 2=Pro). "
            "Not required from the frontend for normal Bio-AI reports."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
    report_service: BioReportService = Depends(get_bioreport_service),
):
    """Return one complete BioReport for ``record_id``.

    Pipeline (all server-side):
    1. Fetch assessment JSON from MetSights ``GET /reports/{record_id}/``
    2. Enrich patient demographics for the same ``record_id``
    3. Merge into one assessment object
    4. Build BioReport (patient → summary → disease sections + KB)
    5. Return ``{ data: BioReport, meta: {} }``
    """
    rid = (record_id or "").strip()
    if not rid:
        raise AppError(status_code=422, error_code="INVALID_STATE", message="record_id is required")

    try:
        report = await report_service.generate_for_record(
            record_id=rid,
            assessment_type_code=assessment_type_code,
            db=db,
        )
    except KnowledgeBaseError as exc:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message=str(exc),
        ) from exc
    except ReportEngineError as exc:
        raise AppError(
            status_code=500,
            error_code="INTERNAL_ERROR",
            message=str(exc),
        ) from exc
    except ValueError as exc:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message=str(exc),
        ) from exc

    return success_response(report.to_dict())
