"""HTTP routes that return assembled BioReport JSON (no PDF).

Frontend contract — single call, single input:

    GET /bioai-report/{assessment_instance_id}

The backend fetches assessment + patient demographics, enriches, assembles,
and returns one self-contained BioReport JSON plus historical trends cut off
at the requested assessment date.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.dependencies import get_current_user
from core.exceptions import AppError
from db.session import get_db
from modules.bioai_report.report_engine.api.dependencies import (
    get_bioai_trend_service,
    get_bioreport_service,
)
from modules.bioai_report.report_engine.exceptions import KnowledgeBaseError, ReportEngineError
from modules.bioai_report.report_engine.services.report_service import BioReportService
from modules.bioai_report.report_engine.services.trend_service import BioAITrendService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bioai-report", tags=["bioai-report"])


@router.get("/{assessment_instance_id}")
async def get_bioreport_content(
    assessment_instance_id: int,
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
    report_service: BioReportService = Depends(get_bioreport_service),
    trend_service: BioAITrendService = Depends(get_bioai_trend_service),
):
    """Return one complete BioReport for ``assessment_instance_id``.

    Pipeline (all server-side):
    1. Resolve Metsights ``record_id`` from DB using ``assessment_instance_id``
    2. Fetch assessment JSON from MetSights ``GET /reports/{record_id}/``
    3. Enrich patient demographics for the same ``record_id``
    4. Merge into one assessment object
    5. Build BioReport (patient → summary → disease sections + KB)
    6. Attach ``health_trends`` cut off at this assessment date
    7. Return the raw ``BioReport`` JSON object plus ``health_trends``
    """
    if assessment_instance_id is None:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="assessment_instance_id is required",
        )

    try:
        report = await report_service.generate_for_assessment_instance(
            assessment_instance_id=int(assessment_instance_id),
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

    payload = report.to_dict()
    # Additive only: existing report keys are never rewritten. health_trends is
    # attached as a new top-level field (object or false).
    # Enrichment may swallow a DB error; Postgres then rejects later statements
    # in this request until the transaction is reset.
    try:
        await db.rollback()
    except Exception:
        logger.exception(
            "Bio-AI report could not reset DB session before trends for assessment_instance_id=%s",
            assessment_instance_id,
        )
    payload["health_trends"] = await trend_service.embed_for_assessment_instance(
        db,
        assessment_instance_id=int(assessment_instance_id),
        report_payload=payload,
    )
    return payload
