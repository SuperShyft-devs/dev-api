"""HTTP routes that return assembled BioReport JSON (no PDF).

Frontend contract — single call, single input:

    GET /bioai-report/{assessment_instance_id}

The backend fetches assessment + patient demographics, enriches, assembles,
and returns one self-contained BioReport JSON. The frontend only renders it.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from db.session import get_db
from modules.bioai_report.report_engine.api.dependencies import get_bioreport_service
from modules.bioai_report.report_engine.exceptions import KnowledgeBaseError, ReportEngineError
from modules.bioai_report.report_engine.services.report_service import BioReportService
from modules.employee.access_control import ensure_internal_employee
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext

router = APIRouter(prefix="/bioai-report", tags=["bioai-report"])


@router.get("/{assessment_instance_id}")
async def get_bioreport_content(
    assessment_instance_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    report_service: BioReportService = Depends(get_bioreport_service),
):
    """Return one complete BioReport for ``assessment_instance_id``.

    Restricted to internal employees (admin / onboarding assistant). Cron jobs
    use ``BioReportService`` in-process via ``register_permanent_bio_ai_report_url``.

    Pipeline (all server-side):
    1. Resolve Metsights ``record_id`` from DB using ``assessment_instance_id``
    2. Fetch assessment JSON from MetSights ``GET /reports/{record_id}/``
    3. Enrich patient demographics for the same ``record_id``
    4. Merge into one assessment object
    5. Build BioReport (patient → summary → disease sections + KB)
    6. Return the raw ``BioReport`` JSON object
    """
    ensure_internal_employee(employee)

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

    # Additive: return the final report JSON object directly.
    return report.to_dict()
