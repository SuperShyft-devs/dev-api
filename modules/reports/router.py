"""Reports HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.reports.dependencies import get_reports_service
from modules.reports.schemas import BloodParameterTrendResponse, HealthSpanIndexRequest
from modules.reports.service import ReportsService


router = APIRouter(prefix="/reports", tags=["reports"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("/{assessment_id}/overview")
async def get_overview_report(
    assessment_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    response = await reports_service.get_overview_for_user(
        db,
        assessment_id=assessment_id,
        user_id=user.user_id,
        user_gender=user.gender,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(response.model_dump())


@router.get("/{assessment_id}/risk-analysis")
async def get_risk_analysis(
    assessment_id: int,
    request: Request,
    disease: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    if disease is not None and disease.strip():
        result = await reports_service.get_disease_detail_for_user(
            db,
            assessment_id=assessment_id,
            user_id=user.user_id,
            disease_code=disease.strip(),
            ip_address=_client_ip(request),
            user_agent=request.headers.get("User-Agent", "unknown"),
            endpoint=str(request.url.path),
        )
    else:
        result = await reports_service.get_risk_analysis_for_user(
            db,
            assessment_id=assessment_id,
            user_id=user.user_id,
            ip_address=_client_ip(request),
            user_agent=request.headers.get("User-Agent", "unknown"),
            endpoint=str(request.url.path),
        )
    await db.commit()
    return success_response(result.model_dump())


@router.get("/{assessment_id}/blood-parameters")
async def get_blood_parameters_report(
    assessment_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    blood_parameter_groups = await reports_service.get_blood_parameters_for_user(
        db,
        assessment_id=assessment_id,
        user_id=user.user_id,
        user_gender=user.gender,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response([group.model_dump() for group in blood_parameter_groups])


@router.get("/{assessment_id}/bio-ai/pdf")
async def get_bio_ai_pdf_report(
    assessment_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    response = await reports_service.get_bio_ai_pdf_for_user(
        db,
        assessment_id=assessment_id,
        user_id=user.user_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(response.model_dump())


@router.post("/{assessment_instance_id}/health-span-index")
async def get_health_span_index(
    assessment_instance_id: int,
    body: HealthSpanIndexRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    response = await reports_service.get_health_span_index(
        db,
        assessment_instance_id=assessment_instance_id,
        user_id=user.user_id,
        source_assessment_instance_ids=body.source_assessment_instance_ids,
        include_details=body.include_details,
    )
    await db.commit()
    return success_response(response.model_dump(exclude_none=not body.include_details))


@router.get("/trends")
async def get_blood_parameter_trends(
    blood_parameter: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    payload, meta, should_trigger = await reports_service.get_blood_parameter_trends_for_user(
        db,
        user_id=user.user_id,
        blood_parameter=blood_parameter,
    )
    if should_trigger:
        await db.commit()
        reports_service.trigger_user_blood_parameters_refresh(user_id=user.user_id)
    response = BloodParameterTrendResponse.model_validate(payload)
    return success_response(response.model_dump(), meta=meta)
