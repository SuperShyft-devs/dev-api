"""Qura HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.service import AssessmentsService
from modules.reports.dependencies import get_reports_service
from modules.reports.service import ReportsService
from modules.qura.context_builder import ContextBuilder
from modules.qura.explainability import EvidenceBuilder
from modules.qura.intent import IntentClassifier
from modules.qura.llm_client import LLMClient
from modules.qura.prompt_builder import PromptBuilder
from modules.qura.query_planner import QueryPlanner
from modules.qura.safety import SafetyLayer
from modules.qura.schemas import ChatRequest
from modules.qura.service import QuraService


router = APIRouter(prefix="/qura", tags=["qura"])


def get_qura_service(
    db: AsyncSession = Depends(get_db),
    reports_service: ReportsService = Depends(get_reports_service),
    assessments_service: AssessmentsService = Depends(get_assessments_service),
) -> QuraService:
    safety = SafetyLayer()
    classifier = IntentClassifier(safety)
    return QuraService(
        safety_layer=safety,
        intent_classifier=classifier,
        query_planner=QueryPlanner(classifier),
        context_builder=ContextBuilder(
            db=db,
            reports_service=reports_service,
            assessments_service=assessments_service,
        ),
        prompt_builder=PromptBuilder(),
        llm_client=LLMClient.from_settings(),
        evidence_builder=EvidenceBuilder(),
    )


@router.post("/chat")
async def chat(
    body: ChatRequest,
    request: Request,
    user=Depends(get_current_user),
    service: QuraService = Depends(get_qura_service),
):
    response = await service.chat(
        user_id=int(user.user_id),
        request=body,
        user_gender=getattr(user, "gender", None),
        user_first_name=getattr(user, "first_name", "") or "",
        user_last_name=getattr(user, "last_name", "") or "",
        ip_address=request.client.host if request.client is not None else "unknown",
        user_agent=request.headers.get("User-Agent", "unknown"),
    )
    return success_response(response.model_dump(mode="json"))
