"""FastAPI dependency providers for the Bio-AI report content engine."""

from __future__ import annotations

from functools import lru_cache

from modules.assessments.repository import AssessmentsRepository
from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.services.assessment_service import AssessmentFetchService
from modules.bioai_report.report_engine.services.patient_service import PatientProfileService
from modules.bioai_report.report_engine.services.report_service import BioReportService
from modules.metsights.dependencies import get_metsights_service
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository


@lru_cache(maxsize=1)
def get_knowledge_base_store() -> KnowledgeBaseStore:
    return KnowledgeBaseStore()


def get_assessment_fetch_service() -> AssessmentFetchService:
    return AssessmentFetchService(metsights_service=get_metsights_service())


def get_patient_profile_service() -> PatientProfileService:
    return PatientProfileService(
        metsights_service=get_metsights_service(),
        assessments_repository=AssessmentsRepository(),
        users_repository=UsersRepository(),
        questionnaire_repository=QuestionnaireRepository(),
    )


def get_bioreport_service() -> BioReportService:
    return BioReportService(
        assessment_service=get_assessment_fetch_service(),
        patient_service=get_patient_profile_service(),
        kb_store=get_knowledge_base_store(),
    )
