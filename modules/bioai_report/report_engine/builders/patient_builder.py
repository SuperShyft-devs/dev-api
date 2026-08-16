"""Build the patient object for BioReport output."""

from __future__ import annotations

from modules.bioai_report.report_engine.models.assessment import AssessmentPayload
from modules.bioai_report.report_engine.models.report import PatientInfo


def build_patient(assessment: AssessmentPayload) -> PatientInfo:
    """Map assessment fields into the patient section of the BioReport."""
    return PatientInfo(
        record_id=assessment.record_id,
        name=assessment.name,
        age=assessment.age,
        sex=assessment.sex,
        gender=assessment.gender or assessment.sex,
        date_of_birth=assessment.date_of_birth,
        height=assessment.height,
        weight=assessment.weight,
        bmi=assessment.bmi,
        profile_id=assessment.profile_id,
        metabolic_age=assessment.metabolic_age,
        metabolic_score=assessment.metabolic_score,
        metabolic_health_status=assessment.metabolic_health_status,
        assessment_code=assessment.assessment_code,
        assessment_date=assessment.assessment_date or assessment.created_at,
    )
