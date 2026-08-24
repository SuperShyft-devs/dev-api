"""Orchestrate patient → diseases → summary → final BioReport assembly."""

from __future__ import annotations

from datetime import datetime, timezone

from modules.bioai_report.report_engine import config
from modules.bioai_report.report_engine.builders.disease_builder import build_disease_sections
from modules.bioai_report.report_engine.builders.patient_builder import build_patient
from modules.bioai_report.report_engine.builders.summary_builder import build_executive_summary
from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.models.assessment import AssessmentPayload
from modules.bioai_report.report_engine.models.report import BioReport, ReportMetadata
from modules.bioai_report.report_engine.utils.assessment_normalizer import normalize_assessment


def _kb_version(kb_store: KnowledgeBaseStore, disease_ids: list[str]) -> str | None:
    versions: list[str] = []
    for disease_id in disease_ids:
        try:
            versions.append(kb_store.get(disease_id).knowledge_base_version)
        except Exception:
            continue
    if not versions:
        return None
    unique = sorted(set(versions))
    return unique[0] if len(unique) == 1 else ",".join(unique)


def build_bioreport(
    assessment: AssessmentPayload | dict,
    *,
    record_id: str | None = None,
    kb_store: KnowledgeBaseStore | None = None,
    generated_at: str | None = None,
) -> BioReport:
    """Assemble a complete render-ready BioReport from an enriched assessment.

    Pipeline (deterministic, no PDF, no LLM):
      Patient Builder → Disease Builder (KB slab selection) → Executive Summary
      → Report Metadata → BioReport

    Input must already include disease scores and any available patient
    demographics (enrichment happens in ``BioReportService`` before this call).
    """
    store = kb_store or KnowledgeBaseStore()

    if isinstance(assessment, AssessmentPayload):
        payload = assessment
        if record_id and not payload.record_id:
            payload = payload.model_copy(update={"record_id": record_id})
    else:
        payload = normalize_assessment(assessment, record_id=record_id)

    resolved_record_id = payload.record_id or (record_id or "")
    if record_id and payload.record_id != record_id:
        payload = payload.model_copy(update={"record_id": record_id})
        resolved_record_id = record_id

    patient = build_patient(payload)
    disease_sections = build_disease_sections(payload.diseases, kb_store=store)
    executive_summary = build_executive_summary(
        patient=patient,
        disease_sections=disease_sections,
    )

    timestamp = generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    disease_ids = [section.disease_id for section in disease_sections]

    return BioReport(
        patient=patient,
        executive_summary=executive_summary,
        disease_sections=disease_sections,
        report_metadata=ReportMetadata(
            record_id=resolved_record_id,
            engine_version=config.ENGINE_VERSION,
            kb_version=_kb_version(store, disease_ids),
            template_version=config.TEMPLATE_VERSION,
            generated_at=timestamp,
            disease_count=len(disease_sections),
            source="metsights",
        ),
    )
