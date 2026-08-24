#!/usr/bin/env python3
"""Offline runner for the full production Bio-AI Report Engine pipeline.

Production and offline share the same pipeline after assessment load:

    enrichment → builders → knowledge-base selection → BioReport

The **only** difference is the assessment source:

- Production: ``generate_for_record(record_id)`` fetches MetSights
  ``GET /reports/{record_id}/``
- Offline: this script loads a local assessment JSON file, then calls
  ``generate_from_assessment_with_enrichment`` (same enrichment + assembly)

Usage:
    python test_engine.py report_user1.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from modules.assessments.repository import AssessmentsRepository  # noqa: E402
from modules.bioai_report.report_engine.api.dependencies import (  # noqa: E402
    get_assessment_fetch_service,
    get_knowledge_base_store,
)
from modules.bioai_report.report_engine.services.patient_service import (  # noqa: E402
    PatientProfileService,
)
from modules.bioai_report.report_engine.services.report_service import (  # noqa: E402
    BioReportService,
)
from modules.bioai_report.report_engine.utils.patient_enrichment import (  # noqa: E402
    missing_demographic_fields,
    needs_patient_enrichment,
)
from modules.metsights.dependencies import get_metsights_service  # noqa: E402
from modules.questionnaire.repository import QuestionnaireRepository  # noqa: E402
from modules.users.repository import UsersRepository  # noqa: E402


def log(stage: str, message: str = "") -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    if message:
        print(f"[{stamp}] ▶ {stage}\n    {message}")
    else:
        print(f"[{stamp}] ▶ {stage}")


def log_json(stage: str, payload: Any, *, max_chars: int = 6000) -> None:
    try:
        text = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    except TypeError:
        text = repr(payload)
    if len(text) > max_chars:
        text = text[:max_chars] + f"\n… truncated ({len(text)} chars total)"
    log(stage, text)


def enrichment_event_reporter(stage: str, status: str, detail: str) -> None:
    """Print every enrichment stage clearly — never silent on empty/fail."""
    label = {
        "record_detail": "Record Detail response",
        "profile_detail": "Profile Detail response",
        "physical_measurement": "Physical Measurement response",
        "user_api": "Patient/User API",
        "questionnaire": "Questionnaire (height/weight/bmi)",
        "metsights_fallback": "MetSights fallback",
        "enrichment": "Patient enrichment",
    }.get(stage, stage.replace("_", " ").title())
    icon = {
        "ok": "OK",
        "extracted": "OK",
        "merged": "OK",
        "identifiers": "…",
        "started": "…",
        "skipped": "SKIP",
        "empty": "EMPTY",
        "failed": "FAIL",
    }.get(status, status.upper())
    log(f"{label} [{icon}]", detail)


def resolve_record_id(assessment: dict[str, Any]) -> str:
    body = assessment.get("data") if isinstance(assessment.get("data"), dict) else assessment
    for key in ("record", "record_id", "id"):
        value = body.get(key) if isinstance(body, dict) else None
        if value is not None and str(value).strip():
            return str(value).strip()
    raise SystemExit(
        "Could not resolve record_id from assessment JSON "
        "(expected data.record / data.record_id / data.id)"
    )


def load_assessment(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise SystemExit(f"Input file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Assessment root must be a JSON object, got {type(payload).__name__}")
    # Unwrap MetSights envelope so the pipeline sees the same shape as
    # AssessmentFetchService.fetch_raw (which returns the inner report body).
    data = payload.get("data")
    if isinstance(data, dict) and (
        "diseases" in data or "metabolic_score" in data or "metabolic_age" in data
    ):
        return data
    return payload


def create_production_service() -> BioReportService:
    """Create BioReportService with the same dependencies as production DI."""
    return BioReportService(
        assessment_service=get_assessment_fetch_service(),
        patient_service=PatientProfileService(
            metsights_service=get_metsights_service(),
            assessments_repository=AssessmentsRepository(),
            users_repository=UsersRepository(),
            questionnaire_repository=QuestionnaireRepository(),
            event_reporter=enrichment_event_reporter,
        ),
        kb_store=get_knowledge_base_store(),
    )


async def run_pipeline(input_path: Path) -> dict[str, Any]:
    """Run the production enrichment + assembly path with a local assessment file.

    Equivalent to ``generate_for_record`` after the MetSights fetch step.
    """
    assessment = load_assessment(input_path)
    record_id = resolve_record_id(assessment)

    log(
        "Assessment loaded (offline source)",
        f"file={input_path.resolve()} record_id={record_id}",
    )
    log(
        "Pipeline parity",
        "same as production generate_for_record after assessment fetch: "
        "generate_from_assessment_with_enrichment → builders → BioReport",
    )

    missing_before = missing_demographic_fields(assessment)
    log(
        "Missing demographic fields (before enrichment)",
        json.dumps(missing_before or ["<none>"]),
    )
    log(
        "Enrichment required",
        str(needs_patient_enrichment(assessment)),
    )

    service = create_production_service()
    log(
        "BioReportService created",
        "AssessmentFetchService + PatientProfileService + KnowledgeBaseStore",
    )

    log(
        "Production enrichment+assembly started",
        "await service.generate_from_assessment_with_enrichment(...)",
    )

    try:
        from db.session import AsyncSessionLocal

        async with AsyncSessionLocal() as db:
            report = await service.generate_from_assessment_with_enrichment(
                assessment,
                record_id=record_id,
                db=db,
            )
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        # DB may be unavailable offline — still run the exact production method without db.
        log(
            "Database session unavailable",
            f"{type(exc).__name__}: {exc} — retrying generate_from_assessment_with_enrichment(db=None)",
        )
        try:
            report = await service.generate_from_assessment_with_enrichment(
                assessment,
                record_id=record_id,
                db=None,
            )
        except Exception as inner:  # noqa: BLE001
            log("Pipeline FAILED", f"{type(inner).__name__}: {inner}")
            traceback.print_exc()
            raise SystemExit(1) from inner

    patient = report.patient.model_dump(mode="json")
    still_missing = [
        field
        for field in ("name", "gender", "sex", "age", "height", "weight", "bmi")
        if patient.get(field) in (None, "")
    ]
    log(
        "Missing demographic fields (after enrichment)",
        json.dumps(still_missing or ["<none>"]),
    )
    log_json("Final BioReport.patient", patient)

    log(
        "Final BioReport generated",
        f"disease_count={report.report_metadata.disease_count} "
        f"engine_version={report.report_metadata.engine_version} "
        f"kb_version={report.report_metadata.kb_version} "
        f"template_version={report.report_metadata.template_version}",
    )

    # Additive test-runner requirement:
    # Do not write `output_*.json` files. Print the JSON object instead.
    report_json = report.to_dict()
    print(json.dumps(report_json, indent=2, ensure_ascii=False))
    return report_json


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full production Bio-AI Report Engine against a local assessment JSON "
            "(same enrichment + assembly as GET /bioai-report/{assessment_instance_id}; "
            "no FastAPI / JWT)."
        ),
    )
    parser.add_argument(
        "assessment_json",
        nargs="?",
        default="report_user1.json",
        help="Path to assessment JSON (default: report_user1.json)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = Path(args.assessment_json).expanduser()
    if not input_path.is_absolute():
        input_path = Path.cwd() / input_path
    try:
        asyncio.run(run_pipeline(input_path))
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else 1
    except Exception as exc:  # noqa: BLE001
        log("FATAL", f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
        return 1
    print("\nSUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
