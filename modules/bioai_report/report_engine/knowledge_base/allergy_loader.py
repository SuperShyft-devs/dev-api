"""Load the allergy knowledge base. Clinical text stays pending until signed off."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

DEFAULT_ALLERGY_KB_DIR = Path(__file__).resolve().parent / "allergy"


class AllergyKnowledgeBase:
    def __init__(self, kb_dir: Path | str | None = None) -> None:
        self.kb_dir = Path(kb_dir) if kb_dir is not None else DEFAULT_ALLERGY_KB_DIR
        registry = _read(self.kb_dir / "allergen_master_registry.json")
        self.registry_version = str(registry.get("kb_version") or "")
        manifest_path = self.kb_dir / "approval_manifest.json"
        self.manifest = _read(manifest_path) if manifest_path.is_file() else {}
        self.version = str(self.manifest.get("kb_version") or self.registry_version)
        self.qualified_clinical_approval = str(
            (self.manifest.get("qualified_clinical_approval") or {}).get("status") or "pending"
        )
        self.entries: list[dict[str, Any]] = list(registry.get("entries") or [])
        self.by_lab_name: dict[str, dict[str, Any]] = {}
        self.by_id: dict[str, dict[str, Any]] = {}
        for entry in self.entries:
            name = entry.get("lab_test_name")
            if isinstance(name, str) and name not in self.by_lab_name:
                self.by_lab_name[name] = entry
            allergen_id = entry.get("allergen_id")
            if isinstance(allergen_id, str):
                self.by_id[allergen_id] = entry
        self.records = _load_records(self.kb_dir)
        rules = _read(self.kb_dir / "allergy_interpretation_rules.json")
        self.interpretation_rules = rules
        self.rules_review_status = str(rules.get("clinical_review_status") or "pending")
        caution = rules.get("caution_library")
        self.caution_library = caution if isinstance(caution, dict) else {}
        self.global_rules = {
            str(item.get("rule_id")): item
            for item in rules.get("global_rules") or []
            if isinstance(item, dict) and item.get("rule_id")
        }
        recommendations = _read(self.kb_dir / "allergy_recommendations.json")
        self.recommendations = {
            item["recommendation_id"]: item
            for item in recommendations.get("recommendations") or []
            if isinstance(item, dict) and item.get("recommendation_id")
        }
        self.recommendations_review = str(recommendations.get("clinical_review_status") or "")
        total = _read(self.kb_dir / "total_ige_interpretation.json")
        self.total_ige = total.get("record") if isinstance(total.get("record"), dict) else {}
        self.total_ige_review_status = str(
            total.get("clinical_review_status") or self.total_ige.get("clinical_review_status") or "pending"
        )
        categories = _read(self.kb_dir / "allergen_categories.json")
        self.source_groups = list(categories.get("source_groups") or [])

    def record(self, allergen_id: str | None) -> dict[str, Any] | None:
        if not allergen_id:
            return None
        return self.records.get(allergen_id)

    def content_released(self) -> bool:
        release = self.manifest.get("content_release") if isinstance(self.manifest, dict) else None
        if not isinstance(release, dict):
            return False
        covers = release.get("covers") if isinstance(release.get("covers"), list) else []
        required = {"allergen_records", "total_ige", "recommendations", "interpretation_rules"}
        return (
            release.get("status") == "approved"
            and release.get("authority") == "project_owner"
            and required <= set(covers)
        )

    def narratives_approved(self, record: dict[str, Any] | None) -> bool:
        if record is None:
            return False
        if str(record.get("clinical_review_status") or "") == "approved" and self.rules_review_status == "approved":
            return True
        if not self.content_released():
            return False
        allergen_id = record.get("allergen_id")
        if allergen_id and allergen_id in self.records:
            return True
        return allergen_id == "ige_total" or record is self.total_ige

    def recommendation_released(self, item: dict[str, Any]) -> bool:
        if str(item.get("clinical_review_status") or "") == "approved":
            return True
        return self.content_released()


def _read(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Allergy knowledge base file must be an object: {path.name}")
    return payload


def _load_records(kb_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    allergen_dir = kb_dir / "allergens"
    if allergen_dir.is_dir():
        for path in sorted(allergen_dir.glob("*.json")):
            payload = _read(path)
            for record in payload.get("allergens") or []:
                if isinstance(record, dict) and record.get("allergen_id"):
                    records[str(record["allergen_id"])] = record
    return records


@lru_cache(maxsize=1)
def default_allergy_kb() -> AllergyKnowledgeBase:
    return AllergyKnowledgeBase()
