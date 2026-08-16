"""Load and cache disease knowledge-base JSON files."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from modules.bioai_report.report_engine.exceptions import KnowledgeBaseError
from modules.bioai_report.report_engine.models.knowledge_base import DiseaseKnowledgeBase

logger = logging.getLogger(__name__)

DEFAULT_KB_DIR = Path(__file__).resolve().parent


class KnowledgeBaseStore:
    """Filesystem-backed store of disease knowledge bases.

    Dropping a new valid ``{disease_id}.json`` into the knowledge_base directory
    is enough to support that disease — no code changes required.
    """

    def __init__(self, kb_dir: Path | str | None = None) -> None:
        self._kb_dir = Path(kb_dir) if kb_dir is not None else DEFAULT_KB_DIR
        self._cache: dict[str, DiseaseKnowledgeBase] = {}

    @property
    def kb_dir(self) -> Path:
        return self._kb_dir

    def list_disease_ids(self) -> list[str]:
        """Return sorted disease ids available as JSON files."""
        return sorted(path.stem for path in self._kb_dir.glob("*.json"))

    def has_disease(self, disease_id: str) -> bool:
        return (self._kb_dir / f"{disease_id}.json").is_file()

    def get(self, disease_id: str) -> DiseaseKnowledgeBase:
        """Load (and cache) a disease knowledge base by id."""
        if disease_id in self._cache:
            return self._cache[disease_id]

        path = self._kb_dir / f"{disease_id}.json"
        if not path.is_file():
            raise KnowledgeBaseError(
                f"Knowledge base not found for disease '{disease_id}' "
                f"(expected file: {path.name})"
            )

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise KnowledgeBaseError(
                f"Invalid JSON in knowledge base '{path.name}': {exc}"
            ) from exc

        if not isinstance(payload, dict):
            raise KnowledgeBaseError(f"Knowledge base '{path.name}' must be a JSON object")

        # Ensure disease_id is set even if omitted in the file header.
        payload.setdefault("disease_id", disease_id)
        payload.setdefault("display_name", disease_id.replace("_", " ").title())

        try:
            kb = DiseaseKnowledgeBase.model_validate(payload)
        except Exception as exc:  # pydantic ValidationError
            raise KnowledgeBaseError(
                f"Knowledge base '{path.name}' failed validation: {exc}"
            ) from exc

        self._cache[disease_id] = kb
        logger.debug("Loaded knowledge base for disease_id=%s from %s", disease_id, path.name)
        return kb

    def clear_cache(self) -> None:
        self._cache.clear()
