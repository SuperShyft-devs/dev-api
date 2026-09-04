"""Archive blood diagnostic PDFs to permanent supershyft.com storage."""

from __future__ import annotations

import logging
import os
import re
import secrets
import string
from pathlib import Path

import httpx

from core.config import settings

logger = logging.getLogger(__name__)

_FILENAME_ALPHABET = string.ascii_letters + string.digits
_FILENAME_LENGTH = 16
_FILENAME_PATTERN = re.compile(r"^[A-Za-z0-9]{16}\.pdf$")


def is_archived_blood_report_url(url: str | None) -> bool:
    """Return True when ``url`` is a persisted supershyft.com blood report link."""
    if not url or not isinstance(url, str):
        return False
    stripped = url.strip()
    if not stripped:
        return False
    base = (settings.BLOOD_REPORTS_BASE_URL or "").strip().rstrip("/")
    if not base:
        return False
    prefix = f"{base}/"
    if not stripped.startswith(prefix):
        return False
    filename = stripped[len(prefix):]
    return bool(_FILENAME_PATTERN.match(filename))


def generate_blood_report_filename() -> str:
    """Return a random 16-character alphanumeric filename (without extension)."""
    return "".join(secrets.choice(_FILENAME_ALPHABET) for _ in range(_FILENAME_LENGTH))


async def archive_blood_report_pdf(
    source_url: str,
    *,
    assessment_instance_id: int,
) -> str:
    """Download a PDF from ``source_url``, store locally, and return the public URL."""
    source = (source_url or "").strip()
    if not source:
        raise ValueError("source_url is required")

    max_bytes = settings.BLOOD_REPORTS_MAX_MB * 1024 * 1024
    timeout = float(settings.BLOOD_REPORTS_TIMEOUT_SECONDS)

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(source)
            response.raise_for_status()
            payload = response.content
    except httpx.HTTPError as exc:
        logger.warning(
            "Blood report PDF download failed for assessment_instance_id=%s: %s",
            assessment_instance_id,
            exc,
        )
        raise

    if not payload:
        raise ValueError("Downloaded PDF is empty")
    if len(payload) > max_bytes:
        raise ValueError("Downloaded PDF exceeds maximum allowed size")
    if not payload.startswith(b"%PDF"):
        raise ValueError("Downloaded file is not a PDF")

    reports_root = Path(settings.BLOOD_REPORTS_ROOT)
    reports_root.mkdir(parents=True, exist_ok=True)

    filename = generate_blood_report_filename()
    final_path = reports_root / f"{filename}.pdf"
    tmp_path = reports_root / f"{filename}.pdf.tmp"

    try:
        tmp_path.write_bytes(payload)
        os.replace(tmp_path, final_path)
    except OSError as exc:
        if tmp_path.is_file():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        logger.warning(
            "Blood report PDF write failed for assessment_instance_id=%s: %s",
            assessment_instance_id,
            exc,
        )
        raise

    public_url = f"{settings.BLOOD_REPORTS_BASE_URL.rstrip('/')}/{filename}.pdf"
    logger.info(
        "Archived blood report PDF for assessment_instance_id=%s at %s",
        assessment_instance_id,
        public_url,
    )
    return public_url


async def resolve_persistable_diagnostic_report_url(
    healthians_url: str,
    *,
    is_full_report: bool,
    existing_url: str | None,
    assessment_instance_id: int,
) -> str | None:
    """Return the URL to store in ``individual_health_report.diagnostic_report_url``.

    Partial Healthians reports must never be persisted (their signed S3 links expire).
    Only a successful archive to ``BLOOD_REPORTS_BASE_URL`` (or an already-archived
    existing URL) is stored. Callers should write the returned value even when it is
    ``None`` so leftover S3 URLs are cleared.
    """
    healthians = (healthians_url or "").strip()
    existing = (existing_url or "").strip()

    if not is_full_report:
        # Keep an already-archived supershyft URL; otherwise leave/clear to null.
        if existing and is_archived_blood_report_url(existing):
            return existing
        return None

    if existing and is_archived_blood_report_url(existing):
        return existing

    if not healthians:
        return None

    try:
        return await archive_blood_report_pdf(
            healthians,
            assessment_instance_id=assessment_instance_id,
        )
    except Exception as exc:
        logger.warning(
            "Blood report archival failed for assessment_instance_id=%s: %s",
            assessment_instance_id,
            exc,
        )
        return None


def diagnostic_report_url_to_persist(
    *,
    is_full_report: bool,
    persistable_url: str | None,
    existing_url: str | None,
) -> str | None:
    """Canonical ``diagnostic_report_url`` value to write (never a Healthians/S3 link)."""
    existing = (existing_url or "").strip() or None
    persistable = (persistable_url or "").strip() or None

    if persistable and is_archived_blood_report_url(persistable):
        return persistable

    if existing and is_archived_blood_report_url(existing):
        # Full-report archival failed: keep prior archive. Partial: keep archive.
        return existing

    # Never persist Healthians/S3 (or any non-archived) URLs.
    return None
