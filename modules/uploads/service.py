"""Media uploads service."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import filetype
from fastapi import UploadFile

from core.config import settings
from core.exceptions import AppError


_ALLOWED_CONTENT_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}

_PDF_CONTENT_TYPE = "application/pdf"
_PDF_EXTENSION = ".pdf"

_CONSULTATION_ATTACHMENT_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "text/plain": ".txt",
}


class UploadsService:
    """Validate and store uploaded media files."""

    def __init__(self) -> None:
        self._media_root = Path(settings.MEDIA_ROOT)
        self._media_base_url = settings.MEDIA_BASE_URL.rstrip("/")
        self._user_max_bytes = settings.USER_PROFILE_PHOTO_MAX_MB * 1024 * 1024
        self._org_max_bytes = settings.ORG_LOGO_MAX_MB * 1024 * 1024
        self._expert_max_bytes = settings.EXPERT_PROFILE_PHOTO_MAX_MB * 1024 * 1024
        self._package_max_bytes = settings.PACKAGE_IMAGE_MAX_MB * 1024 * 1024
        self._pdf_max_bytes = settings.USER_PDF_UPLOAD_MAX_MB * 1024 * 1024

    async def save_user_profile_photo(self, file: UploadFile) -> str:
        return await self._save_image(file, folder="users", max_bytes=self._user_max_bytes)

    async def save_organization_logo(self, file: UploadFile) -> str:
        return await self._save_image(file, folder="organizations", max_bytes=self._org_max_bytes)

    async def save_expert_profile_photo(self, file: UploadFile) -> str:
        return await self._save_image(file, folder="experts", max_bytes=self._expert_max_bytes)

    async def save_package_image(self, file: UploadFile) -> str:
        return await self._save_image(file, folder="packages", max_bytes=self._package_max_bytes)

    async def save_bio_ai_pdf(self, file: UploadFile) -> str:
        return await self._save_pdf(file, folder="bio-ai", max_bytes=self._pdf_max_bytes)

    async def save_blood_parameters_pdf(self, file: UploadFile) -> str:
        return await self._save_pdf(file, folder="blood-parameters", max_bytes=self._pdf_max_bytes)

    async def save_consultation_attachments(self, files: list[UploadFile]) -> list[str]:
        if not files:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="At least one file is required")
        urls: list[str] = []
        for file in files:
            urls.append(
                await self._save_consultation_attachment(
                    file, folder="consultation-attachments", max_bytes=self._pdf_max_bytes
                )
            )
        return urls

    async def _save_consultation_attachment(self, file: UploadFile, *, folder: str, max_bytes: int) -> str:
        content_type = (file.content_type or "").lower().strip()
        extension = _CONSULTATION_ATTACHMENT_TYPES.get(content_type)
        if extension is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Unsupported attachment type; allowed: images, PDF, DOC, DOCX, TXT",
            )

        payload = await file.read()
        if not payload:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="File is required")
        if len(payload) > max_bytes:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="File is too large")

        if content_type == _PDF_CONTENT_TYPE:
            if not payload.startswith(b"%PDF"):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="File content does not match declared type",
                )
        elif content_type.startswith("image/"):
            kind = filetype.guess(payload)
            detected_mime = kind.mime if kind else None
            if detected_mime != content_type:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="File content does not match declared type",
                )

        target_dir = self._media_root / folder
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{uuid4().hex}{extension}"
        file_path = target_dir / filename
        file_path.write_bytes(payload)

        return f"{self._media_base_url}/{folder}/{filename}"

    async def _save_pdf(self, file: UploadFile, *, folder: str, max_bytes: int) -> str:
        content_type = (file.content_type or "").lower().strip()
        if content_type != _PDF_CONTENT_TYPE:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid file type; PDF required")

        payload = await file.read()
        if not payload:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="File is required")
        if len(payload) > max_bytes:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="File is too large")

        if not payload.startswith(b"%PDF"):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="File content does not match declared type",
            )

        kind = filetype.guess(payload)
        detected_mime = kind.mime if kind else None
        if detected_mime != _PDF_CONTENT_TYPE:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="File content does not match declared type",
            )

        target_dir = self._media_root / folder
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{uuid4().hex}{_PDF_EXTENSION}"
        file_path = target_dir / filename
        file_path.write_bytes(payload)

        return f"{self._media_base_url}/{folder}/{filename}"

    async def _save_image(self, file: UploadFile, *, folder: str, max_bytes: int) -> str:
        content_type = (file.content_type or "").lower().strip()
        extension = _ALLOWED_CONTENT_TYPES.get(content_type)
        if extension is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid image type")

        payload = await file.read()
        if not payload:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="File is required")
        if len(payload) > max_bytes:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="File is too large")

        kind = filetype.guess(payload)
        detected_mime = kind.mime if kind else None
        if detected_mime != content_type:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="File content does not match declared type",
            )

        target_dir = self._media_root / folder
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{uuid4().hex}{extension}"
        file_path = target_dir / filename
        file_path.write_bytes(payload)

        return f"{self._media_base_url}/{folder}/{filename}"
