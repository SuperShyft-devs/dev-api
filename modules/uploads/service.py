"""Media uploads service."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import UploadFile

from core.config import settings
from core.exceptions import AppError


_ALLOWED_CONTENT_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}


class UploadsService:
    """Validate and store uploaded media files."""

    def __init__(self) -> None:
        self._media_root = Path(settings.MEDIA_ROOT)
        self._media_base_url = settings.MEDIA_BASE_URL.rstrip("/")
        self._user_max_bytes = settings.USER_PROFILE_PHOTO_MAX_MB * 1024 * 1024
        self._org_max_bytes = settings.ORG_LOGO_MAX_MB * 1024 * 1024

    async def save_user_profile_photo(self, file: UploadFile) -> str:
        return await self._save_image(file, folder="users", max_bytes=self._user_max_bytes)

    async def save_organization_logo(self, file: UploadFile) -> str:
        return await self._save_image(file, folder="organizations", max_bytes=self._org_max_bytes)

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

        target_dir = self._media_root / folder
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{uuid4().hex}{extension}"
        file_path = target_dir / filename
        file_path.write_bytes(payload)

        return f"{self._media_base_url}/{folder}/{filename}"
