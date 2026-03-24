"""Business service for Metsights communication."""

from __future__ import annotations

from typing import Any

import httpx

from core.config import settings
from core.exceptions import AppError
from modules.metsights.client import MetsightsClient
from modules.metsights.schemas import MetsightsEnvelope


class MetsightsService:
    """Encapsulates Metsights error handling and resource retrieval."""

    def __init__(self, client: MetsightsClient):
        self._client = client

    async def get_blood_parameters(self, *, record_id: str) -> Any:
        normalized_record_id = (record_id or "").strip()
        if not normalized_record_id:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights record id is missing")
        if not settings.METSIGHTS_API_KEY:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        try:
            payload = await self._client.get_record_resource(record_id=normalized_record_id, resource="blood-parameters")
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="BLOOD_PARAMETERS_NOT_FOUND",
                    message="Blood parameters not found",
                ) from exc
            if status_code == 403:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights authorization failed",
                ) from exc
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights request failed",
            ) from exc
        except httpx.HTTPError as exc:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights request failed",
            ) from exc

        envelope = MetsightsEnvelope.model_validate(payload)
        return envelope.data
