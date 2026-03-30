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

    async def get_or_create_profile_id(
        self,
        *,
        first_name: str,
        last_name: str,
        phone: str,
        email: str | None,
        gender: str,
        date_of_birth: str | None,
        age: int | None,
    ) -> str:
        if not settings.METSIGHTS_API_KEY:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        safe_first = (first_name or "").strip()
        safe_last = (last_name or "").strip()
        safe_phone = (phone or "").strip()
        safe_gender = (gender or "").strip()
        safe_email = (email or "").strip() if email is not None else None
        safe_dob = (date_of_birth or "").strip() if date_of_birth is not None else None

        if not safe_first or not safe_last or not safe_phone or not safe_gender:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Missing required profile fields")

        payload: dict[str, Any] = {
            "first_name": safe_first,
            "last_name": safe_last,
            "phone": safe_phone,
            "gender": safe_gender,
        }
        if safe_email:
            payload["email"] = safe_email
        if safe_dob:
            payload["date_of_birth"] = safe_dob
        elif age is not None:
            payload["age"] = int(age)
        else:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="DOB or age is required")

        try:
            created = await self._client.create_profile(data=payload)
            envelope = MetsightsEnvelope.model_validate(created)
            data = envelope.data if isinstance(envelope.data, dict) else {}
            profile_id = str(data.get("id") or "").strip()
            if not profile_id:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights profile creation returned no id",
                )
            return profile_id
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 403:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights authorization failed",
                ) from exc
            if status_code == 400:
                # Common case: profile already exists (phone/email uniqueness).
                # Try to locate it via search and reuse its id.
                async def _find_existing_by_search(search: str | None, field: str, expected: str) -> str | None:
                    if not search:
                        return None
                    listed = await self._client.list_profiles(search=search)
                    envelope = MetsightsEnvelope.model_validate(listed)
                    rows = envelope.data if isinstance(envelope.data, list) else []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        if str(row.get(field) or "").strip().lower() != expected.strip().lower():
                            continue
                        existing_id = str(row.get("id") or "").strip()
                        if existing_id:
                            return existing_id
                    return None

                try:
                    existing_by_phone = await _find_existing_by_search(safe_phone, "phone", safe_phone)
                    if existing_by_phone:
                        return existing_by_phone
                except Exception:
                    pass

                if safe_email:
                    try:
                        existing_by_email = await _find_existing_by_search(safe_email, "email", safe_email)
                        if existing_by_email:
                            return existing_by_email
                    except Exception:
                        pass

                # If email is causing uniqueness conflicts, retry creation without email.
                if safe_email:
                    payload_without_email = dict(payload)
                    payload_without_email.pop("email", None)
                    try:
                        created_wo_email = await self._client.create_profile(data=payload_without_email)
                        envelope = MetsightsEnvelope.model_validate(created_wo_email)
                        data = envelope.data if isinstance(envelope.data, dict) else {}
                        profile_id = str(data.get("id") or "").strip()
                        if profile_id:
                            return profile_id
                    except Exception:
                        pass
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
