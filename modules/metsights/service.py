"""Business service for Metsights communication."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from core.config import settings
from core.exceptions import AppError
from modules.metsights.client import MetsightsClient
from modules.metsights.integration_logging import MetsightsSyncContext, metsights_api_url, tracked_metsights_call
from modules.metsights.schemas import MetsightsEnvelope, MetsightsProfilesPage

logger = logging.getLogger(__name__)


def _normalize_phone_digits(raw: str | None) -> str:
    return "".join(ch for ch in str(raw or "") if ch.isdigit())


def _phones_equivalent(left: str | None, right: str | None) -> bool:
    left_digits = _normalize_phone_digits(left)
    right_digits = _normalize_phone_digits(right)
    if not left_digits or not right_digits:
        return False
    if left_digits == right_digits:
        return True
    # Treat +91XXXXXXXXXX and 10-digit local numbers as equivalent.
    if len(left_digits) >= 10 and len(right_digits) >= 10:
        return left_digits[-10:] == right_digits[-10:]
    return False


def _profile_name_matches(row: dict[str, Any], *, first_name: str, last_name: str) -> bool:
    row_first = str(row.get("first_name") or "").strip().lower()
    row_last = str(row.get("last_name") or "").strip().lower()
    target_first = (first_name or "").strip().lower()
    target_last = (last_name or "").strip().lower()
    if not row_first or not row_last or not target_first or not target_last:
        return False
    if row_first == target_first and row_last == target_last:
        return True
    # MetSights may store a leading initial (e.g. "S Pratheek").
    row_first_tokens = row_first.split()
    target_first_tokens = target_first.split()
    return row_last == target_last and (
        row_first == target_first
        or row_first.endswith(target_first)
        or target_first.endswith(row_first)
        or row_first_tokens[-1:] == target_first_tokens[-1:]
    )


class MetsightsService:
    """Encapsulates Metsights error handling and resource retrieval."""

    def __init__(self, client: MetsightsClient):
        self._client = client

    def _require_api_key(self) -> None:
        if not settings.METSIGHTS_API_KEY:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

    async def get_profile_detail(self, *, profile_id: str) -> Any:
        """GET /profiles/:profile_id/ — profile details."""

        pid = (profile_id or "").strip()
        if not pid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights profile id is missing")
        self._require_api_key()
        try:
            payload = await self._client.get_profile_detail(profile_id=pid)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="PROFILE_NOT_FOUND",
                    message="Metsights profile not found",
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

    async def list_profiles_page(self, *, page: int = 1) -> MetsightsProfilesPage:
        """GET /profiles/?page=N — paginated profile list."""

        if page < 1:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="page must be >= 1")
        self._require_api_key()
        try:
            payload = await self._client.list_profiles(
                page=page,
                timeout_seconds=settings.METSIGHTS_IMPORT_TIMEOUT_SECONDS,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
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

        raw_data = payload.get("data")
        rows = raw_data if isinstance(raw_data, list) else []
        normalized_rows = [row for row in rows if isinstance(row, dict)]
        return MetsightsProfilesPage(
            detail=payload.get("detail"),
            count=int(payload.get("count") or 0),
            next=payload.get("next"),
            previous=payload.get("previous"),
            data=normalized_rows,
        )

    async def list_profile_records(
        self,
        *,
        profile_id: str,
        completed: str | None = None,
        code: str | None = None,
        search: str | None = None,
        sync_context: MetsightsSyncContext | None = None,
    ) -> Any:
        """GET /profiles/:profile_id/records/ — returns envelope `data` (list or dict)."""

        pid = (profile_id or "").strip()
        if not pid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights profile id is missing")
        self._require_api_key()
        query_params: dict[str, str] = {}
        if completed is not None and str(completed).strip() != "":
            query_params["completed"] = str(completed).strip()
        if code is not None and str(code).strip() != "":
            query_params["code"] = str(code).strip()
        if search is not None and str(search).strip() != "":
            query_params["search"] = str(search).strip()
        api_url = metsights_api_url(f"profiles/{pid}/records/")
        try:
            payload = await tracked_metsights_call(
                sync_context,
                api_url=api_url,
                request_payload=query_params or None,
                operation=lambda: self._client.list_profile_records(
                    profile_id=pid,
                    completed=completed,
                    code=code,
                    search=search,
                ),
            )
            if payload is None:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights request failed",
                )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="PROFILE_NOT_FOUND",
                    message="Metsights profile not found",
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

    async def get_record_detail(self, *, record_id: str) -> Any:
        """GET /records/:record_id/ — full record including nested questionnaire payloads."""

        rid = (record_id or "").strip()
        if not rid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights record id is missing")
        self._require_api_key()
        try:
            payload = await self._client.get_record_detail(record_id=rid)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="RECORD_NOT_FOUND",
                    message="Metsights record not found",
                ) from exc
            if status_code == 403:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights authorization failed",
                ) from exc
            if status_code == 429:
                raise AppError(
                    status_code=429,
                    error_code="METSIGHTS_RATE_LIMITED",
                    message="Metsights rate limit exceeded",
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

    async def get_record_subresource_or_none(self, *, record_id: str, resource: str) -> dict[str, Any] | None:
        """GET /records/:id/:resource/ — returns ``data`` object, or ``None`` if missing or unreadable.

        Some Metsights deployments return ``404`` (no payload) or ``405`` (GET not exposed on that path);
        both are treated as no data so import can continue with other resources.

        Known 405-on-GET resources are skipped without calling the API — callers should fall back to
        nested fields on ``GET /records/:id/`` (see sync_service ``_RESOURCE_TO_DETAIL_FIELD``).
        """

        rid = (record_id or "").strip()
        res = (resource or "").strip().strip("/")
        if not rid or not res:
            return None
        # Docs claim GET works for these, but production MetSights returns 405 Method Not Allowed.
        # Avoid noisy failed calls; import paths already fall back to record detail.
        if res in {"vitals", "fitness-parameters", "advanced-blood-parameters"}:
            return None
        self._require_api_key()
        try:
            payload = await self._client.get_record_resource(record_id=rid, resource=res)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (404, 405):
                return None
            if exc.response.status_code == 403:
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
        data = envelope.data
        return data if isinstance(data, dict) else None

    async def options_record_subresource(self, *, record_id: str, resource: str) -> dict[str, Any]:
        """OPTIONS /records/:id/:resource/ — raw JSON envelope; empty dict if unavailable."""

        rid = (record_id or "").strip()
        res = (resource or "").strip().strip("/")
        if not rid or not res:
            return {}
        if not settings.METSIGHTS_API_KEY:
            return {}
        try:
            payload = await self._client.options_record_resource(record_id=rid, resource=res)
        except httpx.HTTPStatusError:
            return {}
        except httpx.HTTPError:
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload

    async def get_fetch_collections(self, *, record_id: str) -> dict[str, Any]:
        """GET /records/{record_id}/fetch-collections/ — returns envelope ``data``."""
        rid = (record_id or "").strip()
        if not rid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights record id is missing")
        self._require_api_key()
        try:
            payload = await self._client.get_record_fetch_collections(record_id=rid)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=422,
                    error_code="BLOOD_SAMPLE_NOT_COLLECTED",
                    message="Sample collection does not exist for this record",
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
        data = envelope.data
        return data if isinstance(data, dict) else {}

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

    async def get_report(self, *, record_id: str, assessment_type_code: str | None) -> Any:
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
            payload = await self._client.get_report(
                record_id=normalized_record_id,
                assessment_type_code=assessment_type_code,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="REPORT_NOT_FOUND",
                    message="Report not found for this record",
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

    async def is_bioai_report_generated(
        self,
        *,
        record_id: str,
        assessment_type_code: str | None,
    ) -> bool:
        """Return True when Metsights GET report returns 200; False on 404."""
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
            await self._client.get_report(
                record_id=normalized_record_id,
                assessment_type_code=assessment_type_code,
            )
            return True
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                return False
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

    async def get_report_pdf(self, *, record_id: str, assessment_type_code: str | None) -> Any:
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
            payload = await self._client.get_report_pdf(
                record_id=normalized_record_id,
                assessment_type_code=assessment_type_code,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="REPORT_NOT_FOUND",
                    message="Report PDF not found for this record",
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

    async def patch_record_subresource(
        self,
        *,
        record_id: str,
        resource: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """PATCH /records/:record_id/:resource/ — partial update (questionnaire sections)."""

        rid = (record_id or "").strip()
        res = (resource or "").strip().strip("/")
        if not rid or not res:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights record id is missing")
        self._require_api_key()
        try:
            payload = await self._client.patch_record_resource(record_id=rid, resource=res, data=body)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                raise AppError(
                    status_code=404,
                    error_code="RECORD_NOT_FOUND",
                    message="Metsights record not found",
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
        data = envelope.data
        return data if isinstance(data, dict) else {}

    def _raise_metsights_record_http(self, exc: httpx.HTTPStatusError) -> None:
        status_code = exc.response.status_code
        if status_code == 404:
            raise AppError(
                status_code=404,
                error_code="RECORD_NOT_FOUND",
                message="Metsights record not found",
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

    async def upsert_record_subresource(
        self,
        *,
        record_id: str,
        resource: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """POST when the sub-resource has not been created yet; otherwise PATCH.

        Metsights returns 404 on PATCH if no row exists yet for ``physical-measurement``, ``vitals``,
        ``diet-lifestyle-parameters``, or ``fitness-parameters`` (create with POST first).

        On a 400 with field-level validation errors for *optional* / non-critical fields,
        those fields are stripped and the request is retried. Critical measurement fields
        (height/weight/waist/hip/body_fat) are never stripped — their rejection is returned
        as ``METSIGHTS_VALIDATION_ERROR`` so the client can fix the answer.
        """

        rid = (record_id or "").strip()
        res = (resource or "").strip().strip("/")
        if not rid or not res:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights record id is missing")
        self._require_api_key()

        existing = await self.get_record_subresource_or_none(record_id=rid, resource=res)
        critical_fields = frozenset(
            {
                "height",
                "weight",
                "waist_circumference",
                "body_fat",
            }
        )

        def _parse_payload(payload: dict[str, Any]) -> dict[str, Any]:
            envelope = MetsightsEnvelope.model_validate(payload)
            data = envelope.data
            return data if isinstance(data, dict) else {}

        def _extract_bad_fields(exc: httpx.HTTPStatusError) -> set[str]:
            """Parse a Metsights 400 response to find which fields failed validation."""
            try:
                err = exc.response.json()
            except Exception:
                return set()
            detail = err.get("detail") if isinstance(err, dict) else None
            if isinstance(detail, dict):
                return {str(k) for k in detail.keys()}
            return set()

        def _is_already_exists_conflict(exc: httpx.HTTPStatusError) -> bool:
            """True when Metsights signals the sub-resource row already exists."""
            if exc.response.status_code == 409:
                return True
            if exc.response.status_code != 400:
                return False
            try:
                err = exc.response.json()
            except Exception:
                err = None
            detail = err.get("detail") if isinstance(err, dict) else None
            text = str(detail if detail is not None else exc.response.text).lower()
            return "already exist" in text

        def _raise_validation(exc: httpx.HTTPStatusError, *, payload: dict[str, Any] | None = None) -> None:
            try:
                error_body = exc.response.json()
            except Exception:
                error_body = exc.response.text
            detail_text = ""
            if isinstance(error_body, dict):
                detail_text = str(error_body.get("detail") or "")
            else:
                detail_text = str(error_body)
            # Metsights requires the parent blood-parameters row before advanced.
            if (
                res == "advanced-blood-parameters"
                and "blood parameter does not exist" in detail_text.lower()
            ):
                raise AppError(
                    status_code=422,
                    error_code="METSIGHTS_VALIDATION_ERROR",
                    message=(
                        "Metsights rejected data for advanced-blood-parameters: "
                        "parent blood-parameters does not exist for this record. "
                        "Push blood-parameters first, then retry advanced."
                    ),
                ) from exc
            hint = ""
            if isinstance(payload, dict):
                bad = _extract_bad_fields(exc)
                samples = []
                for field in sorted(bad):
                    if field.endswith("_unit"):
                        continue
                    if field in payload:
                        samples.append(f"{field}={payload.get(field)!r} (unit={payload.get(f'{field}_unit')!r})")
                if samples:
                    hint = " Rejected values: " + "; ".join(samples) + "."
            raise AppError(
                status_code=422,
                error_code="METSIGHTS_VALIDATION_ERROR",
                message=f"Metsights rejected data for {res}: {error_body}.{hint}",
            ) from exc

        async def _send_once(data: dict[str, Any]) -> dict[str, Any]:
            if existing is None:
                try:
                    return await self._client.post_record_resource(record_id=rid, resource=res, data=data)
                except httpx.HTTPStatusError as exc_post:
                    # Metsights often returns 400 "… already exists …" instead of 409 when GET
                    # is 404/405 but the row was created earlier.
                    if exc_post.response.status_code == 409 or _is_already_exists_conflict(exc_post):
                        logger.info(
                            "Metsights POST /records/%s/%s/ reported existing resource — falling back to PATCH",
                            rid,
                            res,
                        )
                        return await self._client.patch_record_resource(record_id=rid, resource=res, data=data)
                    raise
            return await self._client.patch_record_resource(record_id=rid, resource=res, data=data)

        async def _try_send(data: dict[str, Any]) -> dict[str, Any]:
            current = dict(data)
            stripped: set[str] = set()
            # Bound retries so a pathological Metsights response cannot loop forever.
            for _attempt in range(8):
                try:
                    return await _send_once(current)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 400:
                        self._raise_metsights_record_http(exc)
                    bad = _extract_bad_fields(exc) - stripped
                    # Never strip core anthropometry — surface the real validation error.
                    if bad & critical_fields:
                        _raise_validation(exc, payload=current)
                    for field in list(bad):
                        bad.add(f"{field}_unit")
                    bad = {f for f in bad if f in current and f != "is_complete"}
                    if not bad:
                        _raise_validation(exc, payload=current)
                    logger.warning(
                        "Metsights %s /records/%s/%s/ rejected fields %s — retrying without them.",
                        "POST" if existing is None else "PATCH",
                        rid,
                        res,
                        bad,
                    )
                    stripped |= bad
                    current = {k: v for k, v in current.items() if k not in bad}
                    if not any(k != "is_complete" for k in current):
                        _raise_validation(exc, payload=data)
            raise AppError(
                status_code=422,
                error_code="METSIGHTS_VALIDATION_ERROR",
                message=f"Metsights rejected data for {res} after repeated field stripping",
            )

        try:
            payload = await _try_send(body)
            return _parse_payload(payload)
        except AppError:
            raise
        except httpx.HTTPStatusError as exc:
            self._raise_metsights_record_http(exc)
        except httpx.HTTPError as exc:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights request failed",
            ) from exc

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
        sync_context: MetsightsSyncContext | None = None,
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

        existing_id = await self._find_best_existing_profile_id(
            first_name=safe_first,
            last_name=safe_last,
            phone=safe_phone,
            email=safe_email,
            sync_context=sync_context,
        )
        if existing_id:
            return existing_id

        payload: dict[str, Any] = {
            "first_name": safe_first,
            "last_name": safe_last,
            "phone": safe_phone,
            "gender": int(safe_gender),
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
            created = await tracked_metsights_call(
                sync_context,
                api_url=metsights_api_url("profiles/"),
                request_payload=dict(payload),
                operation=lambda: self._client.create_profile(data=payload),
            )
            if created is None:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights request failed",
                )
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
            if status_code in (401, 403):
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights authorization failed",
                ) from exc
            if status_code == 400:
                existing_id = await self._find_best_existing_profile_id(
                    first_name=safe_first,
                    last_name=safe_last,
                    phone=safe_phone,
                    email=safe_email,
                    sync_context=sync_context,
                )
                if existing_id:
                    return existing_id

                # If email is causing uniqueness conflicts, retry creation without email.
                if safe_email:
                    payload_without_email = dict(payload)
                    payload_without_email.pop("email", None)
                    try:
                        created_wo_email = await tracked_metsights_call(
                            sync_context,
                            api_url=metsights_api_url("profiles/"),
                            request_payload=dict(payload_without_email),
                            operation=lambda: self._client.create_profile(data=payload_without_email),
                            reraise=False,
                        )
                        if created_wo_email is None:
                            raise ValueError("profile creation without email failed")
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

    async def _find_best_existing_profile_id(
        self,
        *,
        first_name: str,
        last_name: str,
        phone: str,
        email: str | None,
        sync_context: MetsightsSyncContext | None = None,
    ) -> str | None:
        """Reuse an existing Metsights profile when phone/email already exist."""

        search_terms: list[str] = []
        phone_digits = _normalize_phone_digits(phone)
        if phone_digits:
            search_terms.append(phone_digits[-10:] if len(phone_digits) >= 10 else phone_digits)
            search_terms.append(phone.strip())
        if email:
            search_terms.append(email.strip())

        seen_ids: set[str] = set()
        candidates: list[dict[str, Any]] = []
        for term in search_terms:
            if not term:
                continue
            listed = await tracked_metsights_call(
                sync_context,
                api_url=metsights_api_url("profiles/"),
                request_payload={"search": term},
                operation=lambda term=term: self._client.list_profiles(search=term),
                reraise=False,
            )
            if listed is None:
                continue
            try:
                envelope = MetsightsEnvelope.model_validate(listed)
            except Exception:
                continue
            rows = envelope.data if isinstance(envelope.data, list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                profile_id = str(row.get("id") or "").strip()
                if not profile_id or profile_id in seen_ids:
                    continue
                phone_ok = _phones_equivalent(str(row.get("phone") or ""), phone)
                email_ok = (
                    bool(email)
                    and str(row.get("email") or "").strip().lower() == email.strip().lower()
                )
                if not phone_ok and not email_ok:
                    continue
                seen_ids.add(profile_id)
                candidates.append(row)

        if not candidates:
            return None

        for row in candidates:
            if _profile_name_matches(row, first_name=first_name, last_name=last_name):
                return str(row.get("id") or "").strip() or None

        # Prefer the oldest profile (original participant record) when names differ slightly.
        def _created_key(row: dict[str, Any]) -> str:
            return str(row.get("created_at") or "")

        oldest = min(candidates, key=_created_key)
        return str(oldest.get("id") or "").strip() or None

    async def create_profile_for_engagement(
        self,
        *,
        engagement_id: str,
        first_name: str,
        last_name: str,
        phone: str,
        email: str | None,
        gender: str,
        date_of_birth: str | None,
        age: int | None,
        sync_context: MetsightsSyncContext | None = None,
    ) -> str:
        self._require_api_key()
        safe_engagement_id = (engagement_id or "").strip()
        safe_first = (first_name or "").strip()
        safe_last = (last_name or "").strip()
        safe_phone = (phone or "").strip()
        safe_gender = (gender or "").strip()
        safe_email = (email or "").strip() if email is not None else None
        safe_dob = (date_of_birth or "").strip() if date_of_birth is not None else None

        if not safe_engagement_id:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights engagement id is missing")
        if not safe_first or not safe_last or not safe_phone or not safe_gender:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Missing required profile fields")

        payload: dict[str, Any] = {
            "first_name": safe_first,
            "last_name": safe_last,
            "phone": safe_phone,
            "gender": int(safe_gender),
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
            created = await tracked_metsights_call(
                sync_context,
                api_url=metsights_api_url(f"engagements/{safe_engagement_id}/register/"),
                request_payload=dict(payload),
                operation=lambda: self._client.create_profile_for_engagement(
                    engagement_id=safe_engagement_id,
                    data=payload,
                ),
            )
            if created is None:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights request failed",
                )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (401, 403):
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

    async def create_record_for_profile(
        self,
        *,
        profile_id: str,
        assessment_type_code: str,
        sync_context: MetsightsSyncContext | None = None,
    ) -> str:
        safe_profile_id = (profile_id or "").strip()
        safe_type_code = (assessment_type_code or "").strip()
        if not safe_profile_id or not safe_type_code:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Missing Metsights record inputs")
        if not settings.METSIGHTS_API_KEY:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        record_payload = {"assessment_type": safe_type_code}
        try:
            payload = await tracked_metsights_call(
                sync_context,
                api_url=metsights_api_url(f"profiles/{safe_profile_id}/records/"),
                request_payload=dict(record_payload),
                operation=lambda: self._client.create_profile_record(
                    profile_id=safe_profile_id,
                    data=record_payload,
                ),
            )
            if payload is None:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Metsights request failed",
                )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 403:
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
        body = envelope.data

        # Some Metsights responses send record in data object, others as first item in data[].
        if isinstance(body, dict):
            record_id = str(body.get("id") or "").strip()
            if record_id:
                return record_id
        if isinstance(body, list) and len(body) > 0 and isinstance(body[0], dict):
            record_id = str(body[0].get("id") or "").strip()
            if record_id:
                return record_id

        raise AppError(
            status_code=503,
            error_code="EXTERNAL_SERVICE_UNAVAILABLE",
            message="Metsights record creation returned no id",
        )
