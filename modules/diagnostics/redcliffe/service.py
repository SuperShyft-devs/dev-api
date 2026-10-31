"""Redcliffe Labs serviceability orchestration with sync logging."""

from __future__ import annotations

import logging
from typing import Any

from modules.audit.cron_sync_logging import sanitize_response_payload
from modules.diagnostics.redcliffe import client as redcliffe_client
from modules.diagnostics.redcliffe.client import RedcliffeApiError
from modules.diagnostics.redcliffe.sync_log import (
    finalize_redcliffe_sync_log_isolated,
    persist_redcliffe_sync_log_isolated,
)

logger = logging.getLogger(__name__)


def _sanitize(body: Any) -> dict[str, Any] | None:
    return sanitize_response_payload(body)


async def _tracked_call(
    *,
    api_url: str,
    request_payload: dict,
    call,
) -> dict[str, Any]:
    sync_log_id = await persist_redcliffe_sync_log_isolated(
        engagement_id=None,
        user_id=None,
        api_url=api_url,
        request_payload=request_payload,
        status="pending",
    )
    try:
        result = await call()
        await finalize_redcliffe_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="success",
            response_payload=_sanitize(result),
        )
        return result
    except RedcliffeApiError as exc:
        await finalize_redcliffe_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="failed",
            response_payload=_sanitize(exc.body),
            error_message=str(exc),
        )
        raise
    except Exception as exc:
        logger.exception("Redcliffe API call failed")
        await finalize_redcliffe_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="failed",
            error_message=str(exc),
        )
        raise


async def search_area(*, place_query: str) -> dict[str, Any]:
    query = place_query.strip()
    api_url = redcliffe_client.search_area_api_url()
    req = {"place_query": query}
    return await _tracked_call(
        api_url=api_url,
        request_payload=req,
        call=lambda: redcliffe_client.search_area(place_query=query),
    )


async def get_coordinates(*, eloc: str) -> dict[str, Any]:
    code = eloc.strip()
    api_url = redcliffe_client.coordinates_api_url()
    req = {"eloc": code}
    return await _tracked_call(
        api_url=api_url,
        request_payload=req,
        call=lambda: redcliffe_client.get_coordinates(eloc=code),
    )


async def check_location_serviceable(*, latitude: float, longitude: float) -> dict[str, Any]:
    api_url = redcliffe_client.serviceability_api_url()
    req = {"latitude": latitude, "longitude": longitude}
    return await _tracked_call(
        api_url=api_url,
        request_payload=req,
        call=lambda: redcliffe_client.check_location_serviceable(
            latitude=latitude,
            longitude=longitude,
        ),
    )
