"""
Application exception definitions and handlers.
"""

import logging
from dataclasses import dataclass
from typing import Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


@dataclass
class AppError(Exception):
    """Application error with safe client response details.

    Must not use ``frozen=True``: Python sets ``__traceback__`` when raising,
    and frozen dataclasses reject that assignment (500 + FrozenInstanceError).
    """

    status_code: int
    error_code: str
    message: str


def _map_status_to_error_code(status_code: int) -> str:
    """Map HTTP status codes to standard error codes."""
    mapping: Dict[int, str] = {
        400: "INVALID_INPUT",
        401: "AUTH_FAILED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        409: "CONFLICT",
        422: "INVALID_STATE",
        429: "RATE_LIMITED",
        500: "INTERNAL_ERROR",
        503: "EXTERNAL_SERVICE_UNAVAILABLE",
    }
    return mapping.get(status_code, "INTERNAL_ERROR")


def _request_label(request: Request) -> str:
    return f"{request.method} {request.url.path}"


def _format_validation_loc(loc: tuple[object, ...] | list[object]) -> str:
    parts: list[str] = []
    for item in loc:
        if item in {"body", "query", "path", "header", "cookie"}:
            continue
        parts.append(str(item))
    return ".".join(parts) if parts else "request"


def _format_validation_error_item(error: dict[str, object]) -> str:
    field = _format_validation_loc(tuple(error.get("loc") or ()))
    msg = str(error.get("msg") or "Invalid value")
    if msg.startswith("Value error, "):
        msg = msg[len("Value error, ") :]
    return f"{field}: {msg}"


def format_validation_error_message(errors: list[dict[str, object]]) -> str:
    if not errors:
        return "Invalid request"
    return "; ".join(_format_validation_error_item(error) for error in errors)


def _log_error(
    *,
    request: Request,
    status_code: int,
    error_type: str,
    error_code: str,
    message: str,
    exc: BaseException | None = None,
) -> None:
    """Print status code and error type details to the terminal."""
    line = (
        f"API error | status={status_code} | type={error_type} | "
        f"code={error_code} | {_request_label(request)} | {message}"
    )
    if status_code >= 500:
        logger.error(line, exc_info=exc)
    elif status_code >= 400:
        logger.warning(line)
    else:
        logger.info(line)


def add_exception_handlers(app: FastAPI) -> None:
    """Register global exception handlers."""

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
        _log_error(
            request=request,
            status_code=exc.status_code,
            error_type="AppError",
            error_code=exc.error_code,
            message=exc.message,
            exc=exc,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error_code": exc.error_code,
                "message": exc.message,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        errors = exc.errors()
        message = format_validation_error_message(errors)
        _log_error(
            request=request,
            status_code=400,
            error_type="RequestValidationError",
            error_code="INVALID_INPUT",
            message=str(errors),
            exc=exc,
        )
        return JSONResponse(
            status_code=400,
            content={
                "error_code": "INVALID_INPUT",
                "message": message,
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        error_code = _map_status_to_error_code(exc.status_code)
        message = exc.detail if isinstance(exc.detail, str) else "Request failed"
        _log_error(
            request=request,
            status_code=exc.status_code,
            error_type="HTTPException",
            error_code=error_code,
            message=message,
            exc=exc,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error_code": error_code,
                "message": message,
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        _log_error(
            request=request,
            status_code=500,
            error_type=type(exc).__name__,
            error_code="INTERNAL_ERROR",
            message=str(exc) or "An unexpected error occurred",
            exc=exc,
        )
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
            },
        )
