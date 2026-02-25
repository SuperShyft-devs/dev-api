"""
Application exception definitions and handlers.
"""

from dataclasses import dataclass
from typing import Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException


@dataclass(frozen=True)
class AppError(Exception):
    """Application error with safe client response details."""

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


def add_exception_handlers(app: FastAPI) -> None:
    """Register global exception handlers."""

    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error_code": exc.error_code,
                "message": exc.message,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _: Request,
        __: RequestValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "error_code": "INVALID_INPUT",
                "message": "Invalid request",
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        _: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        error_code = _map_status_to_error_code(exc.status_code)
        message = exc.detail if isinstance(exc.detail, str) else "Request failed"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error_code": error_code,
                "message": message,
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, __: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
            },
        )
