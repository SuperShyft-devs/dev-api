"""
Tests for global exception handling.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pytest

from core.exceptions import AppError, add_exception_handlers


class _Payload(BaseModel):
    name: str


def _create_test_app() -> FastAPI:
    app = FastAPI()
    add_exception_handlers(app)
    return app


async def _get_async_client(app: FastAPI):
    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    return AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.asyncio
async def test_app_error_handler_returns_standard_format():
    """AppError responses should be standardized and safe."""
    app = _create_test_app()

    @app.get("/test/app-error")
    async def _app_error_route():
        raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

    async with await _get_async_client(app) as client:
        response = await client.get("/test/app-error")

    assert response.status_code == 404
    assert response.json() == {
        "error_code": "USER_NOT_FOUND",
        "message": "User does not exist",
    }


@pytest.mark.asyncio
async def test_validation_error_returns_standard_format():
    """Validation errors should return standard error format."""
    app = _create_test_app()

    @app.post("/test/validation")
    async def _validation_route(payload: _Payload):
        return payload

    async with await _get_async_client(app) as client:
        response = await client.post("/test/validation", json={"name": 123})

    assert response.status_code == 400
    assert response.json() == {
        "error_code": "INVALID_INPUT",
        "message": "Invalid request",
    }


@pytest.mark.asyncio
async def test_http_exception_returns_standard_format():
    """HTTP exceptions should return standard error format."""
    app = _create_test_app()

    @app.get("/test/http-exception")
    async def _http_exception_route():
        raise HTTPException(status_code=503, detail="Service unavailable")

    async with await _get_async_client(app) as client:
        response = await client.get("/test/http-exception")

    assert response.status_code == 503
    assert response.json() == {
        "error_code": "EXTERNAL_SERVICE_UNAVAILABLE",
        "message": "Service unavailable",
    }


@pytest.mark.asyncio
async def test_unhandled_exception_returns_standard_format():
    """Unhandled exceptions should return standard error format."""
    app = _create_test_app()

    @app.get("/test/unhandled")
    async def _unhandled_route():
        raise ValueError("failure")

    async with await _get_async_client(app) as client:
        response = await client.get("/test/unhandled")

    assert response.status_code == 500
    assert response.json() == {
        "error_code": "INTERNAL_ERROR",
        "message": "An unexpected error occurred",
    }
