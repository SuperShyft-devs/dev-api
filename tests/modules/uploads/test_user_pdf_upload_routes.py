"""Tests for POST /uploads/bio-ai/pdf and POST /uploads/blood-parameters/pdf."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from core.config import settings
from core.exceptions import add_exception_handlers
from core.logging import request_id_middleware
from core.security import create_jwt_token
from db.session import get_db
from modules.uploads.user_pdf_router import router as user_pdf_upload_router

_MIN_PDF = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def pdf_upload_app(test_db_session):
    app = FastAPI()
    add_exception_handlers(app)
    app.middleware("http")(request_id_middleware)
    app.include_router(user_pdf_upload_router)

    async def _get_test_db():
        yield test_db_session

    app.dependency_overrides[get_db] = _get_test_db
    return app


@pytest_asyncio.fixture
async def pdf_upload_client(pdf_upload_app: FastAPI):
    transport = ASGITransport(app=pdf_upload_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture
def pdf_media_root(tmp_path, monkeypatch):
    root = tmp_path / "media"
    root.mkdir()
    monkeypatch.setattr(settings, "MEDIA_ROOT", str(root))
    monkeypatch.setattr(settings, "MEDIA_BASE_URL", "http://testserver/media")
    monkeypatch.setattr(settings, "USER_PDF_UPLOAD_MAX_MB", 5)
    return root


@pytest.mark.asyncio
async def test_upload_bio_ai_pdf_success(pdf_upload_client, pdf_media_root):
    files = {"file": ("report.pdf", _MIN_PDF, "application/pdf")}
    r = await pdf_upload_client.post("/uploads/bio-ai/pdf", files=files, headers=_auth_header(1))
    assert r.status_code == 200
    data = r.json()["data"]
    assert data["url"].startswith("http://testserver/media/bio-ai/")
    assert data["url"].endswith(".pdf")
    name = data["url"].rsplit("/", 1)[-1]
    assert (Path(settings.MEDIA_ROOT) / "bio-ai" / name).is_file()


@pytest.mark.asyncio
async def test_upload_blood_parameters_pdf_success(pdf_upload_client, pdf_media_root):
    files = {"file": ("blood.pdf", _MIN_PDF, "application/pdf")}
    r = await pdf_upload_client.post("/uploads/blood-parameters/pdf", files=files, headers=_auth_header(1))
    assert r.status_code == 200
    data = r.json()["data"]
    assert "blood-parameters" in data["url"]


@pytest.mark.asyncio
async def test_upload_pdf_requires_auth(pdf_upload_client, pdf_media_root):
    files = {"file": ("report.pdf", _MIN_PDF, "application/pdf")}
    r = await pdf_upload_client.post("/uploads/bio-ai/pdf", files=files)
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_upload_pdf_rejects_non_pdf_content_type(pdf_upload_client, pdf_media_root):
    files = {"file": ("x.png", _MIN_PDF, "image/png")}
    r = await pdf_upload_client.post("/uploads/bio-ai/pdf", files=files, headers=_auth_header(1))
    assert r.status_code == 400
    assert r.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_upload_pdf_rejects_mismatched_magic_bytes(pdf_upload_client, pdf_media_root):
    files = {"file": ("fake.pdf", b"\xff\xd8\xff\xe0not a pdf", "application/pdf")}
    r = await pdf_upload_client.post("/uploads/bio-ai/pdf", files=files, headers=_auth_header(1))
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_upload_pdf_rejects_oversize(pdf_upload_client, pdf_media_root, monkeypatch):
    monkeypatch.setattr(settings, "USER_PDF_UPLOAD_MAX_MB", 0)
    files = {"file": ("big.pdf", _MIN_PDF, "application/pdf")}
    r = await pdf_upload_client.post("/uploads/bio-ai/pdf", files=files, headers=_auth_header(1))
    assert r.status_code == 400
    assert r.json()["error_code"] == "INVALID_INPUT"
    assert "large" in r.json()["message"].lower()
