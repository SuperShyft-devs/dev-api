"""Standalone Nutrition Intelligence HTTP API.

POST /calculate → calculate_nutrition(payload) → existing engine JSON.

Run locally::

    uvicorn nutrition_main:app --host 0.0.0.0 --port 8000
    python nutrition_main.py
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from nutrition_engine import calculate_nutrition

app = FastAPI(
    title="Metsights Nutrition Score API",
    description="Nutrition Intelligence Engine. POST /calculate accepts the existing questionnaire payload.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/calculate")
def calculate(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return calculate_nutrition(payload)
    except (TypeError, ValueError, KeyError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/")
def health_check() -> dict[str, str]:
    return {
        "status": "success",
        "message": "Nutrition API Running",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
