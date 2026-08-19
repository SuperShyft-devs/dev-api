"""Provider-isolated structured LLM completion boundary."""

from __future__ import annotations

import json
import re
from typing import Any, Protocol, TypeVar

import httpx

from core.config import settings
from modules.qura.schemas import BuiltPrompt, LLMStructuredOutput


T = TypeVar("T", bound=LLMStructuredOutput)

# Anthropic tool-use schema derived once from LLMStructuredOutput.
_TOOL_NAME = "qura_response"
_TOOL_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "answer": {"type": "string", "description": "Natural-language answer grounded in health context."},
        "grounding": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Health-context identifiers (marker/risk codes) supporting the answer.",
        },
        "confidence": {
            "type": "string",
            "enum": ["low", "medium", "high"],
            "description": "Self-assessed answer confidence.",
        },
        "safety_flags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Safety categories triggered, if any.",
        },
        "escalation": {
            "type": "string",
            "enum": ["none", "clinician", "emergency"],
            "description": "Escalation level.",
        },
        "recommendations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "category": {"type": ["string", "null"]},
                },
                "required": ["text"],
            },
            "description": "Must be empty; recommendations are disabled.",
        },
    },
    "required": ["answer", "grounding", "confidence", "safety_flags", "escalation", "recommendations"],
}

# Gemini responseSchema (subset: no $defs, no advanced refs).
_GEMINI_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "OBJECT",
    "properties": {
        "answer": {"type": "STRING"},
        "grounding": {"type": "ARRAY", "items": {"type": "STRING"}},
        "confidence": {"type": "STRING", "enum": ["low", "medium", "high"]},
        "safety_flags": {"type": "ARRAY", "items": {"type": "STRING"}},
        "escalation": {"type": "STRING", "enum": ["none", "clinician", "emergency"]},
        "recommendations": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "text": {"type": "STRING"},
                    "category": {"type": "STRING", "nullable": True},
                },
                "required": ["text"],
            },
        },
    },
    "required": ["answer", "grounding", "confidence", "safety_flags", "escalation", "recommendations"],
}


class LLMClientError(Exception):
    """Safe application-level provider/validation failure."""


class LLMProvider(Protocol):
    async def complete(self, *, prompt: BuiltPrompt, temperature: float) -> object:
        """Return a provider response payload or JSON text."""


def _user_content(prompt: BuiltPrompt) -> str:
    return "\n\n".join(
        (
            "TASK:\n" + prompt.task,
            "HEALTH CONTEXT:\n" + prompt.health_context_json,
            "USER QUESTION:\n" + prompt.user_question,
            "SAFETY RULES:\n" + prompt.safety_rules,
            "OUTPUT FORMAT (return JSON via the qura_response tool):\n" + prompt.output_format,
            "LANGUAGE:\n" + prompt.language,
        )
    )


class AnthropicProvider:
    """Uses the Anthropic SDK with forced tool-use for guaranteed structured output."""

    def __init__(self, *, api_key: str, model: str, timeout_seconds: int) -> None:
        self._api_key = api_key
        self._model = model
        self._timeout_seconds = timeout_seconds

    async def complete(self, *, prompt: BuiltPrompt, temperature: float) -> object:
        if not self._api_key or not self._model:
            raise LLMClientError("Anthropic is not configured")
        try:
            import anthropic
        except ImportError as exc:
            raise LLMClientError("Anthropic is not configured") from exc
        try:
            client = anthropic.AsyncAnthropic(
                api_key=self._api_key,
                timeout=self._timeout_seconds,
            )
            response = await client.messages.create(
                model=self._model,
                max_tokens=1024,
                temperature=temperature,
                system=prompt.system_instructions,
                messages=[{"role": "user", "content": _user_content(prompt)}],
                tools=[
                    {
                        "name": _TOOL_NAME,
                        "description": "Return the structured Qura health-report explanation.",
                        "input_schema": _TOOL_SCHEMA,
                    }
                ],
                tool_choice={"type": "tool", "name": _TOOL_NAME},
            )
        except anthropic.APIError as exc:
            raise LLMClientError("Anthropic completion failed") from exc
        except Exception as exc:
            raise LLMClientError("Anthropic completion failed") from exc
        # With forced tool_choice the first content block is always tool_use.
        for block in response.content:
            if block.type == "tool_use" and block.name == _TOOL_NAME:
                return block.input  # Already a dict; no JSON parsing needed.
        raise LLMClientError("Anthropic returned no tool_use block")


class GeminiProvider:
    """Uses Gemini REST API with responseSchema for structured JSON output."""

    def __init__(self, *, api_key: str, model: str, timeout_seconds: int) -> None:
        self._api_key = api_key
        self._model = model
        self._timeout_seconds = timeout_seconds

    async def complete(self, *, prompt: BuiltPrompt, temperature: float) -> object:
        if not self._api_key or not self._model:
            raise LLMClientError("Gemini is not configured")
        payload = {
            "system_instruction": {"parts": [{"text": prompt.system_instructions}]},
            "contents": [{"role": "user", "parts": [{"text": _user_content(prompt)}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": 2048,
                "responseMimeType": "application/json",
                "responseSchema": _GEMINI_RESPONSE_SCHEMA,
            },
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent",
                    params={"key": self._api_key},
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise LLMClientError("Gemini completion failed") from exc
        # Gemini 3.6 (thinking model) may return multiple parts:
        #   - thought parts:    {"thought": true, "text": "..."}
        #   - signature parts:  {"thoughtSignature": "..."}
        #   - answer parts:     {"text": "..."}
        # Extract the last non-thought text part as the actual answer.
        try:
            parts = data["candidates"][0]["content"]["parts"]
        except (KeyError, IndexError, TypeError):
            raise LLMClientError("Gemini returned no answer content")
        answer_text: str | None = None
        for part in parts:
            if part.get("thought"):
                continue
            if "text" in part:
                answer_text = part["text"]
        if answer_text is None:
            raise LLMClientError("Gemini returned no answer content")
        return answer_text


class LLMClient:
    def __init__(
        self,
        *,
        primary: LLMProvider | None,
        fallback: LLMProvider | None = None,
        temperature: float = 0.0,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._temperature = temperature

    @classmethod
    def from_settings(cls) -> "LLMClient":
        anthropic = AnthropicProvider(
            api_key=settings.QURA_ANTHROPIC_API_KEY,
            model=settings.QURA_ANTHROPIC_MODEL,
            timeout_seconds=settings.QURA_LLM_TIMEOUT_SECONDS,
        )
        gemini = GeminiProvider(
            api_key=settings.QURA_GEMINI_API_KEY,
            model=settings.QURA_GEMINI_MODEL,
            timeout_seconds=settings.QURA_LLM_TIMEOUT_SECONDS,
        )
        if settings.QURA_LLM_PROVIDER == "gemini":
            return cls(primary=gemini, fallback=anthropic, temperature=settings.QURA_LLM_TEMPERATURE)
        return cls(primary=anthropic, fallback=gemini, temperature=settings.QURA_LLM_TEMPERATURE)

    async def complete(self, prompt: BuiltPrompt, response_schema: type[T] = LLMStructuredOutput) -> T:
        failures: list[Exception] = []
        for provider in (self._primary, self._fallback):
            if provider is None:
                continue
            try:
                raw = await provider.complete(prompt=prompt, temperature=self._temperature)
                return self._parse(raw, response_schema)
            except Exception as exc:
                failures.append(exc)
        raise LLMClientError("Qura language service is unavailable") from (failures[-1] if failures else None)

    @staticmethod
    def _parse(raw: object, response_schema: type[T]) -> T:
        if isinstance(raw, str):
            # Strip markdown JSON fences that models occasionally wrap output in.
            stripped = raw.strip()
            if stripped.startswith("```"):
                stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
                stripped = re.sub(r"\s*```$", "", stripped)
            try:
                raw = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise LLMClientError("Model returned malformed structured output") from exc
        if not isinstance(raw, dict):
            raise LLMClientError("Model returned malformed structured output")
        try:
            return response_schema.model_validate(raw)
        except Exception as exc:
            raise LLMClientError("Model returned invalid structured output") from exc
