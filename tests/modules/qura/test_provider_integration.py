"""Integration-style tests for AnthropicProvider and GeminiProvider.

These tests mock the HTTP / SDK boundaries (no live API calls) to validate
the full provider → LLMClient._parse → LLMStructuredOutput contract.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from modules.qura.llm_client import (
    AnthropicProvider,
    GeminiProvider,
    LLMClient,
    LLMClientError,
    _TOOL_NAME,
)
from modules.qura.prompt_builder import PromptBuilder
from modules.qura.schemas import (
    ContextMeta,
    HealthContext,
    IntentResult,
    LLMStructuredOutput,
    Marker,
    QueryPlan,
)


def _ldl_context() -> HealthContext:
    return HealthContext(
        selected_markers=[
            Marker(code="ldl", name="LDL Cholesterol", value=162, unit="mg/dL", ref_low=0, ref_high=100),
        ],
        metadata=ContextMeta(source="latest_health_report"),
    )


def _prompt() -> "BuiltPrompt":
    from modules.qura.schemas import BuiltPrompt
    return PromptBuilder().build(
        message="Explain my LDL",
        intent=IntentResult(primary="explain_marker"),
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
        context=_ldl_context(),
        language="english",
    )


def _valid_dict() -> dict:
    return {
        "answer": "Your LDL is 162 mg/dL, which is above the reference range of 0–100.",
        "grounding": ["ldl"],
        "confidence": "high",
        "safety_flags": [],
        "escalation": "none",
        "recommendations": [],
    }


# ---------------------------------------------------------------------------
# Anthropic Provider tests (SDK tool-use boundary)
# ---------------------------------------------------------------------------

class _FakeToolUseBlock:
    """Mimics an anthropic.types.ToolUseBlock."""
    type = "tool_use"
    name = _TOOL_NAME

    def __init__(self, data: dict):
        self.input = data


class _FakeTextBlock:
    """Mimics an anthropic.types.TextBlock."""
    type = "text"
    text = "Here is the explanation."


class _FakeResponse:
    """Mimics an anthropic.types.Message."""
    def __init__(self, content: list):
        self.content = content


@pytest.mark.asyncio
async def test_anthropic_provider_extracts_tool_use_block():
    """Tool-use response → dict → LLMStructuredOutput."""
    provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-20250514", timeout_seconds=10)
    fake_response = _FakeResponse([_FakeToolUseBlock(_valid_dict())])

    with patch("anthropic.AsyncAnthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages = MagicMock()
        instance.messages.create = AsyncMock(return_value=fake_response)

        result = await provider.complete(prompt=_prompt(), temperature=0.0)

    assert isinstance(result, dict)
    output = LLMStructuredOutput.model_validate(result)
    assert output.answer.startswith("Your LDL is 162")
    assert output.grounding == ["ldl"]
    assert output.confidence == "high"


@pytest.mark.asyncio
async def test_anthropic_provider_handles_text_only_response():
    """When model returns only text blocks (no tool_use), raise LLMClientError."""
    provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-20250514", timeout_seconds=10)
    fake_response = _FakeResponse([_FakeTextBlock()])

    with patch("anthropic.AsyncAnthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages = MagicMock()
        instance.messages.create = AsyncMock(return_value=fake_response)

        with pytest.raises(LLMClientError, match="no tool_use block"):
            await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_anthropic_provider_wraps_api_errors():
    """SDK APIError is wrapped as LLMClientError with no private details leaked."""
    import anthropic as anthropic_module

    provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-20250514", timeout_seconds=10)

    with patch("anthropic.AsyncAnthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages = MagicMock()
        instance.messages.create = AsyncMock(
            side_effect=anthropic_module.APIConnectionError(request=MagicMock())
        )

        with pytest.raises(LLMClientError, match="Anthropic completion failed"):
            await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_anthropic_provider_not_configured():
    """Missing API key raises immediately without network call."""
    provider = AnthropicProvider(api_key="", model="", timeout_seconds=10)
    with pytest.raises(LLMClientError, match="not configured"):
        await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_anthropic_tool_use_with_preceding_text_block():
    """Model may return a text block before the tool_use block; we extract the right one."""
    provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-20250514", timeout_seconds=10)
    fake_response = _FakeResponse([_FakeTextBlock(), _FakeToolUseBlock(_valid_dict())])

    with patch("anthropic.AsyncAnthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages = MagicMock()
        instance.messages.create = AsyncMock(return_value=fake_response)

        result = await provider.complete(prompt=_prompt(), temperature=0.0)

    assert result == _valid_dict()


# ---------------------------------------------------------------------------
# Gemini Provider tests (httpx REST boundary)
# ---------------------------------------------------------------------------

def _gemini_success_body() -> dict:
    return {
        "candidates": [
            {
                "content": {
                    "parts": [{"text": json.dumps(_valid_dict())}],
                    "role": "model",
                },
                "finishReason": "STOP",
            }
        ]
    }


@pytest.mark.asyncio
async def test_gemini_provider_returns_valid_structured_json():
    """Gemini returns JSON text → LLMClient._parse validates it."""
    provider = GeminiProvider(api_key="test-key", model="gemini-2.0-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = _gemini_success_body()

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = await provider.complete(prompt=_prompt(), temperature=0.0)

    parsed = LLMStructuredOutput.model_validate(json.loads(result))
    assert parsed.grounding == ["ldl"]


@pytest.mark.asyncio
async def test_gemini_provider_sends_response_schema():
    """Verify that the Gemini request includes responseSchema in generationConfig."""
    provider = GeminiProvider(api_key="test-key", model="gemini-2.0-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = _gemini_success_body()

    captured_kwargs = {}

    async def capture_post(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return mock_response

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = capture_post
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        await provider.complete(prompt=_prompt(), temperature=0.0)

    gen_config = captured_kwargs["json"]["generationConfig"]
    assert "responseSchema" in gen_config
    assert gen_config["responseMimeType"] == "application/json"
    assert gen_config["responseSchema"]["type"] == "OBJECT"


@pytest.mark.asyncio
async def test_gemini_provider_not_configured():
    provider = GeminiProvider(api_key="", model="", timeout_seconds=10)
    with pytest.raises(LLMClientError, match="not configured"):
        await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_gemini_provider_wraps_http_errors():
    """httpx errors are wrapped as LLMClientError."""
    import httpx

    provider = GeminiProvider(api_key="test-key", model="gemini-2.0-flash", timeout_seconds=10)

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(side_effect=httpx.ConnectTimeout("timeout"))
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        with pytest.raises(LLMClientError, match="Gemini completion failed"):
            await provider.complete(prompt=_prompt(), temperature=0.0)


# ---------------------------------------------------------------------------
# Gemini 3.6 thinking-model compatibility tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gemini_provider_handles_empty_content():
    """Gemini 3.6 may return empty content when thinking consumes all tokens."""
    provider = GeminiProvider(api_key="test-key", model="gemini-3.6-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "candidates": [{"content": {}, "finishReason": "STOP"}],
    }

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        with pytest.raises(LLMClientError, match="no answer content"):
            await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_gemini_provider_skips_thinking_parts():
    """Gemini 3.6 returns thought parts before the answer; only the answer should be extracted."""
    provider = GeminiProvider(api_key="test-key", model="gemini-3.6-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "candidates": [{
            "content": {
                "parts": [
                    {"thought": True, "text": "Let me reason about this LDL value..."},
                    {"text": json.dumps(_valid_dict())},
                ],
                "role": "model",
            },
            "finishReason": "STOP",
        }],
    }

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = await provider.complete(prompt=_prompt(), temperature=0.0)

    # Must be the JSON answer, not the thinking text
    parsed = LLMStructuredOutput.model_validate(json.loads(result))
    assert parsed.answer.startswith("Your LDL is 162")
    assert "reason about" not in result


@pytest.mark.asyncio
async def test_gemini_provider_handles_thought_signature_parts():
    """Gemini 3.6 may include thoughtSignature parts alongside the answer."""
    provider = GeminiProvider(api_key="test-key", model="gemini-3.6-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "candidates": [{
            "content": {
                "parts": [
                    {"thought": True, "text": "internal reasoning"},
                    {"thoughtSignature": "encrypted-sig-data"},
                    {"text": json.dumps(_valid_dict())},
                ],
                "role": "model",
            },
            "finishReason": "STOP",
        }],
    }

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = await provider.complete(prompt=_prompt(), temperature=0.0)

    parsed = LLMStructuredOutput.model_validate(json.loads(result))
    assert parsed.grounding == ["ldl"]


@pytest.mark.asyncio
async def test_gemini_provider_handles_thinking_only_response():
    """When all parts are thought parts with no answer, raise LLMClientError."""
    provider = GeminiProvider(api_key="test-key", model="gemini-3.6-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "candidates": [{
            "content": {
                "parts": [
                    {"thought": True, "text": "Still thinking..."},
                    {"thought": True, "text": "More reasoning..."},
                ],
                "role": "model",
            },
            "finishReason": "STOP",
        }],
    }

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        with pytest.raises(LLMClientError, match="no answer content"):
            await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_gemini_provider_handles_timeout():
    """httpx.ReadTimeout is wrapped as LLMClientError."""
    import httpx

    provider = GeminiProvider(api_key="test-key", model="gemini-3.6-flash", timeout_seconds=10)

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = AsyncMock(side_effect=httpx.ReadTimeout("read timed out"))
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        with pytest.raises(LLMClientError, match="Gemini completion failed"):
            await provider.complete(prompt=_prompt(), temperature=0.0)


@pytest.mark.asyncio
async def test_gemini_provider_passes_max_output_tokens():
    """Verify that maxOutputTokens is included in the Gemini request payload."""
    provider = GeminiProvider(api_key="test-key", model="gemini-3.6-flash", timeout_seconds=10)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = _gemini_success_body()

    captured_kwargs = {}

    async def capture_post(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return mock_response

    with patch("httpx.AsyncClient") as MockClient:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = capture_post
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = mock_client_instance

        await provider.complete(prompt=_prompt(), temperature=0.0)

    gen_config = captured_kwargs["json"]["generationConfig"]
    assert gen_config["maxOutputTokens"] == 2048


# ---------------------------------------------------------------------------
# LLMClient._parse resilience tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_strips_markdown_json_fences():
    """Models sometimes wrap JSON in ```json ... ``` — _parse should handle it."""
    fenced = '```json\n' + json.dumps(_valid_dict()) + '\n```'

    class FenceProvider:
        async def complete(self, *, prompt, temperature):
            return fenced

    output = await LLMClient(primary=FenceProvider()).complete(_prompt())
    assert output.answer.startswith("Your LDL is 162")


@pytest.mark.asyncio
async def test_parse_handles_plain_json_string():
    """Plain JSON string (no fences) still works."""

    class PlainProvider:
        async def complete(self, *, prompt, temperature):
            return json.dumps(_valid_dict())

    output = await LLMClient(primary=PlainProvider()).complete(_prompt())
    assert output.grounding == ["ldl"]


@pytest.mark.asyncio
async def test_parse_handles_dict_directly():
    """When provider returns a dict (Anthropic tool_use), no JSON parsing needed."""

    class DictProvider:
        async def complete(self, *, prompt, temperature):
            return _valid_dict()

    output = await LLMClient(primary=DictProvider()).complete(_prompt())
    assert output.confidence == "high"


# ---------------------------------------------------------------------------
# End-to-end LLMClient with mocked provider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_llm_client_full_pipeline_anthropic_style():
    """Simulates the full Anthropic pipeline: tool_use dict → _parse → validated output."""
    provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-20250514", timeout_seconds=10)
    client = LLMClient(primary=provider)

    fake_response = _FakeResponse([_FakeToolUseBlock(_valid_dict())])

    with patch("anthropic.AsyncAnthropic") as MockSDK:
        instance = MockSDK.return_value
        instance.messages = MagicMock()
        instance.messages.create = AsyncMock(return_value=fake_response)

        output = await client.complete(_prompt())

    assert isinstance(output, LLMStructuredOutput)
    assert output.answer.startswith("Your LDL is 162")
    assert output.escalation == "none"
    assert output.recommendations == []


@pytest.mark.asyncio
async def test_llm_client_falls_back_from_anthropic_to_gemini():
    """Anthropic fails → Gemini succeeds via fallback."""
    import anthropic as anthropic_module

    anthropic_provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-20250514", timeout_seconds=10)
    gemini_provider = GeminiProvider(api_key="test-key", model="gemini-2.0-flash", timeout_seconds=10)
    client = LLMClient(primary=anthropic_provider, fallback=gemini_provider)

    mock_gemini_response = MagicMock()
    mock_gemini_response.raise_for_status = MagicMock()
    mock_gemini_response.json.return_value = _gemini_success_body()

    with patch("anthropic.AsyncAnthropic") as MockAnthropicSDK:
        instance = MockAnthropicSDK.return_value
        instance.messages = MagicMock()
        instance.messages.create = AsyncMock(
            side_effect=anthropic_module.APIConnectionError(request=MagicMock())
        )

        with patch("httpx.AsyncClient") as MockHTTPX:
            mock_client_instance = AsyncMock()
            mock_client_instance.post = AsyncMock(return_value=mock_gemini_response)
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockHTTPX.return_value = mock_client_instance

            output = await client.complete(_prompt())

    assert isinstance(output, LLMStructuredOutput)
    assert output.grounding == ["ldl"]
