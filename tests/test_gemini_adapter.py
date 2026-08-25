"""Tests for Google Gemini resilience adapter."""

from flowguard.adapters.gemini_adapter import ResilientGemini
from flowguard.exceptions import TransientHTTPError


class MockGeminiClient:
    class aio:
        class models:
            @staticmethod
            async def generate_content(model: str, contents: str, **kwargs):
                return type("GeminiResponse", (), {"text": f"Gemini reply to {contents}"})()


async def test_gemini_adapter_basic():
    client = MockGeminiClient()
    adapter = ResilientGemini(
        client=client,
        rpm_limit=300.0,
        tpm_limit=60_000.0,
    )

    resp = await adapter.generate_content(
        model="gemini-2.5-flash",
        contents="Hello Gemini",
        estimated_tokens=150,
    )
    assert resp.text == "Gemini reply to Hello Gemini"


async def test_gemini_adapter_fallback():
    class FailingGemini:
        class aio:
            class models:
                @staticmethod
                async def generate_content(model: str, contents: str, **kwargs):
                    raise TransientHTTPError(503, "Service Unavailable")

    def gemini_fallback(model: str, contents: str, **kwargs):
        return type("GeminiResponse", (), {"text": "Gemini fallback text"})()

    adapter = ResilientGemini(
        client=FailingGemini(),
        max_retries=1,
        fallback=gemini_fallback,
    )
    if adapter.retry_policy:
        adapter.retry_policy.backoff.base_delay = 0.001

    resp = await adapter.generate_content(
        model="gemini-2.5-flash",
        contents="Test",
    )
    assert resp.text == "Gemini fallback text"
