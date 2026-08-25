"""Tests for Anthropic Claude resilience adapter."""

from flowguard.adapters.anthropic_adapter import ResilientAnthropic
from flowguard.exceptions import TransientHTTPError


class MockAnthropic:
    def __init__(self):
        self.calls = 0
        self.messages = self

    async def create(self, **kwargs):
        self.calls += 1
        model = kwargs.get("model", "claude-3-5-sonnet")
        return {
            "id": "msg_mock",
            "model": model,
            "content": [{"text": "Anthropic response"}],
        }


async def test_anthropic_adapter_basic():
    mock = MockAnthropic()
    adapter = ResilientAnthropic(
        client=mock,
        rpm_limit=600.0,
        tpm_limit=60_000.0,
    )

    resp = await adapter.create_message(
        estimated_tokens=200,
        model="claude-3-5-sonnet",
        max_tokens=1000,
        messages=[{"role": "user", "content": "Hello Claude"}],
    )
    assert resp["content"][0]["text"] == "Anthropic response"
    assert mock.calls == 1


async def test_anthropic_adapter_fallback():
    class FailingAnthropic:
        class messages:
            @staticmethod
            async def create(**kwargs):
                raise TransientHTTPError(529, "Overloaded")

    def claude_fallback(estimated_tokens: int = 500, **kwargs):
        return {"content": [{"text": "Fallback from local model"}]}

    adapter = ResilientAnthropic(
        client=FailingAnthropic(),
        max_retries=1,
        fallback=claude_fallback,
    )
    if adapter.retry_policy:
        adapter.retry_policy.backoff.base_delay = 0.001

    resp = await adapter.create_message(
        model="claude-3-5-sonnet",
        messages=[{"role": "user", "content": "Hi"}],
    )
    assert resp["content"][0]["text"] == "Fallback from local model"
