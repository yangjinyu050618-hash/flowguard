"""Comprehensive tests for ResilientOpenAICompatible adapter, TokenBucketLimiter.refund, and prompt caching reconciliation."""

import asyncio
from typing import Any, AsyncGenerator, Dict
from unittest.mock import MagicMock
import pytest

from flowguard import TokenBucketLimiter
from flowguard.adapters import (
    ResilientOpenAICompatible,
    create_deepseek_adapter,
    create_ollama_adapter,
    create_vllm_adapter,
)
from flowguard.core.streaming import ResilientStream


class FakeUsageDetails:
    def __init__(self, cached_tokens: int = 0, reasoning_tokens: int = 0) -> None:
        self.cached_tokens = cached_tokens
        self.reasoning_tokens = reasoning_tokens


class FakeUsage:
    def __init__(
        self,
        prompt_tokens: int = 1000,
        completion_tokens: int = 200,
        cached_tokens: int = 0,
        reasoning_tokens: int = 0,
    ) -> None:
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = prompt_tokens + completion_tokens
        self.prompt_tokens_details = FakeUsageDetails(cached_tokens=cached_tokens)
        self.completion_tokens_details = FakeUsageDetails(reasoning_tokens=reasoning_tokens)


class FakeChatCompletionResponse:
    def __init__(
        self,
        content: str = "Hello from DeepSeek!",
        prompt_tokens: int = 1000,
        completion_tokens: int = 200,
        cached_tokens: int = 0,
        reasoning_tokens: int = 0,
    ) -> None:
        self.content = content
        self.usage = FakeUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cached_tokens=cached_tokens,
            reasoning_tokens=reasoning_tokens,
        )


class FakeOpenAIClient:
    def __init__(self) -> None:
        self.max_retries: int = 2
        self.chat = MagicMock()
        self.chat.completions = MagicMock()

    def with_options(self, max_retries: int) -> "FakeOpenAIClient":
        clone = FakeOpenAIClient()
        clone.max_retries = max_retries
        clone.chat = self.chat
        return clone


@pytest.mark.asyncio
async def test_token_bucket_refund_wakes_waiters():
    """Verify refund() restores tokens and immediately unblocks queued waiters."""
    limiter = TokenBucketLimiter(rate=10.0, capacity=100.0, initial_tokens=10.0)

    # Acquire all 10 tokens so bucket is empty
    assert await limiter.acquire(tokens=10.0)
    assert limiter.tokens == pytest.approx(0.0, abs=0.5)

    res_arrived = False

    async def waiter():
        nonlocal res_arrived
        # Needs 30 tokens, will queue (30 / 10 = 3.0s, timeout=10.0s allows queuing)
        await limiter.acquire(tokens=30.0, timeout=10.0)
        res_arrived = True

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)
    assert not res_arrived

    # Refund 50 tokens: waiter must be immediately drained and finish
    await limiter.refund(50.0)
    await asyncio.sleep(0.02)
    assert res_arrived
    await task


@pytest.mark.asyncio
async def test_adapter_strips_client_retries_and_factory():
    """Verify client-level retries are stripped via with_options(max_retries=0)."""
    fake_client = FakeOpenAIClient()
    adapter = ResilientOpenAICompatible(client=fake_client, rpm_limit=100.0)
    assert adapter._client.max_retries == 0

    deepseek = create_deepseek_adapter(client=fake_client, rpm_limit=200.0)
    assert deepseek._client.max_retries == 0

    vllm = create_vllm_adapter(client=fake_client, rpm_limit=400.0)
    assert vllm._client.max_retries == 0

    ollama = create_ollama_adapter(client=fake_client, rpm_limit=50.0)
    assert ollama._client.max_retries == 0


@pytest.mark.asyncio
async def test_prompt_caching_token_reconciliation():
    """Verify prompt cache hits calculate discounted usage and refund excess tokens."""
    fake_client = FakeOpenAIClient()

    # 1000 prompt tokens (800 cached, 200 uncached), 200 completion tokens
    # With 0.5 discount: effective = 200 + (800 * 0.5) + 200 = 800 tokens
    # If estimated was 2000, 1200 tokens should be refunded back!
    fake_response = FakeChatCompletionResponse(
        prompt_tokens=1000,
        completion_tokens=200,
        cached_tokens=800,
        reasoning_tokens=50,
    )

    async def mock_create(**kwargs: Any) -> Any:
        return fake_response

    fake_client.chat.completions.create = mock_create

    adapter = ResilientOpenAICompatible(
        client=fake_client,
        rpm_limit=600.0,
        tpm_limit=60_000.0,
        tpm_burst_capacity=10_000.0,
    )
    assert adapter.tpm_limiter is not None
    # Set known initial tokens
    adapter.tpm_limiter.tokens = 5000.0

    # Execute with estimated 2000 tokens
    res = await adapter.create_chat_completion(
        estimated_tokens=2000,
        cache_discount=0.5,
        model="deepseek-reasoner",
        messages=[{"role": "user", "content": "hi"}],
    )
    assert res.content == "Hello from DeepSeek!"

    # 5000 - 2000 (acquired) + 1200 (refunded) = 4200 (approx)
    assert adapter.tpm_limiter.tokens >= 4190.0


@pytest.mark.asyncio
async def test_chat_completion_stream():
    """Verify create_chat_completion_stream wraps response into ResilientStream."""
    fake_client = FakeOpenAIClient()

    async def mock_stream_gen() -> AsyncGenerator[Dict[str, str], None]:
        yield {"chunk": "chunk_0"}
        yield {"chunk": "chunk_1"}

    async def mock_create(**kwargs: Any) -> Any:
        return mock_stream_gen()

    fake_client.chat.completions.create = mock_create

    adapter = ResilientOpenAICompatible(
        client=fake_client,
        rpm_limit=300.0,
    )

    stream = await adapter.create_chat_completion_stream(
        estimated_tokens=300,
        model="vllm-model",
    )
    assert isinstance(stream, ResilientStream)

    chunks = [c async for c in stream]
    assert chunks == [{"chunk": "chunk_0"}, {"chunk": "chunk_1"}]
    assert stream.is_closed


def test_extract_usage_metrics_deepseek_and_o1():
    # 1. DeepSeek R1 format
    deepseek_usage = {
        "prompt_tokens": 1200,
        "completion_tokens": 800,
        "total_tokens": 2000,
        "prompt_cache_hit_tokens": 600,
        "completion_tokens_details": {"reasoning_tokens": 500},
    }
    mock_ds_resp = type("Resp", (), {"usage": deepseek_usage})()
    ds_metrics = ResilientOpenAICompatible.extract_usage_metrics(mock_ds_resp)
    assert ds_metrics["prompt_tokens"] == 1200
    assert ds_metrics["completion_tokens"] == 800
    assert ds_metrics["cached_tokens"] == 600
    assert ds_metrics["reasoning_tokens"] == 500

    # 2. OpenAI o1/o3-mini format
    o1_usage = {
        "prompt_tokens": 500,
        "completion_tokens": 300,
        "total_tokens": 800,
        "prompt_tokens_details": {"cached_tokens": 250},
        "completion_tokens_details": {"reasoning_tokens": 200},
    }
    mock_o1_resp = type("Resp", (), {"usage": o1_usage})()
    o1_metrics = ResilientOpenAICompatible.extract_usage_metrics(mock_o1_resp)
    assert o1_metrics["cached_tokens"] == 250
    assert o1_metrics["reasoning_tokens"] == 200


def test_factory_helpers_service_names():
    fake_client = FakeOpenAIClient()
    ds = create_deepseek_adapter(client=fake_client)
    assert ds.service_name == "deepseek"

    vllm = create_vllm_adapter(client=fake_client)
    assert vllm.service_name == "vllm"

    ollama = create_ollama_adapter(client=fake_client)
    assert ollama.service_name == "ollama"
