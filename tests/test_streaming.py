"""Comprehensive test suite for ResilientStream and FlowGuard streaming resilience."""

import asyncio
from typing import AsyncIterator, List
import pytest
from flowguard.core.bulkhead import Bulkhead
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.core.pipeline import FlowGuard, guard, guard_stream
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.streaming import ResilientStream
from flowguard.exceptions import (
    BulkheadFullError,
    MidStreamInterruptedError,
)
from flowguard.metrics.collector import MetricsCollector


async def sample_generator(items: List[str], delay: float = 0.0) -> AsyncIterator[str]:
    for item in items:
        if delay > 0:
            await asyncio.sleep(delay)
        yield item


async def failing_midstream_generator() -> AsyncIterator[str]:
    yield "token1"
    yield "token2"
    await asyncio.sleep(0.01)
    raise RuntimeError("Connection dropped by remote host")


@pytest.mark.asyncio
async def test_resilient_stream_normal_consumption():
    bulkhead = Bulkhead(max_concurrent=1, max_queued=0)
    circuit = CircuitBreaker(failure_threshold=3)
    metrics = MetricsCollector(name="test_stream")

    await bulkhead.acquire()
    assert bulkhead.active_count == 1

    stream: ResilientStream[str] = ResilientStream(
        iterator=sample_generator(["Hello", " ", "World"]),
        bulkhead=bulkhead,
        circuit_breaker=circuit,
        metrics=metrics,
    )

    collected: List[str] = []
    async for chunk in stream:
        collected.append(chunk)

    assert collected == ["Hello", " ", "World"]
    assert stream.is_closed is True
    assert stream.chunks_delivered == 3
    assert bulkhead.active_count == 0
    assert circuit.state == CircuitState.CLOSED
    assert metrics.success_count == 1
    assert stream.ttft is not None


@pytest.mark.asyncio
async def test_resilient_stream_empty():
    bulkhead = Bulkhead(max_concurrent=1)
    await bulkhead.acquire()

    stream: ResilientStream[str] = ResilientStream(
        iterator=sample_generator([]),
        bulkhead=bulkhead,
        is_empty=True,
    )

    collected: List[str] = []
    async for chunk in stream:
        collected.append(chunk)

    assert collected == []
    assert stream.is_closed is True
    assert bulkhead.active_count == 0


@pytest.mark.asyncio
async def test_resilient_stream_bulkhead_concurrency_isolation():
    """Verify Bulkhead slot is held throughout stream iteration and blocks concurrent callers."""
    pipeline = FlowGuard(
        name="test_bulkhead_stream",
        bulkhead=Bulkhead(max_concurrent=1, max_queued=0),
    )

    release_stream_event = asyncio.Event()

    async def slow_stream_func() -> AsyncIterator[str]:
        yield "chunk_start"
        await release_stream_event.wait()
        yield "chunk_end"

    stream = await pipeline.execute_stream(slow_stream_func)

    # First chunk was prefetched, stream is open, bulkhead must be active
    assert pipeline.bulkhead is not None
    assert pipeline.bulkhead.active_count == 1

    # Concurrent call must be rejected by bulkhead
    async def another_task():
        return "ok"

    with pytest.raises(BulkheadFullError):
        await pipeline.execute(another_task)

    # Consume stream to completion
    first = await stream.__anext__()
    assert first == "chunk_start"
    release_stream_event.set()
    second = await stream.__anext__()
    assert second == "chunk_end"

    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()

    # After completion, bulkhead must be freed
    assert pipeline.bulkhead.active_count == 0
    res = await pipeline.execute(another_task)
    assert res == "ok"


@pytest.mark.asyncio
async def test_resilient_stream_early_break_with_async_context():
    """Verify early break inside async with stream: releases bulkhead slot deterministically."""
    bulkhead = Bulkhead(max_concurrent=1, max_queued=0)
    pipeline = FlowGuard(name="test_break", bulkhead=bulkhead)

    async def infinite_tokens() -> AsyncIterator[int]:
        count = 0
        while True:
            yield count
            count += 1
            await asyncio.sleep(0.001)

    stream = await pipeline.execute_stream(infinite_tokens)
    assert bulkhead.active_count == 1

    async with stream:
        async for item in stream:
            if item == 2:
                break

    assert stream.is_closed is True
    assert bulkhead.active_count == 0

    # Ensure another operation can run immediately
    await bulkhead.acquire()
    assert bulkhead.active_count == 1
    await bulkhead.release()


@pytest.mark.asyncio
async def test_resilient_stream_explicit_aclose():
    """Verify explicit aclose() releases bulkhead and marks stream closed."""
    bulkhead = Bulkhead(max_concurrent=1)
    pipeline = FlowGuard(name="test_aclose", bulkhead=bulkhead)

    stream = await pipeline.execute_stream(sample_generator(["a", "b", "c"]))
    assert bulkhead.active_count == 1

    chunk = await stream.__anext__()
    assert chunk == "a"

    await stream.aclose()
    assert stream.is_closed is True
    assert bulkhead.active_count == 0

    # Idempotency check: calling aclose again is completely safe
    await stream.aclose()
    assert bulkhead.active_count == 0


@pytest.mark.asyncio
async def test_resilient_stream_pre_token_failure_retry():
    """Pre-first-token failures must be retried transparently by RetryPolicy."""
    attempts = 0

    async def flaky_stream_func() -> AsyncIterator[str]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("Upstream handshake failed")
        for chunk in ["hello", "world"]:
            yield chunk

    pipeline = FlowGuard(
        name="test_retry_stream",
        retry=RetryPolicy(max_attempts=3, backoff=ExponentialBackoff(base_delay=0.01)),
    )

    stream = await pipeline.execute_stream(flaky_stream_func)
    assert attempts == 3

    collected = [chunk async for chunk in stream]
    assert collected == ["hello", "world"]


@pytest.mark.asyncio
async def test_resilient_stream_mid_stream_failure():
    """Mid-stream failures must NOT be retried, must trip circuit breaker and raise MidStreamInterruptedError."""
    circuit = CircuitBreaker(failure_threshold=1)
    bulkhead = Bulkhead(max_concurrent=2)
    pipeline = FlowGuard(
        name="test_midstream",
        circuit_breaker=circuit,
        bulkhead=bulkhead,
        retry=RetryPolicy(max_attempts=3, backoff=ExponentialBackoff(base_delay=0.01)),
    )

    stream = await pipeline.execute_stream(failing_midstream_generator)

    first = await stream.__anext__()
    assert first == "token1"

    second = await stream.__anext__()
    assert second == "token2"

    with pytest.raises(MidStreamInterruptedError) as exc_info:
        await stream.__anext__()

    err = exc_info.value
    assert err.chunks_delivered == 2
    assert "Connection dropped" in str(err.cause)

    # Bulkhead must be released
    assert bulkhead.active_count == 0
    # Circuit breaker must have tripped to OPEN
    assert circuit.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_resilient_stream_cancellation():
    """Cancellation of consumer task releases bulkhead slot without corrupting circuit state."""
    bulkhead = Bulkhead(max_concurrent=1)
    circuit = CircuitBreaker(failure_threshold=5)
    pipeline = FlowGuard(name="test_cancel", bulkhead=bulkhead, circuit_breaker=circuit)

    hanging_event = asyncio.Event()

    async def hanging_stream() -> AsyncIterator[str]:
        yield "first"
        await hanging_event.wait()
        yield "second"

    stream = await pipeline.execute_stream(hanging_stream)
    assert bulkhead.active_count == 1

    async def consumer():
        async for chunk in stream:
            pass

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.01)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Bulkhead slot must be freed
    assert bulkhead.active_count == 0
    assert circuit.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_guard_stream_decorator():
    """Test @guard_stream decorator syntax."""

    @guard_stream(name="test_dec", max_concurrent=2)
    async def generate_response(prompt: str) -> AsyncIterator[str]:
        for word in prompt.split():
            yield word

    stream = await generate_response("FlowGuard is resilient")
    words = [w async for w in stream]
    assert words == ["FlowGuard", "is", "resilient"]


@pytest.mark.asyncio
async def test_guard_decorator_with_async_generator():
    """Test @guard automatically detecting async generator function."""

    @guard(name="test_auto_gen", max_concurrent=2)
    async def my_generator() -> AsyncIterator[int]:
        for i in range(3):
            yield i

    stream = await my_generator()
    results = [num async for num in stream]
    assert results == [0, 1, 2]


@pytest.mark.asyncio
async def test_stream_fallback():
    """Test fallback when stream pre-token call fails completely."""

    async def failing_stream() -> AsyncIterator[str]:
        raise RuntimeError("Immediate failure")
        yield "never"

    async def fallback_func(*args, **kwargs) -> List[str]:
        return ["fallback_token"]

    pipeline = FlowGuard(
        name="test_stream_fb",
        fallback=fallback_func,
    )

    res = await pipeline.execute_stream(failing_stream)
    assert res == ["fallback_token"]
