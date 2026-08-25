"""Tests for Fallback graceful degradation mechanism in FlowGuard."""

import asyncio
import pytest
from flowguard.core.pipeline import FlowGuard, guard
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.fallback import FallbackContext, ChoiceFallback


async def test_fallback_on_circuit_breaker_open():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=60.0)
    await cb.record_failure(RuntimeError("Service down"))
    assert cb.state == CircuitState.OPEN

    async def fallback_func(query: str, exc: Exception = None):
        return f"cached_response_for_{query}"

    pipe = FlowGuard(
        name="test-fallback-cb",
        circuit_breaker=cb,
        fallback=fallback_func,
    )

    async def call_api(query: str):
        return f"live_{query}"

    res = await pipe.execute(call_api, "user_question")
    assert res == "cached_response_for_user_question"


async def test_fallback_on_retry_exhaustion():
    async def flaky_call():
        raise ConnectionResetError("Connection reset by peer")

    def sync_fallback():
        return "sync_default_value"

    pipe = FlowGuard(
        name="test-fallback-retry",
        retry=RetryPolicy(
            max_attempts=2, backoff=ExponentialBackoff(base_delay=0.001, jitter="none")
        ),
        fallback=sync_fallback,
    )

    res = await pipe.execute(flaky_call)
    assert res == "sync_default_value"


async def test_fallback_decorator_integration():
    def backup_calculator(a: int, b: int, exc: Exception = None):
        return a + b

    @guard(
        name="calc-guard",
        failure_threshold=1,
        fallback=backup_calculator,
    )
    async def faulty_remote_calc(a: int, b: int) -> int:
        raise TimeoutError("Remote server timed out")

    res = await faulty_remote_calc(10, 20)
    assert res == 30


async def test_fallback_cancellation_does_not_trigger_fallback():
    fallback_called = False

    async def fallback_handler():
        nonlocal fallback_called
        fallback_called = True
        return "fallback"

    pipe = FlowGuard(name="cancel-test", fallback=fallback_handler)

    async def long_running():
        await asyncio.sleep(10.0)

    task = asyncio.create_task(pipe.execute(long_running))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert fallback_called is False


async def test_choice_fallback_interactive_selection():
    async def route_claude(prompt: str):
        return f"claude_solved_{prompt}"

    async def route_gemini(prompt: str):
        return f"gemini_solved_{prompt}"

    selected = "gemini"

    def user_selector(ctx: FallbackContext, options: list):
        assert "claude" in options
        assert "gemini" in options
        assert isinstance(ctx.exception, ConnectionResetError)
        return selected

    router = ChoiceFallback(
        candidates={"claude": route_claude, "gemini": route_gemini},
        selector=user_selector,
    )

    pipe = FlowGuard(name="choice-test", fallback=router)

    async def broken_gpt(prompt: str):
        raise ConnectionResetError("GPT outage")

    # 1. User selects 'gemini'
    res = await pipe.execute(broken_gpt, "hello")
    assert res == "gemini_solved_hello"

    # 2. User returns None -> clean cancellation / re-raise original
    selected = None
    with pytest.raises(ConnectionResetError):
        await pipe.execute(broken_gpt, "hello")

    # 3. Invalid candidate choice raises KeyError
    selected = "non_existent_model"
    with pytest.raises(KeyError):
        await pipe.execute(broken_gpt, "hello")


def test_choice_fallback_validation():
    with pytest.raises(ValueError):
        ChoiceFallback(candidates={}, selector=lambda ctx, opt: None)

    with pytest.raises(TypeError):
        ChoiceFallback(candidates={"a": lambda: 1}, selector=None)  # type: ignore
