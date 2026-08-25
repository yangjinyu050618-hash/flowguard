"""Comprehensive regression test suite for P1 and P2 review fixes."""

import asyncio
import pytest
from flowguard.core.pipeline import FlowGuard
from flowguard.core.fallback import FallbackContext, ChoiceFallback
from flowguard.core.retry import is_permanent_client_error
from flowguard.adapters.anthropic_adapter import ResilientAnthropic
from flowguard.adapters.gemini_adapter import ResilientGemini
from flowguard.exceptions import TransientHTTPError


# -------------------------------------------------------------------------
# P1-1: No TypeError guessing & Handlers execute exactly ONCE
# -------------------------------------------------------------------------
async def test_fallback_internal_type_error_executed_once():
    call_count = 0

    def buggy_fallback(ctx: FallbackContext):
        nonlocal call_count
        call_count += 1
        # Internal business TypeError
        return "number: " + 123  # type: ignore

    pipe = FlowGuard(name="type-error-pipe", fallback=buggy_fallback)

    async def fail_task():
        raise RuntimeError("Service crash")

    with pytest.raises(TypeError) as exc_info:
        await pipe.execute(fail_task)

    assert "can only concatenate str" in str(exc_info.value) or "must be str" in str(exc_info.value)
    # Must be called exactly ONCE, NOT multiple times due to guessing!
    assert call_count == 1


async def test_choice_fallback_candidate_type_error_executed_once():
    candidate_calls = 0

    async def buggy_candidate(prompt: str):
        nonlocal candidate_calls
        candidate_calls += 1
        raise TypeError("Bug inside candidate logic")

    router = ChoiceFallback(
        candidates={"buggy": buggy_candidate},
        selector=lambda ctx, opts: "buggy",
    )

    pipe = FlowGuard(name="choice-type-err", fallback=router)

    async def fail_task(prompt: str):
        raise ConnectionResetError("503")

    with pytest.raises(TypeError) as exc_info:
        await pipe.execute(fail_task, "test_prompt")

    assert "Bug inside candidate logic" in str(exc_info.value)
    assert candidate_calls == 1


# -------------------------------------------------------------------------
# P1-2: FallbackContext & Parameter collision immunity
# -------------------------------------------------------------------------
async def test_fallback_with_caller_exc_keyword_collision():
    received_ctx = None

    def my_fallback(ctx: FallbackContext):
        nonlocal received_ctx
        received_ctx = ctx
        return f"fallback_for_{ctx.kwargs.get('exc')}"

    pipe = FlowGuard(name="collision-pipe", fallback=my_fallback)

    async def task_with_exc_param(query: str, exc: str = "default_exc"):
        raise ValueError("Task failed")

    # Call with exc="user_supplied_exc"
    res = await pipe.execute(task_with_exc_param, "my_query", exc="user_supplied_exc")
    assert res == "fallback_for_user_supplied_exc"
    assert received_ctx is not None
    assert isinstance(received_ctx.exception, ValueError)
    assert received_ctx.kwargs["exc"] == "user_supplied_exc"
    assert received_ctx.args == ("my_query",)


async def test_estimated_tokens_preserved_in_fallback():
    captured_ctx = None

    def capture_fallback(ctx: FallbackContext):
        nonlocal captured_ctx
        captured_ctx = ctx
        return {"content": [{"text": "fallback_ok"}]}

    class FailAnthropic:
        class messages:
            @staticmethod
            async def create(**kwargs):
                raise TransientHTTPError(529, "Overloaded")

    adapter = ResilientAnthropic(
        client=FailAnthropic(),
        max_retries=1,
        fallback=capture_fallback,
    )
    if adapter.retry_policy:
        adapter.retry_policy.backoff.base_delay = 0.001

    await adapter.create_message(
        estimated_tokens=123,
        model="claude-3-5-sonnet",
        messages=[{"role": "user", "content": "hello"}],
    )

    assert captured_ctx is not None
    assert captured_ctx.kwargs.get("estimated_tokens") == 123
    assert captured_ctx.kwargs.get("model") == "claude-3-5-sonnet"


async def test_concurrent_selectors_isolated():
    async def slow_selector(ctx: FallbackContext, options: list):
        await asyncio.sleep(0.01)
        return ctx.kwargs["route_target"]

    router = ChoiceFallback(
        candidates={
            "target_a": lambda prompt, **kw: f"resp_a_{prompt}",
            "target_b": lambda prompt, **kw: f"resp_b_{prompt}",
        },
        selector=slow_selector,
    )

    pipe = FlowGuard(name="concurrent-pipe", fallback=router)

    async def failing_call(prompt: str, route_target: str):
        raise ConnectionError(f"fail_{prompt}")

    t1 = asyncio.create_task(pipe.execute(failing_call, "p1", route_target="target_a"))
    t2 = asyncio.create_task(pipe.execute(failing_call, "p2", route_target="target_b"))

    r1, r2 = await asyncio.gather(t1, t2)
    assert r1 == "resp_a_p1"
    assert r2 == "resp_b_p2"


# -------------------------------------------------------------------------
# P1-3: Google GenAI ClientError(.code) classification
# -------------------------------------------------------------------------
class MockGoogleGenAIClientError(Exception):
    def __init__(self, code: int, message: str = ""):
        super().__init__(f"Google GenAI ClientError ({code}): {message}")
        self.code = code


def test_google_genai_error_classification():
    # 400, 401, 403, 404 must be permanent
    assert is_permanent_client_error(MockGoogleGenAIClientError(400, "Bad Request")) is True
    assert is_permanent_client_error(MockGoogleGenAIClientError(401, "Unauthenticated")) is True
    assert is_permanent_client_error(MockGoogleGenAIClientError(403, "Permission Denied")) is True
    assert is_permanent_client_error(MockGoogleGenAIClientError(404, "Not Found")) is True

    # 429, 500, 503 must be retryable
    assert is_permanent_client_error(MockGoogleGenAIClientError(429, "Resource Exhausted")) is False
    assert (
        is_permanent_client_error(MockGoogleGenAIClientError(500, "Internal Server Error")) is False
    )
    assert (
        is_permanent_client_error(MockGoogleGenAIClientError(503, "Service Unavailable")) is False
    )


async def test_gemini_permanent_400_fail_fast():
    calls = 0

    class FailingGemini:
        class aio:
            class models:
                @staticmethod
                async def generate_content(**kwargs):
                    nonlocal calls
                    calls += 1
                    raise MockGoogleGenAIClientError(400, "Invalid argument")

    adapter = ResilientGemini(client=FailingGemini(), max_retries=4)
    if adapter.retry_policy:
        adapter.retry_policy.backoff.base_delay = 0.001

    with pytest.raises(MockGoogleGenAIClientError):
        await adapter.generate_content(model="gemini-2.5-flash", contents="test")

    # Must fail fast after exactly 1 call!
    assert calls == 1


# -------------------------------------------------------------------------
# P1-4: Sole Retry Owner (No SDK internal retry multiplication)
# -------------------------------------------------------------------------
async def test_sdk_max_retries_zero_enforced():
    captured_kwargs = {}

    class MockAnthropicMessages:
        async def create(self, **kwargs):
            nonlocal captured_kwargs
            captured_kwargs = kwargs
            return {"content": [{"text": "ok"}]}

    class MockAnthropicClient:
        messages = MockAnthropicMessages()

    adapter = ResilientAnthropic(client=MockAnthropicClient(), max_retries=3)
    await adapter.create_message(model="claude-3-5-sonnet", messages=[])

    # Downstream SDK call must have max_retries=0 to prevent unmetered retries
    assert captured_kwargs.get("max_retries") == 0


# -------------------------------------------------------------------------
# P2-1: Gemini sync client detection & fail-fast
# -------------------------------------------------------------------------
def test_gemini_sync_client_rejection():
    class SyncOnlyGemini:
        class models:
            @staticmethod
            def generate_content(model, contents):
                return "sync_result"

    with pytest.raises(TypeError) as exc:
        ResilientGemini(client=SyncOnlyGemini())

    assert "requires an asynchronous client" in str(exc.value)


# -------------------------------------------------------------------------
# P2-2: ChoiceFallback semantic tightening & candidate freezing
# -------------------------------------------------------------------------
async def test_choice_fallback_candidate_freeze_and_none_sentinel():
    mutable_candidates = {
        "m1": lambda prompt, **kw: f"m1_{prompt}",
    }

    router = ChoiceFallback(
        candidates=mutable_candidates,
        selector=lambda ctx, opts: "m1",
    )

    # Mutate caller dict
    mutable_candidates["m1"] = lambda prompt, **kw: "mutated"
    mutable_candidates["m2"] = lambda prompt, **kw: "m2"

    pipe = FlowGuard(name="freeze-pipe", fallback=router)

    async def fail_call(prompt: str):
        raise ConnectionError("down")

    res = await pipe.execute(fail_call, "test")
    # Router must use frozen snapshot: "m1_test", not "mutated"
    assert res == "m1_test"

    # None selector cancels fallback
    cancel_router = ChoiceFallback(
        candidates={"m1": lambda prompt, **kw: "m1"},
        selector=lambda ctx, opts: None,  # User canceled
    )
    cancel_pipe = FlowGuard(name="cancel-pipe", fallback=cancel_router)

    with pytest.raises(ConnectionError):
        await cancel_pipe.execute(fail_call, "test")
