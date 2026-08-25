"""Comprehensive regression test suite for P1 and P2 review fixes with strict signatures."""

import asyncio
import pytest
from flowguard.core.pipeline import FlowGuard
from flowguard.core.fallback import FallbackContext, ChoiceFallback, with_fallback_context
from flowguard.core.retry import is_permanent_client_error
from flowguard.adapters.anthropic_adapter import ResilientAnthropic
from flowguard.adapters.gemini_adapter import ResilientGemini
from flowguard.adapters.openai_adapter import ResilientOpenAI
from flowguard.exceptions import TransientHTTPError


# -------------------------------------------------------------------------
# P1-1: Authentic Strict SDK Resource Method Signatures (WITHOUT **kwargs!)
# -------------------------------------------------------------------------
class StrictOpenAICompletions:
    """Exact strict signature of openai.resources.chat.completions.AsyncCompletions.create WITHOUT **kwargs."""

    def __init__(self, parent_client):
        self.parent = parent_client

    async def create(self, *, messages, model, temperature=1.0, max_tokens=None, stream=False):
        return {
            "choices": [
                {
                    "message": {
                        "content": f"OpenAI response with parent retries={self.parent.max_retries}"
                    }
                }
            ]
        }


class StrictOpenAIClient:
    """Simulation of openai.AsyncOpenAI client with with_options support."""

    def __init__(self, max_retries: int = 2):
        self.max_retries = max_retries
        self.chat = type("Chat", (), {"completions": StrictOpenAICompletions(self)})()

    def with_options(self, *, max_retries: int = 0):
        return StrictOpenAIClient(max_retries=max_retries)


class StrictAnthropicMessages:
    """Exact strict signature of anthropic.resources.messages.AsyncMessages.create WITHOUT **kwargs."""

    def __init__(self, parent_client):
        self.parent = parent_client

    async def create(self, *, messages, model, max_tokens=1000, temperature=1.0):
        return {
            "content": [
                {"text": f"Anthropic response with parent retries={self.parent.max_retries}"}
            ]
        }


class StrictAnthropicClient:
    """Simulation of anthropic.AsyncAnthropic client with with_options support."""

    def __init__(self, max_retries: int = 2):
        self.max_retries = max_retries
        self.messages = StrictAnthropicMessages(self)

    def with_options(self, *, max_retries: int = 0):
        return StrictAnthropicClient(max_retries=max_retries)


async def test_openai_strict_signature_and_sole_retry_ownership():
    raw_client = StrictOpenAIClient(max_retries=2)
    adapter = ResilientOpenAI(client=raw_client, max_retries=3)

    assert raw_client.max_retries == 2
    assert adapter._client.max_retries == 0

    resp = await adapter.create_chat_completion(
        model="gpt-4o",
        messages=[{"role": "user", "content": "hi"}],
    )
    assert "parent retries=0" in resp["choices"][0]["message"]["content"]


async def test_anthropic_strict_signature_and_sole_retry_ownership():
    raw_client = StrictAnthropicClient(max_retries=2)
    adapter = ResilientAnthropic(client=raw_client, max_retries=3)

    assert raw_client.max_retries == 2
    assert adapter._client.max_retries == 0

    resp = await adapter.create_message(
        model="claude-3-5-sonnet",
        messages=[{"role": "user", "content": "hi"}],
    )
    assert "parent retries=0" in resp["content"][0]["text"]


# -------------------------------------------------------------------------
# P1: Synchronous candidate TypeError must be called EXACTLY ONCE!
# -------------------------------------------------------------------------
async def test_choice_fallback_sync_candidate_type_error_executed_once():
    calls = 0

    def buggy_sync_candidate(prompt: str) -> str:
        nonlocal calls
        calls += 1
        raise TypeError("business logic bug inside sync candidate")

    router = ChoiceFallback(
        candidates={"buggy_sync": buggy_sync_candidate},
        selector=lambda ctx, opts: "buggy_sync",
    )

    pipe = FlowGuard(name="sync-cand-type-err", fallback=router)

    async def fail_task(prompt: str):
        raise ConnectionResetError("Primary model 503")

    with pytest.raises(TypeError) as exc_info:
        await pipe.execute(fail_task, "test_prompt")

    assert "business logic bug inside sync candidate" in str(exc_info.value)
    # Must be called exactly ONCE!
    assert calls == 1


# -------------------------------------------------------------------------
# P1-2: Ordinary business parameters named 'context' / 'ctx' must pass through!
# -------------------------------------------------------------------------
async def test_ordinary_business_fallback_with_context_param_name():
    def business_fallback(context: str, prompt: str):
        return f"business_fallback_res_{context}_{prompt}"

    pipe = FlowGuard(name="business-ctx-pipe", fallback=business_fallback)

    async def fail_task(context: str, prompt: str):
        raise RuntimeError("fail")

    res = await pipe.execute(fail_task, "user_context_val", prompt="user_prompt_val")
    assert res == "business_fallback_res_user_context_val_user_prompt_val"


async def test_ordinary_business_fallback_with_ctx_param_name():
    def business_fallback_ctx(ctx: str, *, user_id: int):
        return f"res_{ctx}_{user_id}"

    pipe = FlowGuard(name="business-ctx2", fallback=business_fallback_ctx)

    async def fail_task(ctx: str, *, user_id: int):
        raise RuntimeError("fail")

    res = await pipe.execute(fail_task, "my_ctx_data", user_id=42)
    assert res == "res_my_ctx_data_42"


async def test_explicit_fallback_context_handler_via_annotation_or_decorator():
    def annotated_handler(c: FallbackContext):
        return f"annotated_{c.kwargs.get('k')}"

    pipe1 = FlowGuard(name="annotated-pipe", fallback=annotated_handler)

    async def f1(k: str):
        raise RuntimeError("err")

    res1 = await pipe1.execute(f1, k="val1")
    assert res1 == "annotated_val1"

    @with_fallback_context
    def decorated_handler(any_name):
        assert isinstance(any_name, FallbackContext)
        return f"decorated_{any_name.kwargs.get('k')}"

    pipe2 = FlowGuard(name="dec-pipe", fallback=decorated_handler)
    res2 = await pipe2.execute(f1, k="val2")
    assert res2 == "decorated_val2"


# -------------------------------------------------------------------------
# P2: FallbackContext truly immutable (MappingProxyType)
# -------------------------------------------------------------------------
def test_fallback_context_kwargs_true_immutability():
    ctx = FallbackContext(
        exception=RuntimeError("test"),
        args=(1, 2),
        kwargs={"key": "before"},
        pipeline_name="test-pipe",
    )
    assert ctx.kwargs["key"] == "before"

    with pytest.raises(TypeError):
        ctx.kwargs["key"] = "after"  # type: ignore

    with pytest.raises(TypeError):
        ctx.kwargs["new_key"] = "val"  # type: ignore


# -------------------------------------------------------------------------
# P1-1: Async Fallback & ChoiceFallback candidate TypeError executed ONCE
# -------------------------------------------------------------------------
async def test_fallback_internal_type_error_executed_once():
    call_count = 0

    def buggy_fallback(c: FallbackContext):
        nonlocal call_count
        call_count += 1
        return "number: " + 123  # type: ignore

    pipe = FlowGuard(name="type-error-pipe", fallback=buggy_fallback)

    async def fail_task():
        raise RuntimeError("Service crash")

    with pytest.raises(TypeError) as exc_info:
        await pipe.execute(fail_task)

    assert "can only concatenate str" in str(exc_info.value) or "must be str" in str(exc_info.value)
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
# P1-2: Parameter collision immunity & token preservation
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
    assert is_permanent_client_error(MockGoogleGenAIClientError(400, "Bad Request")) is True
    assert is_permanent_client_error(MockGoogleGenAIClientError(401, "Unauthenticated")) is True
    assert is_permanent_client_error(MockGoogleGenAIClientError(403, "Permission Denied")) is True
    assert is_permanent_client_error(MockGoogleGenAIClientError(404, "Not Found")) is True

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

    assert calls == 1


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

    mutable_candidates["m1"] = lambda prompt, **kw: "mutated"
    mutable_candidates["m2"] = lambda prompt, **kw: "m2"

    pipe = FlowGuard(name="freeze-pipe", fallback=router)

    async def fail_call(prompt: str):
        raise ConnectionError("down")

    res = await pipe.execute(fail_call, "test")
    assert res == "m1_test"

    cancel_router = ChoiceFallback(
        candidates={"m1": lambda prompt, **kw: "m1"},
        selector=lambda ctx, opts: None,
    )
    cancel_pipe = FlowGuard(name="cancel-pipe", fallback=cancel_router)

    with pytest.raises(ConnectionError):
        await cancel_pipe.execute(fail_call, "test")
