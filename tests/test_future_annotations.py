"""Tests for FallbackContext protocol with from __future__ import annotations."""

from __future__ import annotations

from flowguard.core.pipeline import FlowGuard
from flowguard.core.fallback import FallbackContext, ChoiceFallback


class BusinessFallbackContext:
    """An unrelated business class that happens to end with 'FallbackContext'."""

    def __init__(self, trace_id: str):
        self.trace_id = trace_id


async def test_future_annotations_fallback_context():
    def deferred_fallback(ctx: FallbackContext):
        return f"deferred_handled_{type(ctx.exception).__name__}_{ctx.kwargs.get('data')}"

    pipe = FlowGuard(name="future-pipe", fallback=deferred_fallback)

    async def fail_op(data: str):
        raise ValueError("simulated_error")

    result = await pipe.execute(fail_op, data="test_payload")
    assert result == "deferred_handled_ValueError_test_payload"


async def test_future_annotations_choice_fallback_candidate():
    def deferred_candidate(ctx: FallbackContext):
        return f"cand_{ctx.kwargs.get('prompt')}"

    router = ChoiceFallback(
        candidates={"cand1": deferred_candidate},
        selector=lambda ctx, opts: "cand1",
    )

    pipe = FlowGuard(name="future-choice-pipe", fallback=router)

    async def fail_op(prompt: str):
        raise ConnectionError("down")

    res = await pipe.execute(fail_op, prompt="future_world")
    assert res == "cand_future_world"


async def test_return_annotation_does_not_trigger_context_handler():
    """
    Assert that a function returning FallbackContext (-> FallbackContext) does NOT
    get mistaken for an input FallbackContext consumer. Original input arguments must pass through.
    """
    seen = []

    def business_fallback(prompt: str) -> FallbackContext:
        seen.append(prompt)
        return FallbackContext(
            exception=RuntimeError("dummy"),
            args=(),
            kwargs={},
            pipeline_name="custom",
        )

    pipe = FlowGuard(name="return-annot-pipe", fallback=business_fallback)

    async def fail_op(prompt: str):
        raise ConnectionError("upstream unavailable")

    res = await pipe.execute(fail_op, "hello_world")
    assert seen == ["hello_world"]
    assert isinstance(res, FallbackContext)


async def test_unrelated_suffix_type_does_not_trigger_context_handler():
    """
    Assert that an unrelated business type (e.g. BusinessFallbackContext) is NOT
    mistaken for FlowGuard's FallbackContext, and the original business instance is passed.
    """
    seen = []

    def business_fallback(val: BusinessFallbackContext):
        seen.append(val)
        return f"handled_{val.trace_id}"

    pipe = FlowGuard(name="unrelated-suffix-pipe", fallback=business_fallback)

    async def fail_op(val: BusinessFallbackContext):
        raise ConnectionError("upstream unavailable")

    biz_ctx = BusinessFallbackContext("TRACE-999")
    res = await pipe.execute(fail_op, biz_ctx)
    assert res == "handled_TRACE-999"
    assert seen == [biz_ctx]
