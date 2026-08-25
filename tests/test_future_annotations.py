"""Tests for FallbackContext protocol with from __future__ import annotations."""

from __future__ import annotations

from flowguard.core.pipeline import FlowGuard
from flowguard.core.fallback import FallbackContext, ChoiceFallback


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
