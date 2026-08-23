import asyncio
import pytest
from flowguard.core.pipeline import guard, FlowGuard
from flowguard.core.limiter import TokenBucketLimiter


@pytest.mark.asyncio
async def test_guard_decorator():
    call_count = 0

    @guard(name="test-guard", rate_per_sec=50.0, max_retries=2)
    async def fetch_user(user_id: int) -> dict:
        nonlocal call_count
        call_count += 1
        return {"user_id": user_id, "status": "active"}

    res = await fetch_user(42)
    assert res == {"user_id": 42, "status": "active"}
    assert call_count == 1
