import asyncio
import pytest
from flowguard.core.bulkhead import Bulkhead
from flowguard.exceptions import BulkheadFullError


@pytest.mark.asyncio
async def test_bulkhead_concurrency_limit():
    bh = Bulkhead(max_concurrent=2, max_queued=1)

    async def worker():
        async with bh:
            await asyncio.sleep(0.05)

    t1 = asyncio.create_task(worker())
    t2 = asyncio.create_task(worker())
    t3 = asyncio.create_task(worker())
    await asyncio.sleep(0.01)

    # 4th should exceed queue
    with pytest.raises(BulkheadFullError):
        async with bh:
            pass

    await asyncio.gather(t1, t2, t3)
