"""
Example 01: Quickstart with @guard decorator
============================================
Demonstrates using the all-in-one @guard decorator to protect
an async function with rate limiting, retries, and circuit breaking.
"""

import asyncio
import os
import random
import sys

# Allow running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from flowguard import guard


# Protect this function with 10 calls/sec, 3 auto-retries, and circuit breaking
@guard(
    name="payment-processor",
    rate_per_sec=10.0,  # Max 10 calls per second
    burst_capacity=15.0,  # Allow bursts up to 15 calls
    max_retries=3,  # Auto-retry up to 3 times on transient errors
    failure_threshold=5,  # Trip circuit breaker after 5 consecutive failures
    recovery_timeout=10.0,  # Wait 10s before probing in HALF_OPEN state
    max_concurrent=5,  # Bulkhead: allow max 5 concurrent executions
)
async def process_payment(order_id: str, amount: float) -> str:
    # Simulate occasional network flakiness (20% failure rate)
    if random.random() < 0.2:
        raise ConnectionResetError("Payment gateway temporarily unavailable (503)")

    await asyncio.sleep(0.05)  # Simulate network latency
    return f"Payment of ${amount:.2f} for order '{order_id}' succeeded!"


async def main() -> None:
    print("--- Starting Payment Processing Batch (20 requests) ---")
    tasks = [process_payment(f"ORD-{i:03d}", 99.9) for i in range(20)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"[{i:02d}] FAILED: {type(res).__name__} - {res}")
        else:
            print(f"[{i:02d}] SUCCESS: {res}")


if __name__ == "__main__":
    asyncio.run(main())
