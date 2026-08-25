"""
Example 04: Graceful Degradation with Fallback
==============================================
Demonstrates how FlowGuard automatically executes fallback routines
when upstream LLMs or microservices trip circuit breakers or exhaust retries.
"""

import asyncio
import os
import sys

# Allow running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from flowguard import guard


# Define a graceful fallback handler
async def cached_response_fallback(query: str, exc: Exception = None) -> str:
    print(f"  [Fallback Activated] Primary service failed with {type(exc).__name__}: {exc}")
    return f"[CACHED RESULT] Default weather report for '{query}': 20°C, Clear Sky."


# Protect with 1 failure threshold and automatic fallback
@guard(
    name="weather-service",
    failure_threshold=1,
    recovery_timeout=30.0,
    fallback=cached_response_fallback,
)
async def query_live_weather(city: str) -> str:
    print(f"Calling upstream weather API for '{city}'...")
    # Simulate an external gateway crash
    raise ConnectionRefusedError("Weather API gateway is unreachable (502)")


async def main() -> None:
    print("--- Testing Fallback Graceful Degradation ---")
    result = await query_live_weather("San Francisco")
    print(f"Final Returned Result: {result}\n")


if __name__ == "__main__":
    asyncio.run(main())
