"""
Example 03: FastAPI Web Service Integration & Prometheus Telemetry
==================================================================
Demonstrates how to integrate FlowGuard into a FastAPI microservice
and expose Prometheus metrics.
"""

import asyncio
import os
import sys

# Allow running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from flowguard import FlowGuard, TokenBucketLimiter, CircuitBreaker
from flowguard.metrics import export_prometheus

# Setup a pipeline for downstream weather API
weather_pipeline = FlowGuard(
    name="weather-api",
    limiter=TokenBucketLimiter(rate=5.0, capacity=10.0),
    circuit_breaker=CircuitBreaker(failure_threshold=3, recovery_timeout=5.0),
)


async def fetch_weather(city: str) -> dict:
    """Mock external weather provider."""

    async def _call():
        await asyncio.sleep(0.02)
        return {"city": city, "temperature": 22.5, "condition": "Sunny"}

    return await weather_pipeline.execute(_call)


async def main() -> None:
    print("--- Simulating FastAPI Request Handling ---")

    # Process multiple incoming web requests
    cities = ["Tokyo", "London", "New York", "Paris", "Berlin"]
    for city in cities:
        data = await fetch_weather(city)
        print(f"API Response: {data}")

    print("\n--- Exporting Prometheus Metrics Endpoint ---")
    print(export_prometheus(weather_pipeline.metrics))


if __name__ == "__main__":
    asyncio.run(main())
