"""Automated tests for deterministic resilience benchmark execution."""

import pytest
from benchmarks.resilience_scenario import run_scenario


@pytest.mark.asyncio
async def test_resilience_scenario_invariants_and_schema():
    """Verify that run_scenario executes cleanly and returns valid schema satisfying all invariants."""
    data = await run_scenario()

    required_keys = {
        "total_requests",
        "successful_requests",
        "primary_calls",
        "retried_calls",
        "fallback_calls",
        "cancelled_requests",
        "exceptions_caught",
        "circuit_state_final",
        "latency_p50_ms",
        "latency_p95_ms",
    }
    assert required_keys.issubset(data.keys())

    assert data["total_requests"] == 50
    assert data["successful_requests"] > 0
    assert data["fallback_calls"] > 0
    assert data["cancelled_requests"] == 3
    assert data["retried_calls"] > 0
    assert data["circuit_state_final"] == "CLOSED"
    assert data["latency_p50_ms"] >= 0.0
    assert data["latency_p95_ms"] >= data["latency_p50_ms"]
    assert (
        data["successful_requests"]
        + data["fallback_calls"]
        + data["cancelled_requests"]
        + data["exceptions_caught"]
        == data["total_requests"]
    )
