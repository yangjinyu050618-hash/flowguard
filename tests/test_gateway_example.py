"""Tests for Web & LLM Gateway integration reference pattern."""

import asyncio
import importlib.util
import os
import pytest


def _load_gateway_module():
    spec = importlib.util.spec_from_file_location(
        "fastapi_integration",
        os.path.join(os.path.dirname(__file__), "..", "examples", "03_fastapi_integration.py"),
    )
    if spec is None or spec.loader is None:
        raise ImportError("Failed to load 03_fastapi_integration.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_gw = _load_gateway_module()
LLMGatewayService = _gw.LLMGatewayService
GatewayRequest = _gw.GatewayRequest
create_gateway_app = _gw.create_gateway_app


@pytest.mark.asyncio
async def test_gateway_successful_request():
    gateway = LLMGatewayService()
    req = GatewayRequest(
        request_id="REQ-TEST-1", user_id="USR-1", prompt="Hello", estimated_tokens=150
    )
    res = await gateway.handle_request(req)

    assert res.status == "SUCCESS"
    assert res.routed_model == "gpt-4o"
    assert res.tokens_budgeted == 150
    assert "GPT-4o response" in res.content


@pytest.mark.asyncio
async def test_gateway_fallback_degradation():
    gateway = LLMGatewayService()
    req = GatewayRequest(
        request_id="REQ-TEST-2", user_id="USR-2", prompt="Test (sim_fail)", estimated_tokens=120
    )
    res = await gateway.handle_request(req)

    assert res.status == "DEGRADED"
    assert res.routed_model == "claude-3.5-sonnet"
    assert res.tokens_budgeted == 120
    assert "Claude 3.5 Sonnet fallback" in res.content


@pytest.mark.asyncio
async def test_gateway_cancellation_propagation():
    gateway = LLMGatewayService()
    req = GatewayRequest(
        request_id="REQ-TEST-3", user_id="USR-3", prompt="Hang (sim_hang)", estimated_tokens=200
    )

    task = asyncio.create_task(gateway.handle_request(req))
    await asyncio.sleep(0.02)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # Ensure subsequent request executes cleanly without resource deadlock
    req_after = GatewayRequest(
        request_id="REQ-TEST-4", user_id="USR-4", prompt="After", estimated_tokens=100
    )
    res_after = await gateway.handle_request(req_after)
    assert res_after.status == "SUCCESS"
    assert res_after.tokens_budgeted == 100


@pytest.mark.asyncio
async def test_fastapi_endpoints_actual_requests():
    """Verify actual ASGI HTTP requests to /v1/chat/completions and /metrics."""
    try:
        import httpx
        from fastapi import FastAPI
    except ImportError:
        pytest.skip("fastapi or httpx not installed; skipping ASGI endpoint test")

    app = create_gateway_app()
    assert isinstance(app, FastAPI), "Expected create_gateway_app() to return a FastAPI instance"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testgateway"
    ) as client:
        # 1. Test POST /v1/chat/completions normal request
        payload1 = {
            "request_id": "REQ-ASGI-1",
            "user_id": "U1",
            "prompt": "Hello",
            "estimated_tokens": 100,
        }
        resp1 = await client.post("/v1/chat/completions", json=payload1)
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["status"] == "SUCCESS"
        assert data1["routed_model"] == "gpt-4o"
        assert data1["tokens_budgeted"] == 100

        # 2. Test POST /v1/chat/completions fallback request
        payload2 = {
            "request_id": "REQ-ASGI-2",
            "user_id": "U2",
            "prompt": "Outage test (sim_fail)",
            "estimated_tokens": 80,
        }
        resp2 = await client.post("/v1/chat/completions", json=payload2)
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["status"] == "DEGRADED"
        assert data2["routed_model"] == "claude-3.5-sonnet"
        assert data2["tokens_budgeted"] == 80

        # 3. Test GET /metrics Prometheus exposition endpoint
        resp3 = await client.get("/metrics")
        assert resp3.status_code == 200
        assert resp3.headers["content-type"].startswith("text/plain")
        assert "flowguard_requests_total" in resp3.text
        assert "flowguard_latency_p50_ms" in resp3.text
