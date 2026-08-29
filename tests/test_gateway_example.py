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


@pytest.mark.asyncio
async def test_gateway_successful_request():
    gateway = LLMGatewayService()
    req = GatewayRequest(request_id="REQ-TEST-1", user_id="USR-1", prompt="Hello")
    res = await gateway.handle_request(req)

    assert res.status == "SUCCESS"
    assert res.routed_model == "gpt-4o"
    assert "GPT-4o response" in res.content


@pytest.mark.asyncio
async def test_gateway_fallback_degradation():
    gateway = LLMGatewayService()
    req = GatewayRequest(request_id="REQ-TEST-2", user_id="USR-2", prompt="Test (sim_fail)")
    res = await gateway.handle_request(req)

    assert res.status == "DEGRADED"
    assert res.routed_model == "claude-3.5-sonnet"
    assert "Claude 3.5 Sonnet fallback" in res.content


@pytest.mark.asyncio
async def test_gateway_cancellation_propagation():
    gateway = LLMGatewayService()
    req = GatewayRequest(request_id="REQ-TEST-3", user_id="USR-3", prompt="Hang (sim_hang)")

    task = asyncio.create_task(gateway.handle_request(req))
    await asyncio.sleep(0.02)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # Ensure subsequent request executes cleanly without resource deadlock
    req_after = GatewayRequest(request_id="REQ-TEST-4", user_id="USR-4", prompt="After")
    res_after = await gateway.handle_request(req_after)
    assert res_after.status == "SUCCESS"
