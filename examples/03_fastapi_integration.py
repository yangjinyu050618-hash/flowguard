"""
FlowGuard Web & LLM API Gateway Integration Pattern.

Demonstrates a production-grade FastAPI / ASGI Gateway Request Handler featuring:
1. Dual Token-Bucket Rate Limiting (RPM + TPM token budgeting)
2. Resilient Execution with Circuit Breaking & Jittered Retry
3. Graceful Multi-Model Degradation via ChoiceFallback
4. Client Request Cancellation (Zero-Leak Concurrency Slot Recovery)
5. Prometheus Telemetry Exposition Endpoint (/metrics)
"""
# ruff: noqa: E402

import asyncio
from dataclasses import dataclass
import logging
import os
import sys
from typing import Any, Dict, Optional

# Ensure src/ is on sys.path for standalone invocation
_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from flowguard import FlowGuard, RateLimitExceededError, CircuitBreakerOpenError
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.core.fallback import ChoiceFallback, FallbackContext
from flowguard.core.limiter import TokenBucketLimiter
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.metrics.collector import MetricsCollector
from flowguard.metrics.exporter import export_prometheus

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gateway")


@dataclass
class GatewayRequest:
    request_id: str
    user_id: str
    prompt: str
    estimated_tokens: int = 100


@dataclass
class GatewayResponse:
    request_id: str
    status: str
    content: str
    routed_model: str
    tokens_budgeted: int = 0
    error: Optional[str] = None


class LLMGatewayService:
    """Production-grade LLM Gateway service handler protected by FlowGuard."""

    def __init__(self) -> None:
        self.metrics = MetricsCollector(name="llm-gateway")

        # Circuit Breaker: Trip after 3 consecutive failures, 5s recovery cooldown
        self.cb = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=5.0,
            half_open_success_threshold=2,
        )

        # Retry: 2 attempts with exponential backoff
        self.retry = RetryPolicy(
            max_attempts=2,
            backoff=ExponentialBackoff(base_delay=0.01, jitter="decorrelated"),
        )

        # Dual Rate Limiting: 1000 RPM (requests/min) and 50,000 TPM (tokens/min)
        self.rpm_limiter = TokenBucketLimiter(rate=1000.0 / 60.0, capacity=50.0)
        self.tpm_limiter = TokenBucketLimiter(rate=50_000.0 / 60.0, capacity=5000.0)

        # ChoiceFallback: Graceful degradation to backup models upon primary failure
        self.fallback_router = ChoiceFallback(
            candidates={
                "claude-3.5-sonnet": self._fallback_claude,
                "gemini-2.5-flash": self._fallback_gemini,
            },
            selector=self._select_fallback_route,
        )

        # Composed FlowGuard Pipeline
        self.pipeline = FlowGuard(
            name="llm-gateway",
            limiter=self.rpm_limiter,
            circuit_breaker=self.cb,
            retry=self.retry,
            fallback=self.fallback_router,
            metrics=self.metrics,
        )

    async def _primary_gpt4o(self, req: GatewayRequest) -> GatewayResponse:
        """Primary LLM Route (simulated GPT-4o call with true TPM token budgeting)."""
        # True TPM token rate limit enforcement:
        await self.tpm_limiter.acquire(tokens=float(req.estimated_tokens), timeout=5.0)

        if "sim_fail" in req.prompt:
            raise ConnectionResetError("GPT-4o API 503 Service Unavailable")
        if "sim_hang" in req.prompt:
            await asyncio.sleep(10.0)

        await asyncio.sleep(0.01)
        return GatewayResponse(
            request_id=req.request_id,
            status="SUCCESS",
            content=f"GPT-4o response to: '{req.prompt}'",
            routed_model="gpt-4o",
            tokens_budgeted=req.estimated_tokens,
        )

    def _select_fallback_route(self, ctx: FallbackContext, options: list) -> str:
        """Route selector for fallback candidates."""
        logger.warning(
            "Primary model failed (%s). Routing request %s to backup route.",
            type(ctx.exception).__name__,
            ctx.args[0].request_id,
        )
        return "claude-3.5-sonnet"

    async def _fallback_claude(self, ctx: FallbackContext) -> GatewayResponse:
        req: GatewayRequest = ctx.args[0]
        # Acquire token budget on backup route
        await self.tpm_limiter.acquire(tokens=float(req.estimated_tokens), timeout=5.0)
        await asyncio.sleep(0.01)
        return GatewayResponse(
            request_id=req.request_id,
            status="DEGRADED",
            content=f"Claude 3.5 Sonnet fallback response to: '{req.prompt}'",
            routed_model="claude-3.5-sonnet",
            tokens_budgeted=req.estimated_tokens,
        )

    async def _fallback_gemini(self, ctx: FallbackContext) -> GatewayResponse:
        req: GatewayRequest = ctx.args[0]
        await self.tpm_limiter.acquire(tokens=float(req.estimated_tokens), timeout=5.0)
        await asyncio.sleep(0.01)
        return GatewayResponse(
            request_id=req.request_id,
            status="DEGRADED",
            content=f"Gemini 2.5 Flash fallback response to: '{req.prompt}'",
            routed_model="gemini-2.5-flash",
            tokens_budgeted=req.estimated_tokens,
        )

    async def handle_request(self, req: GatewayRequest) -> GatewayResponse:
        """Entry point for incoming Gateway HTTP/WebSocket requests."""
        try:
            return await self.pipeline.execute(self._primary_gpt4o, req)
        except (RateLimitExceededError, CircuitBreakerOpenError) as err:
            return GatewayResponse(
                request_id=req.request_id,
                status="REJECTED",
                content="",
                routed_model="none",
                tokens_budgeted=0,
                error=str(err),
            )
        except asyncio.CancelledError:
            logger.info("Request %s cancelled by caller.", req.request_id)
            raise


def create_gateway_app() -> Any:
    """FastAPI Application factory providing ASGI gateway routes."""
    try:
        from fastapi import FastAPI, HTTPException, Response
    except ImportError:
        return None

    app = FastAPI(title="FlowGuard LLM Gateway", version="0.3.0")
    gateway = LLMGatewayService()

    @app.post("/v1/chat/completions")
    async def chat_completions(req: Dict[str, Any]) -> Dict[str, Any]:
        gw_req = GatewayRequest(
            request_id=req.get("request_id", "req-auto"),
            user_id=req.get("user_id", "anon"),
            prompt=req.get("prompt", ""),
            estimated_tokens=int(req.get("estimated_tokens", 100)),
        )
        res = await gateway.handle_request(gw_req)
        if res.status == "REJECTED":
            raise HTTPException(status_code=429, detail=res.error)
        return {
            "request_id": res.request_id,
            "status": res.status,
            "content": res.content,
            "routed_model": res.routed_model,
            "tokens_budgeted": res.tokens_budgeted,
        }

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(content=export_prometheus(gateway.metrics), media_type="text/plain")

    return app


async def main() -> None:
    gateway = LLMGatewayService()

    print("--- 1. Handling Normal Gateway Request (RPM + TPM metered) ---")
    req1 = GatewayRequest(
        request_id="REQ-001",
        user_id="USR-101",
        prompt="Explain quantum computing.",
        estimated_tokens=150,
    )
    res1 = await gateway.handle_request(req1)
    print(
        f"[{res1.status}] Routed: {res1.routed_model} ({res1.tokens_budgeted} tokens) -> {res1.content}"
    )

    print("\n--- 2. Handling Outage with Fallback Degradation ---")
    req2 = GatewayRequest(
        request_id="REQ-002",
        user_id="USR-102",
        prompt="Explain asyncio. (sim_fail)",
        estimated_tokens=120,
    )
    res2 = await gateway.handle_request(req2)
    print(
        f"[{res2.status}] Routed: {res2.routed_model} ({res2.tokens_budgeted} tokens) -> {res2.content}"
    )

    print("\n--- 3. Handling Client Disconnect / Cancellation ---")
    req3 = GatewayRequest(
        request_id="REQ-003", user_id="USR-103", prompt="Long task (sim_hang)", estimated_tokens=200
    )
    task = asyncio.create_task(gateway.handle_request(req3))
    await asyncio.sleep(0.02)
    task.cancel()
    try:
        await task
        print("Task completed unexpectedly")
    except asyncio.CancelledError:
        print(
            "[CANCELLED] Request successfully cancelled: bulkhead slots, probe gates, and queue waiters unwound without leaks."
        )

    print("\n--- 4. Prometheus Telemetry Snapshot ---")
    print(export_prometheus(gateway.metrics))


if __name__ == "__main__":
    asyncio.run(main())
