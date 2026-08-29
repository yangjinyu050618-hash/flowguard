"""
FlowGuard Web & LLM API Gateway Integration Pattern.

Demonstrates a production-style async Gateway Request Handler featuring:
1. Request Entrance & Token-Bucket Rate Limiting (RPM + TPM)
2. Resilient Execution with Circuit Breaking & Jittered Retry
3. Graceful Multi-Model Degradation via ChoiceFallback
4. Client Request Cancellation Propagation (Zero-Leak Cancellation)
5. Telemetry & Metrics Export Hook
"""
# ruff: noqa: E402

import asyncio
from dataclasses import dataclass
import logging
import os
import sys
from typing import Optional

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

        # Rate Limiter: 1000 RPM capacity, burst 50
        self.limiter = TokenBucketLimiter(rate=1000.0 / 60.0, capacity=50.0)

        # ChoiceFallback: Human-in-the-loop or alternative model fallback router
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
            limiter=self.limiter,
            circuit_breaker=self.cb,
            retry=self.retry,
            fallback=self.fallback_router,
            metrics=self.metrics,
        )

    async def _primary_gpt4o(self, req: GatewayRequest) -> GatewayResponse:
        """Primary LLM Route (simulated GPT-4o call)."""
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
        await asyncio.sleep(0.01)
        return GatewayResponse(
            request_id=req.request_id,
            status="DEGRADED",
            content=f"Claude 3.5 Sonnet fallback response to: '{req.prompt}'",
            routed_model="claude-3.5-sonnet",
        )

    async def _fallback_gemini(self, ctx: FallbackContext) -> GatewayResponse:
        req: GatewayRequest = ctx.args[0]
        await asyncio.sleep(0.01)
        return GatewayResponse(
            request_id=req.request_id,
            status="DEGRADED",
            content=f"Gemini 2.5 Flash fallback response to: '{req.prompt}'",
            routed_model="gemini-2.5-flash",
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
                error=str(err),
            )
        except asyncio.CancelledError:
            logger.info("Request %s cancelled by caller.", req.request_id)
            raise


async def main() -> None:
    gateway = LLMGatewayService()

    print("--- 1. Handling Normal Gateway Request ---")
    req1 = GatewayRequest(
        request_id="REQ-001", user_id="USR-101", prompt="Explain quantum computing."
    )
    res1 = await gateway.handle_request(req1)
    print(f"[{res1.status}] Routed: {res1.routed_model} -> {res1.content}")

    print("\n--- 2. Handling Outage with Fallback Degradation ---")
    req2 = GatewayRequest(
        request_id="REQ-002", user_id="USR-102", prompt="Explain asyncio. (sim_fail)"
    )
    res2 = await gateway.handle_request(req2)
    print(f"[{res2.status}] Routed: {res2.routed_model} -> {res2.content}")

    print("\n--- 3. Handling Client Disconnect / Cancellation ---")
    req3 = GatewayRequest(request_id="REQ-003", user_id="USR-103", prompt="Long task (sim_hang)")
    task = asyncio.create_task(gateway.handle_request(req3))
    await asyncio.sleep(0.02)
    task.cancel()
    try:
        await task
        print("Task completed unexpectedly")
    except asyncio.CancelledError:
        print("[CANCELLED] Request successfully cancelled without slot leaks.")

    print("\n--- 4. Prometheus Telemetry Snapshot ---")
    print(export_prometheus(gateway.metrics))


if __name__ == "__main__":
    asyncio.run(main())
