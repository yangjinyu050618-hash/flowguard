"""
FlowGuard Distributed Multi-Worker FastAPI Gateway Integration Pattern.

Demonstrates a production-grade distributed ASGI Gateway featuring:
1. Multi-Worker Cluster Rate Limiting: Shared RPM & TPM budgeting via RedisTokenBucketLimiter (<1ms atomic Lua)
2. Streaming Concurrency Isolation: ResilientStream holding Bulkhead concurrency slots across entire SSE lifecycle
3. High-Availability Degradation: Automatic fallback_local degradation to in-memory TokenBucket on Redis disconnect
4. Client Request Cancellation: Zero-leak concurrency slot recovery upon mid-stream client disconnect
5. Token Refund Reconciliation: Returning unconsumed tokens to Redis for Prompt Caching discounts
6. Observability: Prometheus telemetry exposition endpoint (/metrics) & health check (/health)
"""
# ruff: noqa: E402

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import logging
import os
import sys
import time
from typing import Any, AsyncIterator, Dict, List, Optional, cast

# Ensure src/ is on sys.path for standalone invocation
_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from flowguard import (
    BulkheadFullError,
    CircuitBreakerOpenError,
    FlowGuard,
    MidStreamInterruptedError,
    RateLimitExceededError,
)
from flowguard.backends.redis import (
    LUA_ACQUIRE_SCRIPT,
    LUA_REFUND_SCRIPT,
    LUA_RESET_SCRIPT,
    RedisTokenBucketLimiter,
)
from flowguard.core.bulkhead import Bulkhead
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.core.retry import ExponentialBackoff, RetryPolicy
from flowguard.core.streaming import ResilientStream
from flowguard.metrics.collector import MetricsCollector
from flowguard.metrics.exporter import export_prometheus

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("distributed_gateway")


# ============================================================================
# In-Memory Simulated Redis Client for Standalone Execution & Offline Demos
# ============================================================================
class SimulatedRedisClient:
    """
    Lightweight in-memory asynchronous Redis client that accurately emulates
    FlowGuard's Redis Lua scripts for offline standalone testing and demonstrations.
    """

    def __init__(self) -> None:
        self.storage: Dict[str, Dict[str, str]] = {}
        self.connected: bool = True

    async def eval(self, script: str, numkeys: int, key: str, *args: Any) -> Any:
        if not self.connected:
            raise ConnectionError("Simulated Redis connection failure (network partition)")

        now = time.time()
        if script == LUA_ACQUIRE_SCRIPT:
            requested = float(args[0])
            rate = float(args[1])
            capacity = float(args[2])
            client_now = float(args[3]) if float(args[3]) > 0 else now
            initial = float(args[4]) if len(args) > 4 and args[4] is not None else capacity

            bucket = self.storage.get(key)
            if bucket is None:
                tokens = min(capacity, initial)
                last_refill = client_now
            else:
                tokens = float(bucket["tokens"])
                last_refill = float(bucket.get("last_refill", str(client_now)))
                elapsed = max(0.0, client_now - last_refill)
                tokens = min(capacity, tokens + elapsed * rate)
                last_refill = client_now

            if tokens >= requested:
                tokens -= requested
                allowed = 1
                wait_time = 0.0
            else:
                allowed = 0
                wait_time = (requested - tokens) / rate

            self.storage[key] = {"tokens": str(tokens), "last_refill": str(last_refill)}
            return [allowed, str(tokens), str(wait_time)]

        elif script == LUA_REFUND_SCRIPT:
            refund_tokens = float(args[0])
            capacity = float(args[1])
            rate = float(args[2])
            client_now = float(args[3]) if float(args[3]) > 0 else now

            bucket = self.storage.get(key)
            if bucket is None:
                tokens = capacity
                last_refill = client_now
            else:
                tokens = float(bucket["tokens"])
                last_refill = float(bucket.get("last_refill", str(client_now)))
                elapsed = max(0.0, client_now - last_refill)
                tokens = min(capacity, tokens + elapsed * rate)
                tokens = min(capacity, tokens + refund_tokens)
                last_refill = client_now

            self.storage[key] = {"tokens": str(tokens), "last_refill": str(last_refill)}
            return str(tokens)

        elif script == LUA_RESET_SCRIPT:
            capacity = float(args[0])
            self.storage[key] = {"tokens": str(capacity), "last_refill": str(now)}
            return str(capacity)

        raise NotImplementedError(f"Unsupported Lua script: {script[:30]}...")

    def disconnect(self) -> None:
        """Simulate network partition or Redis service restart."""
        self.connected = False

    def reconnect(self) -> None:
        """Restore network connectivity to Redis."""
        self.connected = True


# ============================================================================
# Gateway Data Models
# ============================================================================
@dataclass
class GatewayRequest:
    request_id: str
    user_id: str
    prompt: str
    estimated_tokens: int = 100
    actual_tokens: Optional[int] = None
    stream: bool = False


@dataclass
class GatewayResponse:
    request_id: str
    status: str
    content: str
    model: str
    tokens_budgeted: int
    tokens_refunded: int = 0
    error: Optional[str] = None


# ============================================================================
# Distributed LLM Gateway Service
# ============================================================================
class DistributedLLMGateway:
    """
    Production-grade LLM Gateway Service engineered for distributed multi-worker
    ASGI deployments (e.g. uvicorn --workers 4).
    """

    def __init__(
        self,
        redis_client: Optional[Any] = None,
        rate: float = 20.0,
        capacity: float = 30.0,
        max_concurrent_streams: int = 5,
    ) -> None:
        self.metrics = MetricsCollector(name="distributed-llm-gateway")

        # 1. Shared Distributed Rate Limiter with Automatic Local Fallback
        # If Redis disconnects, degrades to local in-memory TokenBucket with 0.5 capacity ratio.
        self.redis_client = redis_client or SimulatedRedisClient()
        self.limiter = RedisTokenBucketLimiter(
            client=self.redis_client,
            rate=rate,
            capacity=capacity,
            key="flowguard:gateway:global_rate",
            on_redis_error="fallback_local",
            local_capacity_ratio=0.5,
            recovery_interval=1.0,
        )

        # 2. Concurrency Bulkhead Gate
        # Bounded concurrency slots held across the entire streaming lifespan.
        self.bulkhead = Bulkhead(
            max_concurrent=max_concurrent_streams,
            max_queued=10,
            queue_timeout=2.0,
        )

        # 3. Circuit Breaker for Downstream Model Outage Protection
        self.cb = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=3.0,
            half_open_max_probes=1,
        )

        # 4. Jittered Retry Policy
        self.retry = RetryPolicy(
            max_attempts=2,
            backoff=ExponentialBackoff(base_delay=0.02, jitter="decorrelated"),
        )

        # 5. Composed FlowGuard Pipeline
        self.pipeline = FlowGuard(
            name="distributed-gateway",
            limiter=self.limiter,
            bulkhead=self.bulkhead,
            circuit_breaker=self.cb,
            retry=self.retry,
            metrics=self.metrics,
        )

    @property
    def is_degraded(self) -> bool:
        """Indicate whether the rate limiter is currently degraded to local fallback mode."""
        return bool(self.limiter.is_fallback_active)

    async def _execute_primary_model(self, req: GatewayRequest) -> GatewayResponse:
        """Simulate dispatching a non-streaming request to upstream LLM."""
        if "sim_fail" in req.prompt:
            raise ConnectionResetError("Upstream LLM 503 Service Unavailable")
        if "sim_hang" in req.prompt:
            await asyncio.sleep(5.0)

        await asyncio.sleep(0.01)  # Simulate network latency

        # Calculate Prompt Caching discount refund
        tokens_budgeted = req.estimated_tokens
        actual_used = req.actual_tokens or (req.estimated_tokens // 2)
        refund_amount = max(0, tokens_budgeted - actual_used)
        if refund_amount > 0:
            await self.limiter.refund(tokens=float(refund_amount))

        return GatewayResponse(
            request_id=req.request_id,
            status="SUCCESS",
            content=f"LLM response to '{req.prompt}' (Degraded Mode: {self.is_degraded})",
            model="gpt-4o",
            tokens_budgeted=tokens_budgeted,
            tokens_refunded=refund_amount,
        )

    async def handle_request(self, req: GatewayRequest) -> GatewayResponse:
        """Entry point for non-streaming Gateway HTTP requests."""
        try:
            return cast(
                GatewayResponse,
                await self.pipeline.execute(self._execute_primary_model, req),
            )
        except (RateLimitExceededError, CircuitBreakerOpenError, BulkheadFullError) as err:
            logger.warning("Request %s rejected: %s", req.request_id, err)
            return GatewayResponse(
                request_id=req.request_id,
                status="REJECTED",
                content="",
                model="none",
                tokens_budgeted=0,
                error=str(err),
            )

    async def _generate_token_stream(self, req: GatewayRequest) -> AsyncIterator[str]:
        """Simulate an upstream LLM token stream (SSE generator)."""
        chunks = [
            "Hello",
            " from",
            " FlowGuard",
            " streaming",
            " resilience",
            "!",
        ]

        for i, chunk in enumerate(chunks):
            await asyncio.sleep(0.02)

            if "sim_mid_error" in req.prompt and i == 2:
                # Simulate mid-stream network interruption
                raise ConnectionResetError("Mid-stream connection dropped by upstream provider")

            if "sim_hang" in req.prompt:
                await asyncio.sleep(10.0)

            yield chunk

    async def handle_stream(self, req: GatewayRequest) -> ResilientStream[str]:
        """
        Entry point for streaming LLM requests.
        Crucially, execute_stream transfers ownership of the Bulkhead concurrency slot
        to ResilientStream, which holds it across all chunk iterations.
        """
        # Execute stream returns a ResilientStream wrapping the generator
        return await self.pipeline.execute_stream(
            lambda: self._generate_token_stream(req),
            prefetch_first_chunk=True,
        )


# ============================================================================
# FastAPI Application Factory
# ============================================================================
def create_app(gateway: Optional[DistributedLLMGateway] = None) -> Any:
    """Create and configure the production FastAPI application."""
    try:
        from fastapi import FastAPI, HTTPException, Request
        from fastapi.responses import JSONResponse, Response, StreamingResponse
    except ImportError:
        logger.warning("fastapi not installed; create_app returning None")
        return None

    gw = gateway or DistributedLLMGateway()
    app = FastAPI(
        title="FlowGuard Distributed LLM Gateway",
        description="Production Multi-Worker LLM Gateway with Redis Rate Limiting & Streaming Bulkheads",
        version="0.3.0",
    )

    @app.post("/v1/chat/completions")
    async def chat_completions(req: Dict[str, Any]) -> Dict[str, Any]:
        """Standard JSON completion endpoint with distributed rate limiting."""
        gw_req = GatewayRequest(
            request_id=req.get("request_id", f"req-{time.time()}"),
            user_id=req.get("user_id", "anon"),
            prompt=req.get("prompt", ""),
            estimated_tokens=int(req.get("estimated_tokens", 100)),
            actual_tokens=req.get("actual_tokens"),
            stream=False,
        )
        res = await gw.handle_request(gw_req)
        if res.status == "REJECTED":
            raise HTTPException(status_code=429, detail=res.error)

        return {
            "request_id": res.request_id,
            "status": res.status,
            "content": res.content,
            "model": res.model,
            "tokens_budgeted": res.tokens_budgeted,
            "tokens_refunded": res.tokens_refunded,
            "limiter_mode": "fallback_local" if gw.is_degraded else "redis_distributed",
        }

    @app.post("/v1/chat/completions/stream")
    async def chat_completions_stream(req: Dict[str, Any], raw_req: Request) -> Any:
        """
        Streaming completion endpoint emitting Server-Sent Events (SSE).
        Holds Bulkhead concurrency slot throughout the entire streaming lifespan.
        """
        gw_req = GatewayRequest(
            request_id=req.get("request_id", f"req-stream-{time.time()}"),
            user_id=req.get("user_id", "anon"),
            prompt=req.get("prompt", ""),
            estimated_tokens=int(req.get("estimated_tokens", 100)),
            stream=True,
        )

        try:
            stream: ResilientStream[str] = await gw.handle_stream(gw_req)
        except (RateLimitExceededError, BulkheadFullError, CircuitBreakerOpenError) as err:
            raise HTTPException(status_code=429, detail=str(err))

        async def sse_generator() -> AsyncIterator[str]:
            try:
                async for chunk in stream:
                    # Check if client disconnected early
                    if await raw_req.is_disconnected():
                        logger.info("Client disconnected early; aborting SSE stream.")
                        await stream.aclose()
                        break

                    data = {
                        "chunk": chunk,
                        "chunks_delivered": stream.chunks_delivered,
                        "ttft_ms": round(stream.ttft * 1000, 2) if stream.ttft else None,
                    }
                    yield f"data: {json.dumps(data)}\n\n"

                yield "data: [DONE]\n\n"
            except MidStreamInterruptedError as exc:
                logger.error("Stream interrupted mid-flight: %s", exc)
                err_data = {"error": str(exc), "chunks_delivered": exc.chunks_delivered}
                yield f"event: error\ndata: {json.dumps(err_data)}\n\n"
            except asyncio.CancelledError:
                logger.info("Streaming task cancelled; bulkhead slot released.")
                raise

        return StreamingResponse(sse_generator(), media_type="text/event-stream")

    @app.get("/health")
    async def health() -> JSONResponse:
        """Health check endpoint exposing limiter degradation state and active concurrency slots."""
        return JSONResponse(
            {
                "status": "healthy",
                "limiter_mode": "fallback_local" if gw.is_degraded else "redis_distributed",
                "active_bulkhead_streams": gw.bulkhead.active_count,
                "circuit_state": gw.cb.state.value,
            }
        )

    @app.get("/metrics")
    async def metrics() -> Response:
        """Prometheus metrics exposition endpoint."""
        return Response(content=export_prometheus(gw.metrics), media_type="text/plain")

    return app


# Module-level ASGI application entrypoint for uvicorn
app = create_app()


# ============================================================================
# Standalone Demonstration Runner
# ============================================================================
async def run_standalone_demonstration() -> None:
    """
    Demonstrates FlowGuard's distributed multi-worker architecture, streaming
    bulkhead lifecycle holding, and automatic degradation on Redis disconnect.
    """
    print("\n" + "=" * 78)
    print(" 🛡️  FlowGuard Distributed Multi-Worker FastAPI Gateway Demonstration")
    print("=" * 78 + "\n")

    sim_redis = SimulatedRedisClient()
    gateway = DistributedLLMGateway(redis_client=sim_redis, rate=5.0, capacity=10.0)

    # ------------------------------------------------------------------------
    # Step 1: Normal Distributed Rate Limiting & Non-Streaming Request
    # ------------------------------------------------------------------------
    print("▶ Step 1: Handling Normal Non-Streaming Request (Shared Redis Rate Limiter)...")
    req1 = GatewayRequest(
        request_id="REQ-001",
        user_id="USR-101",
        prompt="Explain distributed consensus.",
        estimated_tokens=100,
        actual_tokens=40,
    )
    res1 = await gateway.handle_request(req1)
    print(f"  Status          : {res1.status}")
    print(f"  Content         : {res1.content}")
    print(f"  Tokens Budgeted : {res1.tokens_budgeted}")
    print(f"  Tokens Refunded : {res1.tokens_refunded} (Prompt Caching Reconciliation via Lua)")
    print(f"  Limiter Mode    : {'fallback_local' if gateway.is_degraded else 'redis_distributed'}")
    assert res1.status == "SUCCESS"
    assert res1.tokens_refunded == 60

    # ------------------------------------------------------------------------
    # Step 2: Streaming Request with ResilientStream & Bulkhead Slot Holding
    # ------------------------------------------------------------------------
    print("\n▶ Step 2: Streaming Request with Bulkhead Concurrency Slot Lifecycle Holding (SSE)...")
    req2 = GatewayRequest(
        request_id="REQ-002",
        user_id="USR-102",
        prompt="Stream response chunks.",
        stream=True,
    )

    print(f"  Bulkhead active slots BEFORE stream start: {gateway.bulkhead.active_count}")
    stream: ResilientStream[str] = await gateway.handle_stream(req2)

    # Slot is actively held during streaming
    print(
        f"  Bulkhead active slots DURING stream execution: {gateway.bulkhead.active_count} (SLOT HELD)"
    )
    assert gateway.bulkhead.active_count == 1

    chunks: List[str] = []
    async for chunk in stream:
        chunks.append(chunk)

    print(f"  Delivered Chunks : {''.join(chunks)}")
    print(f"  Total Chunks     : {stream.chunks_delivered}")
    print(
        f"  TTFT (First Token): {round((stream.ttft or 0.0) * 1000, 2)} ms (Recorded via MetricsCollector)"
    )
    print(
        f"  Bulkhead active slots AFTER stream completion: {gateway.bulkhead.active_count} (SLOT RELEASED)"
    )
    assert gateway.bulkhead.active_count == 0

    # ------------------------------------------------------------------------
    # Step 3: Client Disconnect / Cancellation Mid-Stream (Zero-Leak Slot Recovery)
    # ------------------------------------------------------------------------
    print("\n▶ Step 3: Handling Client Disconnect Mid-Stream (Zero-Leak Concurrency Recovery)...")
    req3 = GatewayRequest(
        request_id="REQ-003",
        user_id="USR-103",
        prompt="Simulate client drop mid-stream (sim_hang)",
        stream=True,
    )

    stream3 = await gateway.handle_stream(req3)
    assert gateway.bulkhead.active_count == 1

    async def client_consumer() -> None:
        async for chunk in stream3:
            pass

    task = asyncio.create_task(client_consumer())
    await asyncio.sleep(0.01)

    # Client aborts / drops connection
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("  Client cancelled connection. asyncio.CancelledError trapped cleanly.")

    print(
        f"  Bulkhead active slots AFTER cancellation: {gateway.bulkhead.active_count} (ZERO LEAK!)"
    )
    assert gateway.bulkhead.active_count == 0

    # ------------------------------------------------------------------------
    # Step 4: Redis Outage & Automatic Local Degradation (fallback_local)
    # ------------------------------------------------------------------------
    print("\n▶ Step 4: Simulating Redis Network Outage & Automatic Fallback Degradation...")
    sim_redis.disconnect()
    print("  Simulated Redis disconnected (network partition).")

    req4 = GatewayRequest(
        request_id="REQ-004",
        user_id="USR-104",
        prompt="Request during Redis outage.",
        estimated_tokens=50,
    )
    res4 = await gateway.handle_request(req4)
    print(f"  Status          : {res4.status}")
    print(f"  Content         : {res4.content}")
    print(f"  Limiter Mode    : {'fallback_local' if gateway.is_degraded else 'redis_distributed'}")
    assert gateway.is_degraded is True
    assert res4.status == "SUCCESS"
    print("  [SUCCESS] Request was served via in-memory TokenBucket without dropping traffic!")

    # ------------------------------------------------------------------------
    # Step 5: Redis Self-Healing Recovery
    # ------------------------------------------------------------------------
    print("\n▶ Step 5: Redis Connectivity Restored (Self-Healing Recovery)...")
    sim_redis.reconnect()
    # Wait for recovery interval cooldown
    await asyncio.sleep(1.05)

    req5 = GatewayRequest(
        request_id="REQ-005",
        user_id="USR-105",
        prompt="Request after Redis reconnection.",
        estimated_tokens=50,
    )
    res5 = await gateway.handle_request(req5)
    print(f"  Status          : {res5.status}")
    print(f"  Limiter Mode    : {'fallback_local' if gateway.is_degraded else 'redis_distributed'}")
    assert gateway.is_degraded is False
    assert res5.status == "SUCCESS"
    print("  [SUCCESS] Rate limiter automatically recovered to distributed Redis mode!")

    # ------------------------------------------------------------------------
    # Step 6: Mid-Stream Failure Isolation (No Duplicate Retries)
    # ------------------------------------------------------------------------
    print("\n▶ Step 6: Mid-Stream Error Isolation (MidStreamInterruptedError)...")
    req6 = GatewayRequest(
        request_id="REQ-006",
        user_id="USR-106",
        prompt="Stream with mid-stream provider error (sim_mid_error)",
        stream=True,
    )

    stream6 = await gateway.handle_stream(req6)
    delivered_before_error = 0
    try:
        async for _ in stream6:
            delivered_before_error += 1
    except MidStreamInterruptedError as mid_err:
        print(f"  Trapped MidStreamInterruptedError: {mid_err}")
        print(f"  Chunks delivered before failure : {mid_err.chunks_delivered}")
        print("  [SUCCESS] Prevented duplicate re-execution from corrupting stream output!")

    assert gateway.bulkhead.active_count == 0
    print(f"  Bulkhead active slots after error : {gateway.bulkhead.active_count} (RELEASED)")

    # ------------------------------------------------------------------------
    # Step 7: Prometheus Telemetry Snapshot
    # ------------------------------------------------------------------------
    print("\n▶ Step 7: Prometheus Telemetry Exposition Snapshot (/metrics)...")
    prom_output = export_prometheus(gateway.metrics)
    for line in prom_output.strip().splitlines()[:10]:
        print(f"  {line}")
    print("  ...")

    print("\n" + "=" * 78)
    print(" ✅ All Distributed Gateway Architectural Invariants Verified Successfully!")
    print("=" * 78 + "\n")


if __name__ == "__main__":
    asyncio.run(run_standalone_demonstration())
