# FlowGuard Examples & Architecture Patterns

This directory contains executable reference architectures, production integration patterns, and runnable scripts showcasing FlowGuard's async resilience engine.

---

## 📂 Example Index

| File | Primary Concepts | Description |
| :--- | :--- | :--- |
| **[`01_quickstart_guard.py`](01_quickstart_guard.py)** | Quickstart, Basic Pipeline | Introductory example showing `FlowGuard` pipeline composition with `TokenBucketLimiter`, `CircuitBreaker`, and `RetryPolicy`. |
| **[`02_openai_resilient_chat.py`](02_openai_resilient_chat.py)** | LLM Adapter, Sole Retry Ownership | Demonstrates `ResilientOpenAI` wrapping official `AsyncOpenAI`, with RPM/TPM budgeting and zero duplicate SDK retries. |
| **[`03_fastapi_integration.py`](03_fastapi_integration.py)** | ASGI Gateway, Dual Token-Bucket | Production-grade FastAPI request handler featuring dual RPM + TPM budgeting, multi-model fallback, zero-leak cancellation, and Prometheus metrics. |
| **[`04_fallback_graceful_degradation.py`](04_fallback_graceful_degradation.py)** | Fallback, Error Handling | Graceful degradation patterns when upstream services experience transient or permanent failures. |
| **[`05_multi_llm_orchestration.py`](05_multi_llm_orchestration.py)** | Multi-LLM, ChoiceFallback | Orchestrates failover routes across OpenAI, Anthropic Claude, and Google Gemini with unified token accounting. |
| **[`06_interactive_human_in_the_loop_fallback.py`](06_interactive_human_in_the_loop_fallback.py)** | HITL, Typed FallbackContext | Demonstrates interactive human-in-the-loop candidate selection using `ChoiceFallback` and typed `FallbackContext`. |
| **[`distributed_fastapi_gateway.py`](distributed_fastapi_gateway.py)** | Distributed Rate Limiting, Streaming Bulkhead | Production multi-worker gateway with `RedisTokenBucketLimiter` (<1ms atomic Lua), automatic `fallback_local` degradation on Redis disconnect, and `ResilientStream` SSE endpoints with full-lifecycle Bulkhead slot holding. |

---

## 🚀 Running Examples

All examples can be executed directly as standalone Python scripts:

```bash
# 1. Basic Quickstart
python examples/01_quickstart_guard.py

# 2. FastAPI In-Process Gateway Pattern
python examples/03_fastapi_integration.py

# 3. Distributed Multi-Worker FastAPI Gateway
python examples/distributed_fastapi_gateway.py
```

### Running the Distributed Gateway with Uvicorn

The distributed gateway (`examples/distributed_fastapi_gateway.py`) is designed for multi-process deployments sharing a global rate limit via Redis:

```bash
# Start 4 worker processes behind ASGI server
uvicorn examples.distributed_fastapi_gateway:app --workers 4 --port 8000
```

#### Key Architecture Highlights of `distributed_fastapi_gateway.py`:
1. **Multi-Worker Shared Rate Limiting**: All 4 worker processes coordinate global RPM and TPM quotas through Redis via atomic single-round-trip Lua scripts without distributed lock contention.
2. **Automatic High-Availability Degradation**: If Redis drops or encounters network timeouts, the gateway seamlessly degrades to `fallback_local` (local in-process `TokenBucketLimiter`), ensuring zero downtime.
3. **Streaming Bulkhead Lifecycle Holding**: Concurrency slots are acquired before upstream dispatch and held across the entire lifetime of `ResilientStream` until the last SSE chunk is yielded, client disconnects, or stream breaks.
4. **Zero-Leak Client Disconnect Recovery**: When a client closes the browser tab or mobile app mid-stream, `asyncio.CancelledError` immediately unwinds the stream, halts upstream generators, and deterministically frees bulkhead concurrency slots.
