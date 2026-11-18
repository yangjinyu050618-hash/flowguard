# Why FlowGuard? Architecture, Boundaries & Trade-offs

> **FlowGuard** is a lightweight, zero-dependency async resilience engine and LLM reliability gateway for Python.
> This document clarifies FlowGuard's stable core, distributed extensions, streaming concurrency guarantees, adapter boundaries, non-goals, and how it compares to hand-rolling resilience components.

---

## 1. Architectural Scope & Boundaries

```text
+-------------------------------------------------------------------------+
|                           Application / Gateway                         |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                             FlowGuard Core                              |
|                                                                         |
|   [Rate Limiter]       [Bulkhead & Stream]  [Circuit Breaker]  [Retry Engine] |
|   TokenBucket &        Concurrency Slots    CLOSED / OPEN /     Exponential    |
|   SlidingWindow        & ResilientStream    HALF_OPEN Probes    Jitter Backoff |
|                                                                         |
|                       [Fallback Orchestrator]                           |
|                  Static Handler & ChoiceFallback (HITL)                 |
|                                                                         |
|                          [Telemetry Exporter]                           |
|                    P50/P95/P99 Latency & Prometheus                     |
+-------------------------------------------------------------------------+
                                     |
         +---------------------------+---------------------------+
         |                                                       |
         v                                                       v
+-----------------------------------+   +------------------------------------+
| Adapter Layer (LLM Normalization) |   | Distributed Backends (Optional)    |
| - ResilientOpenAI                 |   | - RedisTokenBucketLimiter          |
| - ResilientAnthropic              |   |   * Single-Round-Trip Lua (<1ms)   |
| - ResilientGemini                 |   |   * Automatic fallback_local       |
| - ResilientOpenAICompatible       |   |   * Atomic Token Refund Support    |
+-----------------------------------+   +------------------------------------+
         |
         v
  Official SDKs (AsyncOpenAI, AsyncAnthropic, Google GenAI, etc.)
```

### 1.1 Stable Core
FlowGuard's core provides pure asynchronous resilience primitives designed for high-concurrency Python applications:
- **Rate Limiting**: FIFO Token Bucket and Sliding Window rate limiters preventing busy-polling starvation and enforcing dual RPM/TPM budgets.
- **Bulkhead Isolation & Lifecycle Holding**: Semaphore-based active concurrency slots and bounded waiting queues with timeout protection. Crucially, the bulkhead integrates with `ResilientStream` to hold concurrency slots across the entire multi-second streaming lifespan.
- **Circuit Breaking**: Three-state machine (`CLOSED`, `OPEN`, `HALF_OPEN`) with concurrent probe limits and slot recovery on task cancellation.
- **Retry Orchestration**: Single-ownership exponential backoff with full, equal, and decorrelated jitter, plus permanent error classification.
- **Fallback Orchestration**: Static fallback functions and interactive human-in-the-loop `ChoiceFallback` with immutable, typed `FallbackContext`.
- **Telemetry**: Low-overhead rolling histogram (P50/P95/P99 latency) and Prometheus-compatible metrics exposition.

### 1.2 Streaming Resilience & Bulkhead Lifecycle Holding

Modern LLM gateways rely heavily on streaming responses (Server-Sent Events / Chunked Transfer Encoding). Traditional HTTP resilience models break down in streaming environments due to a fundamental mismatch between **request dispatch** and **response consumption**:

#### The "Instant Return vs. Long-Lived Stream" Concurrency Trap
In non-streaming RPCs, an async function executes, completes, and returns its result in milliseconds, after which its concurrency slot can be safely released.
In streaming LLM calls, returning the async generator/iterator takes only a few milliseconds, but streaming 500–2,000 tokens over SSE takes **10 to 60+ seconds**.
- **The Naive Failure Mode**: If a bulkhead semaphore slot is acquired upon calling the stream function and released immediately when the generator object is returned to FastAPI, the gateway loses all concurrency tracking. If 1,000 clients connect, 1,000 concurrent streaming connections flood the process, exhausting file descriptors, consuming excessive memory buffers, and triggering upstream LLM provider concurrency limit rejections (429 concurrency violations).
- **FlowGuard's Solution (`ResilientStream`)**:
  When `guard_stream` or `pipeline.execute_stream()` initializes a stream:
  1. The `Bulkhead` concurrency slot is acquired before contacting upstream.
  2. The acquired slot is transferred directly into the lifecycle of `ResilientStream`.
  3. The slot remains held across every `await stream.__anext__()` chunk iteration.
  4. The slot is deterministically released when the stream naturally completes (`StopAsyncIteration`), encounters an unrecoverable error, is cancelled by the client (`asyncio.CancelledError`), or is cleanly closed via `async with stream:` / `await stream.aclose()`.

```text
Client Request ──► [Bulkhead Acquire] ──► Upstream LLM Dispatch
                                                │
                                                ▼
                                    ResilientStream Created
                                                │ (Slot HELD)
               ┌────────────────────────────────┴────────────────────────────────┐
               ▼                                                                 ▼
   Chunk 1 ──► Chunk 2 ──► ... ──► Chunk N (StopIteration)        Client Disconnect (CancelledError)
               │                                                                 │
               └────────────────────────────────┬────────────────────────────────┘
                                                ▼
                                    [Bulkhead Release Slot]
                                 (Guaranteed Zero Slot Leak)
```

#### Mid-Stream Failure Isolation & Retries
Retrying a failed non-streaming request is safe because the client has not received partial data. However, **retrying a stream that has already yielded tokens to the client is catastrophic**: it causes duplicate text, corrupted JSON payloads, and broken UI state.
- `ResilientStream` distinguishes between **pre-flight failures** (before the first token is yielded) and **mid-stream failures** (after one or more chunks have been delivered).
- Pre-flight failures can safely trigger retry policies or fallback routes.
- Once the first chunk is delivered, any subsequent connection drop, read timeout, or upstream crash raises `MidStreamInterruptedError`, immediately halting the stream, recording metrics failure, and releasing the bulkhead slot without attempting duplicate retries.

---

### 1.3 Distributed Rate Limiting via Redis Token Bucket (<1ms Latency)

When scaling an API gateway horizontally across multiple workers or pods (`uvicorn --workers N`, Kubernetes pods), purely in-memory rate limiters cannot enforce a shared global budget. FlowGuard introduces `RedisTokenBucketLimiter` (`flowguard.backends.redis`), designed for maximum throughput, sub-millisecond execution, and high availability.

#### Sub-Millisecond Latency via Atomic Single-Round-Trip Lua
Standard distributed rate limiting approaches often introduce severe latency bottlenecks:
- **Naive GET/SET**: Reading current tokens, calculating refill in Python, and writing back requires 2 round-trips and is vulnerable to severe Time-Of-Check to Time-Of-Use (TOCTOU) race conditions under high concurrency.
- **Distributed Locking (Redlock)**: Acquiring and releasing distributed locks introduces 3–5 network round-trips, lock contention, and timeout risks, adding 10–50ms of latency per request.

FlowGuard solves this with a **single-round-trip atomic Lua script**:
1. In a single Redis command (`EVALSHA` / `EVAL`), the script passes the current monotonic/epoch timestamp, refill rate, burst capacity, and requested tokens.
2. The entire token replenishment equation is computed atomically directly within Redis memory:
   $$\text{tokens} = \min\left(\text{capacity}, \text{current\_tokens} + (\text{now} - \text{last\_refill}) \times \text{rate}\right)$$
3. If $\text{tokens} \ge \text{requested}$, it deducts tokens, updates `last_refill`, resets the key TTL, and returns success immediately.
4. If tokens are insufficient, it calculates the exact deficit wait time without mutating state.
5. **Execution Latency**: Because the entire operation executes in Redis memory in a single atomic cycle without distributed lock contention, network overhead is reduced to a single round-trip, executing in **sub-millisecond (< 0.5ms)** time over local network or VPC connections.

#### Three-State High Availability Degradation (`on_redis_error`)
In production gateways, a rate limiter must **never** become a single point of failure that brings down upstream traffic during a Redis blip or network partition. `RedisTokenBucketLimiter` provides three configurable degradation strategies:
- **`fallback_local` (Default & Recommended)**: If Redis becomes unreachable or times out, the limiter automatically and transparently switches to an in-process `TokenBucketLimiter`. An optional `local_capacity_ratio` partitions the capacity across local worker processes, ensuring continuous availability with zero request drops while logging warnings.
- **`fail_open`**: Allows requests through during outages when availability is paramount.
- **`fail_closed`**: Rejects requests during outages when strict API budget adherence cannot be compromised.

#### Token Refund Support for Prompt Caching & Speculative Execution
In LLM workloads, exact token consumption is often unknown until response completion. Furthermore, modern providers offer substantial discounts for Prompt Caching hits (e.g. 50–80% cost reduction).
- Callers can pre-allocate an estimated token budget before dispatch, and call `await limiter.refund(unused_tokens)` upon completion.
- `RedisTokenBucketLimiter` executes an atomic Lua refund script to replenish tokens back into the shared Redis bucket, immediately waking any queued waiters and preventing artificial rate limit throttling.

---

### 1.4 Adapter Layer
FlowGuard provides non-intrusive async adapters for popular LLM client libraries (`openai`, `anthropic`, `google-genai`, and OpenAI-compatible gateways like DeepSeek, vLLM, Ollama):
- The adapter layer **only** normalizes third-party SDK calls to FlowGuard's unified resilience semantics (RPM/TPM accounting, error mapping, and retry configuration).
- Adapters enforce **Sole Retry Ownership**: they disable underlying SDK auto-retries (via derived `with_options(max_retries=0)`) so that total physical attempts strictly match metered budget limits.

### 1.5 Non-Goals (What FlowGuard Explicitly Avoids)
- **Not an LLM Provider SDK**: FlowGuard does not invent its own prompt schemas, tool calling protocols, or message formats. It wraps official SDKs.
- **Not an Agent / Workflow Framework**: FlowGuard is not LangChain, LlamaIndex, or CrewAI. It does not manage agent state graphs or memory.
- **Not a Caching Database**: FlowGuard does not ship embedded key-value stores or vector caches.
- **Not a Heavy Service Mesh**: FlowGuard avoids heavy sidecars and complex distributed control planes (like Envoy xDS or Consul). It provides lightweight, zero-dependency async primitives and an optional sub-millisecond Redis backend that drops directly into ASGI/FastAPI applications.
- **No Provider Expansion for the Sake of It**: FlowGuard focuses on rock-solid core semantics rather than maintaining dozens of niche provider integrations.

---

## 2. When to Use (and When NOT to Use) FlowGuard

### ✅ Use FlowGuard when:
1. **Building Multi-LLM Gateways or Microservices**: You orchestrate multiple model providers (e.g. OpenAI, Anthropic, Gemini, DeepSeek, vLLM) and need consistent rate limits, timeouts, and fallback routing.
2. **Serving High-Concurrency Streaming Endpoints**: You serve SSE streaming LLM responses and need deterministic Bulkhead lifecycle holding to prevent concurrency blowout and socket leaks upon client disconnect.
3. **Deploying Multi-Worker / Multi-Node API Gateways**: You deploy FastAPI across multiple processes or containers (`uvicorn --workers`, Kubernetes) and require a sub-millisecond shared Redis rate limiter with automatic local fallback during Redis outages.
4. **Requiring Dynamic Human-in-the-Loop Fallback**: When a primary high-tier model experiences an outage, you want to prompt a human operator, UI modal, or agent decision engine (`ChoiceFallback`) instead of blindly failing or routing arbitrarily.
5. **Preventing Cost & Concurrency Stampedes**: You need strict RPM and TPM token budgeting to avoid unexpected 429 quota exhaustion and massive API billing spikes.
6. **Demanding Zero-Leak Async Cancellation**: When web requests (e.g. FastAPI / Starlette) are aborted by clients, in-flight bulkhead concurrency slots, waiting queues, and circuit breaker half-open probe gates are immediately released without task or slot leaks.

### ❌ Do NOT use FlowGuard when:
1. **Single-SDK, Low-Concurrency Scripts**: If you are writing a simple batch script or hobby project with a single model provider at low volume, the official SDK's built-in retry mechanism (`max_retries=2`) is simpler and sufficient.
2. **Synchronous-Only Codebases**: FlowGuard is built ground-up on Python `asyncio`. Synchronous codebases should use thread-based tools like `tenacity` or `pybreaker`.

---

## 3. Comparison Matrix: FlowGuard vs. Hand-Rolled Components

Developers often attempt to assemble resilience by gluing together independent libraries (e.g., `tenacity` for retries, `aiolimiter` for rate limiting, custom Redis scripts, and ad-hoc `try/except` for fallback). The table below compares the engineering characteristics:

| Dimension | Hand-Rolled Ad-Hoc Assembly | FlowGuard Engine |
| :--- | :--- | :--- |
| **Streaming Concurrency (Bulkhead Lifecycle)** | Semaphores released as soon as generator returns; streaming connections accumulate unchecked, causing connection exhaustion and upstream 429s. | **Full-Stream Lifecycle Holding**: `ResilientStream` preserves Bulkhead concurrency slots across the entire multi-second streaming duration, releasing cleanly on completion, error, or client cancellation. |
| **Mid-Stream Failure Handling** | Generic retry decorators catch stream mid-flight exceptions and blindly re-execute, sending duplicate prefix text to end clients. | **Idempotent Mid-Stream Guard**: Distinguishes pre-flight vs mid-stream errors; raises `MidStreamInterruptedError` to halt corrupted streams without illegal duplicate token playback. |
| **Distributed Rate Limiting (Redis)** | Ad-hoc Python GET/SET scripts suffer TOCTOU race conditions; distributed locks (Redlock) add 10–50ms latency. Redis disconnect crashes the gateway. | **Sub-Millisecond Lua + Fallback**: Atomic single-round-trip Lua script (< 0.5ms) with zero lock contention. Automatic `fallback_local` transparently degrades to local TokenBucket during Redis downtime. |
| **Dual RPM & TPM Budgeting** | Separate limiters required; no unified accounting across retried attempts. | **Built-in Dual Budgeting**: Automatically meters request count (RPM) and token volume (TPM) before each execution attempt. |
| **Retry Ownership** | With 2 SDK retries inside 3 outer retries, exhausting both policies can send up to 12 upstream calls (3 SDK attempts × 4 outer attempts). | The OpenAI and Anthropic adapters use client copies with `max_retries=0`, so FlowGuard controls retries and reserves quota per attempt. |
| **Cancellation Safety** | Cancelling an async task waiting on `tenacity.retry` or custom locks can orphan concurrency slots or block waiting queues indefinitely. | **Zero-Leak Slot Recovery**: `asyncio.CancelledError` immediately unwinds queue waiters, bulkhead active slots, and half-open probe gates (no orphaned tasks or deadlocked slots). |
| **Half-Open Probe Stampede** | Simple breakers let all queued requests flood upstream when transitioning from `OPEN` to `HALF_OPEN`. | **Throttled Probes**: Bounded concurrent probe slots (`half_open_max_probes`) ensure upstream recovery without stampedes. |
| **Fallback Context Decoupling** | Fallback functions often suffer from `*args, **kwargs` keyword collisions (e.g. caller parameter named `exc` colliding with exception payload). | **Typed `FallbackContext`**: Immutable context wrapping `(exception, args, kwargs, pipeline_name)` with `MappingProxyType`, completely immune to keyword collisions. |
| **Candidate Execution Safety** | Using `try...except TypeError:` to adapt dynamic fallback candidate signatures risks catching internal business bugs and executing candidate twice. | **Static Signature Inspection**: Reflects signatures outside execution blocks; sync and async candidate exceptions execute strictly once. |
| **Telemetry Consistency** | Metrics must be manually emitted at every try/catch and retry hook, leading to inconsistent dashboard labels. | **Standardized Telemetry**: Automatic P50/P95/P99 latency histogram tracking and Prometheus text exporter format out-of-the-box. |
| **Dependency & Maintenance** | 4–6 separate external libraries with differing async semantics, maintenance cycles, and potential version conflicts. | **Zero Core External Dependencies**: Pure standard-library async Python (`asyncio`, `dataclasses`, `typing`, `time`), with optional lightweight extras (`redis`, `opentelemetry`). |

---

## 4. Summary

FlowGuard does not aim to replace model APIs or agent frameworks. Its sole purpose is to provide **predictable, uncompromised resilience, clean parameter contracts, and deterministic failure recovery** for modern async Python applications, whether running in a single process or scaled across distributed multi-worker API gateways.
