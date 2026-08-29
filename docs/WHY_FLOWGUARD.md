# Why FlowGuard? Architecture, Boundaries & Trade-offs

> **FlowGuard** is a lightweight, zero-dependency async resilience engine and LLM reliability gateway for Python.
> This document clarifies FlowGuard's stable core, adapter boundaries, non-goals, and how it compares to hand-rolling resilience components.

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
|   [Rate Limiter]       [Bulkhead]       [Circuit Breaker]  [Retry Engine] |
|   TokenBucket &       Concurrency      CLOSED / OPEN /     Exponential    |
|   SlidingWindow       & Queue Slots    HALF_OPEN Probes    Jitter Backoff |
|                                                                         |
|                       [Fallback Orchestrator]                           |
|                  Static Handler & ChoiceFallback (HITL)                 |
|                                                                         |
|                          [Telemetry Exporter]                           |
|                    P50/P95/P99 Latency & Prometheus                     |
+-------------------------------------------------------------------------+
                                     |
               +---------------------+---------------------+
               |                     |                     |
               v                     v                     v
     [ResilientOpenAI]     [ResilientAnthropic]    [ResilientGemini]
       (Adapter Layer)       (Adapter Layer)        (Adapter Layer)
               |                     |                     |
               v                     v                     v
        Official SDK           Official SDK          Official SDK
        AsyncOpenAI           AsyncAnthropic         Google GenAI
```

### 1.1 Stable Core
FlowGuard's core provides pure asynchronous resilience primitives designed for high-concurrency Python applications:
- **Rate Limiting**: FIFO Token Bucket and Sliding Window rate limiters preventing busy-polling starvation and enforcing dual RPM/TPM budgets.
- **Circuit Breaking**: Three-state machine (`CLOSED`, `OPEN`, `HALF_OPEN`) with concurrent probe limits and slot recovery on task cancellation.
- **Retry Orchestration**: Single-ownership exponential backoff with full, equal, and decorrelated jitter, plus permanent error classification.
- **Bulkhead Isolation**: Semaphore-based active concurrency slots and bounded waiting queues with timeout protection.
- **Fallback Orchestration**: Static fallback functions and interactive human-in-the-loop `ChoiceFallback` with immutable, typed `FallbackContext`.
- **Telemetry**: Low-overhead rolling histogram (P50/P95/P99 latency) and Prometheus-compatible metrics exposition.

### 1.2 Adapter Layer
FlowGuard provides non-intrusive async adapters for popular LLM client libraries (`openai`, `anthropic`, `google-genai`).
- The adapter layer **only** normalizes third-party SDK calls to FlowGuard's unified resilience semantics (RPM/TPM accounting, error mapping, and retry configuration).
- Adapters enforce **Sole Retry Ownership**: they disable underlying SDK auto-retries (via derived `with_options(max_retries=0)`) so that total physical attempts strictly match metered budget limits.

### 1.3 Non-Goals (What FlowGuard Explicitly Avoids)
- **Not an LLM Provider SDK**: FlowGuard does not invent its own prompt schemas, tool calling protocols, or message formats. It wraps official SDKs.
- **Not an Agent / Workflow Framework**: FlowGuard is not LangChain, LlamaIndex, or CrewAI. It does not manage agent state graphs or memory.
- **Not a Caching Database**: FlowGuard does not ship embedded key-value stores or vector caches.
- **Not a Distributed Control Plane**: FlowGuard is an in-process, zero-network, zero-dependency async engine. Distributed rate limiting across clusters should use Redis or Envoy.
- **No Provider Expansion for the Sake of It**: FlowGuard focuses on rock-solid core semantics rather than maintaining dozens of niche provider integrations.

---

## 2. When to Use (and When NOT to Use) FlowGuard

### ✅ Use FlowGuard when:
1. **Building Multi-LLM Gateways or Microservices**: You orchestrate multiple model providers (e.g. OpenAI, Anthropic, Gemini, DeepSeek) and need consistent rate limits, timeouts, and fallback routing.
2. **Requiring Dynamic Human-in-the-Loop Fallback**: When a primary high-tier model experiences an outage, you want to prompt a human operator, UI modal, or agent decision engine (`ChoiceFallback`) instead of blindly failing or routing arbitrarily.
3. **Preventing Cost & Concurrency Stampedes**: You need strict RPM and TPM token budgeting to avoid unexpected 429 quota exhaustion and massive API billing spikes.
4. **Demanding Zero-Leak Async Cancellation**: When web requests (e.g. FastAPI / Starlette) are aborted by clients, in-flight rate limiter tokens, bulkhead slots, and circuit breaker half-open probes must be immediately released without resource leaks.

### ❌ Do NOT use FlowGuard when:
1. **Single-SDK, Low-Concurrency Scripts**: If you are writing a simple batch script or hobby project with a single model provider at low volume, the official SDK's built-in retry mechanism (`max_retries=2`) is simpler and sufficient.
2. **Synchronous-Only Codebases**: FlowGuard is built ground-up on Python `asyncio`. Synchronous codebases should use thread-based tools like `tenacity` or `pybreaker`.
3. **Distributed Multi-Node Rate Limiting**: If multiple API gateway worker nodes must share a global distributed rate limit across clusters without local budgeting, consider a dedicated infrastructure proxy like Redis RateLimiter, Kong, or Envoy.

---

## 3. Comparison Matrix: FlowGuard vs. Hand-Rolled Components

Developers often attempt to assemble resilience by gluing together independent libraries (e.g., `tenacity` for retries, `aiolimiter` for rate limiting, and ad-hoc `try/except` for fallback). The table below compares the engineering characteristics:

| Dimension | Hand-Rolled Ad-Hoc Assembly | FlowGuard Engine |
| :--- | :--- | :--- |
| **Dual RPM & TPM Budgeting** | Separate limiters required; no unified accounting across retried attempts. | **Built-in Dual Budgeting**: Automatically meters request count (RPM) and token volume (TPM) before each execution attempt. |
| **Retry Ownership** | SDKs perform hidden internal retries (e.g. 2 retries) while outer retry loops also retry (e.g. 3 retries), causing **2 x 3 = 6 physical requests** and unexpected quota burn. | **Sole Retry Ownership**: Automatically derives `max_retries=0` client copies; every attempt is accounted for and metered exactly once. |
| **Cancellation Safety** | Cancelling a task waiting on `tenacity.retry` or custom locks can orphan queue slots or leave rate limiter tokens permanently drained. | **Zero-Leak Cancellation**: `asyncio.CancelledError` immediately unwinds token bucket waiters, bulkhead active/queue counters, and half-open probe slots. |
| **Half-Open Probe Stampede** | Simple breakers let all queued requests flood upstream when transitioning from `OPEN` to `HALF_OPEN`. | **Throttled Probes**: Bounded concurrent probe slots (`half_open_max_probes`) ensure upstream recovery without stampedes. |
| **Fallback Context Decoupling** | Fallback functions often suffer from `*args, **kwargs` keyword collisions (e.g. caller parameter named `exc` colliding with exception payload). | **Typed `FallbackContext`**: Immutable context wrapping `(exception, args, kwargs, pipeline_name)` with `MappingProxyType`, completely immune to keyword collisions. |
| **Candidate Execution Safety** | Using `try...except TypeError:` to adapt dynamic fallback candidate signatures risks catching internal business bugs and executing candidate twice. | **Static Signature Inspection**: Reflects signatures outside execution blocks; sync and async candidate exceptions execute strictly once. |
| **Telemetry Consistency** | Metrics must be manually emitted at every try/catch and retry hook, leading to inconsistent dashboard labels. | **Standardized Telemetry**: Automatic P50/P95/P99 latency histogram tracking and Prometheus text exporter format out-of-the-box. |
| **Dependency & Maintenance** | 4–6 separate external libraries with differing async semantics, maintenance cycles, and potential version conflicts. | **Zero External Dependencies**: Pure standard-library async Python (`asyncio`, `dataclasses`, `typing`, `time`), zero bloat. |

---

## 4. Summary

FlowGuard does not aim to replace model APIs or agent frameworks. Its sole purpose is to provide **predictable, uncompromised resilience, clean parameter contracts, and deterministic failure recovery** for modern async Python applications.
