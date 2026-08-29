<div align="center">

# 🛡️ FlowGuard

**High-Performance Async Rate Limiting, Circuit Breaking & Resilience Orchestration for LLM & API Pipelines.**

[![CI Status](https://github.com/yangjinyu050618-hash/flowguard/actions/workflows/ci.yml/badge.svg)](https://github.com/yangjinyu050618-hash/flowguard/actions)
[![PyPI version](https://img.shields.io/pypi/v/flowguard-core.svg)](https://pypi.org/project/flowguard-core/)
[![Python Versions](https://img.shields.io/pypi/pyversions/flowguard-core.svg)](https://pypi.org/project/flowguard-core/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type Checked: Mypy](https://img.shields.io/badge/type_checked-mypy-blue.svg)](https://github.com/python/mypy)

[Features](#-key-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Adapters](#-ecosystem-adapters) • [Telemetry](#-telemetry--metrics) • [Examples](#-runnable-examples)

</div>

---

## 📖 Overview

**FlowGuard** (`flowguard-core`) is a lightweight, zero-dependency asynchronous Python resilience engine tailored for AI workloads, high-concurrency microservices, and multi-LLM API gateways.

> 🔍 **Deep Dive**: See [Why FlowGuard? (Architecture, Boundaries & Comparison Matrix)](docs/WHY_FLOWGUARD.md) for a detailed breakdown of FlowGuard's stable core, non-goals, and how it compares with hand-rolling independent libraries.

When dealing with third-party LLM providers, rate limits (RPM / TPM), transient server hiccups (HTTP 429 / 503), and unpredictable downstream latency often degrade application availability. FlowGuard combines **strict FIFO token-bucket rate limiting**, **gated circuit breaking**, **jittered exponential backoff**, and **bulkhead resource partitioning** into a single composable pipeline.

---

## ✨ Key Features

- ⚡ **Strict FIFO Token-Bucket & Sliding-Window Limiters**: Exact Future-based FIFO queuing guaranteeing zero starvation and eliminating busy-polling.
- 🔌 **Probe-Gated Circuit Breaker**: State-machine driven (`CLOSED` → `OPEN` → `HALF_OPEN`) with explicit probe concurrency limits preventing half-open stampedes and safe cancellation slot cleanup.
- 🔁 **Smart Exponential Backoff & Jitter**: Full Jitter, Equal Jitter, and Decorrelated Jitter algorithms preventing thundering herds.
- 🧱 **Bulkhead Isolation**: Asynchronous concurrency gates preventing cascaded resource exhaustion.
- 🤖 **LLM Native Adapters**: RPM & TPM rate limiting with per-attempt token reservation for OpenAI, Anthropic Claude, and Google Gemini.
- 🔀 **Fallback & Human-in-the-Loop Routing**: Graceful degradation with automatic fallback or `ChoiceFallback` interactive multi-model selection.
- 📊 **Telemetry & Exporters**: P50/P95/P99 latency histogram tracker with Prometheus text format & JSON export with sanitized label escaping.
- 🪶 **True Zero Dependencies**: Pure Python asyncio standard library core (`PEP 561` typed).

---

## 🏗️ Architecture

```text
Incoming Task -> [ Retry Loop (outer) ]
                      │
                      ▼
         ┌─────────────────────────┐
         │ Token Bucket / TPM Gate │ (Per-attempt Token Reservation)
         └────────────┬────────────┘
                      │
                      ▼
         ┌─────────────────────────┐
         │    Bulkhead Barrier     │ (Concurrency Isolation)
         └────────────┬────────────┘
                      │
                      ▼
         ┌─────────────────────────┐
         │     Circuit Breaker     │ (Probe-Gated Fast Fail)
         └────────────┬────────────┘
                      │
                      ▼
           Target Downstream Service
                      │ (on permanent failure / circuit open)
                      ▼
         ┌─────────────────────────┐
         │ Fallback / ChoiceRouter │ (Human-in-the-loop / Auto-Failover)
         └─────────────────────────┘
```

---

## 🚀 Quick Start

### Installation

```bash
pip install flowguard-core
```

The core package has zero runtime dependencies. Install only the provider SDKs
used by your application when creating clients for the native adapters:

```bash
pip install openai anthropic google-genai
```

FlowGuard accepts preconfigured SDK clients, so provider credentials and client
settings remain under application control.

*Or install from source:*

```bash
git clone https://github.com/yangjinyu050618-hash/flowguard.git
cd flowguard
pip install -e .
```

### 1. One-Line Decorator `@guard` with Fallback

```python
import asyncio
from flowguard import guard

async def fallback_report(prompt: str, exc: Exception = None) -> str:
    return f"[FALLBACK] Cached report for: {prompt}"

@guard(
    name="llm-caller",
    rate_per_sec=20.0,       # Max 20 calls/sec
    burst_capacity=30.0,     # Allow burst up to 30 calls
    max_retries=3,           # Auto-retry up to 3 times on transient errors
    failure_threshold=5,     # Trip circuit breaker after 5 consecutive failures
    recovery_timeout=15.0,   # Wait 15s before probe in HALF_OPEN state
    max_concurrent=10,       # Max 10 concurrent requests (Bulkhead)
    fallback=fallback_report,# Graceful degradation handler
)
async def fetch_completion(prompt: str) -> str:
    return f"Response to {prompt}"

async def main():
    result = await fetch_completion("Hello FlowGuard!")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🤖 Ecosystem Adapters & Multi-LLM Orchestration

FlowGuard provides native async adapters for OpenAI, Anthropic Claude, and Google Gemini:

```python
from flowguard.adapters import ResilientOpenAI, ResilientAnthropic, ResilientGemini

openai_client = ResilientOpenAI(raw_openai, rpm_limit=500.0, tpm_limit=60_000.0)
claude_client = ResilientAnthropic(raw_anthropic, rpm_limit=300.0, tpm_limit=40_000.0)
gemini_client = ResilientGemini(raw_gemini, rpm_limit=300.0, tpm_limit=60_000.0)
```

> **Note on Retry Ownership**: FlowGuard manages end-to-end retry policies, backoff, and RPM/TPM token reservation as the sole retry owner. Underlying SDK retries are automatically disabled (`max_retries=0` via `with_options` for OpenAI & Anthropic; keep default `http_options` for Google GenAI) so physical requests match metered attempts.

---

## 🔀 Interactive Human-in-the-Loop Fallback (`ChoiceFallback`)

When a primary model trips or fails, `ChoiceFallback` prompts the user or decision engine to dynamically select a replacement model:

```python
from flowguard import guard, ChoiceFallback, FallbackContext

async def ask_user_for_model(context: FallbackContext, available_options: list[str]) -> str:
    # Query CLI prompt, Web UI modal, or agent decision engine
    print(f"Primary model failed: {context.exception}. Choose fallback from {available_options}:")
    return "deepseek-r1"

router = ChoiceFallback(
    candidates={
        "claude-3.5-sonnet": call_claude,
        "deepseek-r1": call_deepseek,
        "gemini-2.5-flash": call_gemini,
    },
    selector=ask_user_for_model,
)

@guard(name="gpt-primary", failure_threshold=2, fallback=router)
async def ask_gpt5(prompt: str) -> str: ...
```

---

## 📊 Telemetry & Metrics

FlowGuard captures execution telemetry and latency percentiles with zero external dependencies:

```python
from flowguard import FlowGuard, TokenBucketLimiter
from flowguard.metrics import export_json, export_prometheus

pipeline = FlowGuard(name="payment-gateway", limiter=TokenBucketLimiter(10, 20))

# Export as JSON
print(export_json(pipeline.metrics))

# Export in Prometheus format
print(export_prometheus(pipeline.metrics))
```

---

## 📚 Runnable Examples

Explore ready-to-run code examples in the [`examples/`](examples/) directory:

- [`examples/01_quickstart_guard.py`](examples/01_quickstart_guard.py) — One-line `@guard` decorator protecting async functions.
- [`examples/02_openai_resilient_chat.py`](examples/02_openai_resilient_chat.py) — OpenAI chat completions with dual RPM & TPM budget throttling.
- [`examples/03_fastapi_integration.py`](examples/03_fastapi_integration.py) — FastAPI microservice integration & Prometheus exposition.
- [`examples/04_fallback_graceful_degradation.py`](examples/04_fallback_graceful_degradation.py) — Graceful degradation when circuit breaker trips.
- [`examples/05_multi_llm_orchestration.py`](examples/05_multi_llm_orchestration.py) — Multi-LLM resilience across OpenAI, Anthropic Claude & Google Gemini.
- [`examples/06_interactive_human_in_the_loop_fallback.py`](examples/06_interactive_human_in_the_loop_fallback.py) — Interactive decision-driven model failover.

Run any example directly:
```bash
python examples/01_quickstart_guard.py
python examples/02_openai_resilient_chat.py
python examples/03_fastapi_integration.py
python examples/04_fallback_graceful_degradation.py
python examples/05_multi_llm_orchestration.py
python examples/06_interactive_human_in_the_loop_fallback.py
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
