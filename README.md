<div align="center">

# 🛡️ FlowGuard

**High-Performance Async Rate Limiting, Circuit Breaking & Resilience Orchestration for LLM & API Pipelines.**

[![CI Status](https://github.com/yangjinyu050618-hash/flowguard/actions/workflows/ci.yml/badge.svg)](https://github.com/yangjinyu050618-hash/flowguard/actions)
[![PyPI version](https://img.shields.io/badge/pypi-v0.2.1-blue.svg)](https://pypi.org/project/flowguard/)
[![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/flowguard/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type Checked: Mypy](https://img.shields.io/badge/type_checked-mypy-blue.svg)](https://github.com/python/mypy)

[Features](#-key-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Adapters](#-ecosystem-adapters) • [Telemetry](#-telemetry--metrics) • [Contributing](#-contributing)

</div>

---

## 📖 Overview

**FlowGuard** is a zero-overhead, production-grade Python async resilience framework tailored for modern AI workloads, high-concurrency Microservices, and LLM API orchestrations (e.g. OpenAI, Anthropic, Gemini, DeepSeek).

When dealing with third-party LLM providers, rate limits (RPM / TPM), transient server hiccups (HTTP 429 / 503), and unpredictable downstream latency often degrade application availability. FlowGuard combines **adaptive token-bucket rate limiting**, **sliding-window circuit breaking**, **jittered exponential backoff**, and **bulkhead resource partitioning** into a single composable pipeline.

---

## ✨ Key Features

- ⚡ **Token-Bucket & Sliding-Window Rate Limiters**: Precise non-blocking token refill with sub-millisecond precision and burst capacity support.
- 🔌 **Sliding-Window Circuit Breaker**: State-machine driven (`CLOSED` → `OPEN` → `HALF_OPEN`) with configurable recovery cooldown and probe verification.
- 🔁 **Smart Exponential Backoff & Jitter**: Full Jitter and Equal Jitter algorithms (following AWS architectural recommendations) preventing thundering herds.
- 🧱 **Bulkhead Isolation**: Asynchronous semaphore-based concurrency gates preventing cascaded resource exhaustion.
- 🤖 **LLM Native Adapters**: Built-in RPM/TPM estimation and automatic token replenishment for OpenAI / HTTPX clients.
- 📊 **Telemetry & Exporters**: In-memory P50/P95/P99 latency histogram tracker with Prometheus text format & JSON export.
- 🪶 **Zero Heavy Dependencies**: Pure Python asyncio core with full type annotations (`PEP 561` typed).

---

## 🏗️ Architecture

```text
Incoming Async Task / LLM Call
              │
              ▼
   ┌──────────────────────┐
   │  Token Bucket / TPM  │ ──► [Rate Limit Exhausted? -> Sleep / Timeout]
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │   Bulkhead Barrier   │ ──► [Concurrency Full? -> Queue / Reject]
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │    Circuit Breaker   │ ──► [State OPEN? -> Fast Fail]
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │ Exponential Backoff  │ ──► [Transient Error? -> Retry with Jitter]
   └──────────┬───────────┘
              │
              ▼
    Target Service / LLM API
```

---

## 🚀 Quick Start

### Installation

```bash
pip install flowguard
```

*Or install from source:*

```bash
git clone https://github.com/yangjinyu050618-hash/flowguard.git
cd flowguard
pip install -e .
```

### 1. One-Line Decorator `@guard`

```python
import asyncio
from flowguard import guard

@guard(
    name="llm-caller",
    rate_per_sec=20.0,       # Max 20 calls/sec
    burst_capacity=30.0,     # Allow burst up to 30 calls
    max_retries=3,           # Auto-retry up to 3 times on transient errors
    failure_threshold=5,     # Trip circuit breaker after 5 consecutive failures
    recovery_timeout=15.0,   # Wait 15s before probe in HALF_OPEN state
    max_concurrent=10,       # Max 10 concurrent requests (Bulkhead)
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

### 2. Standalone Token-Bucket Rate Limiter

```python
import asyncio
from flowguard import TokenBucketLimiter

async def worker():
    # 50 tokens per second, max burst capacity of 100
    limiter = TokenBucketLimiter(rate=50.0, capacity=100.0)

    # Acquire 1 token
    await limiter.acquire(tokens=1.0)
    
    # Or acquire with timeout
    try:
        await limiter.acquire(tokens=10.0, timeout=0.5)
        print("Acquired 10 tokens successfully!")
    except Exception as e:
        print("Rate limit timeout exceeded")

asyncio.run(worker())
```

---

### 3. Circuit Breaker with State Transition Callbacks

```python
from flowguard import CircuitBreaker, CircuitState

def on_state_change(old_state: CircuitState, new_state: CircuitState):
    print(f"[ALERT] Circuit transition: {old_state.value} -> {new_state.value}")

breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=10.0,
    half_open_success_threshold=2,
    on_state_change=on_state_change,
)
```

---

## 🤖 Ecosystem Adapters

### OpenAI Client Throttling & Protection

```python
from openai import AsyncOpenAI
from flowguard.adapters import ResilientOpenAI

client = AsyncOpenAI(api_key="sk-...")

# Wrap client with 500 RPM and 100,000 TPM limit
resilient_client = ResilientOpenAI(
    client=client,
    rpm_limit=500.0,
    tpm_limit=100_000.0,
    max_retries=4,
)

async def run_chat():
    response = await resilient_client.create_chat_completion(
        estimated_tokens=800,
        model="gpt-4o",
        messages=[{"role": "user", "content": "Explain quantum computing in 3 sentences."}]
    )
    print(response.choices[0].message.content)
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

## 💻 CLI Tools

FlowGuard provides a built-in CLI for synthetic benchmarking and health checks:

```bash
# Run a synthetic rate-limiting benchmark
flowguard benchmark --rate 50 --total 200 --concurrency 20
```

---

## 🧪 Running Tests

```bash
# Run pytest suite
python -m pytest

# Run with coverage
python -m pytest --cov=flowguard --cov-report=term-missing
```

---

## 🗺️ Roadmap

- [x] High precision Token Bucket and Sliding Window Log rate limiters
- [x] Sliding-window Circuit Breaker with Half-Open probe validation
- [x] Exponential backoff with AWS Full/Equal Jitter
- [x] OpenAI AsyncClient & HTTPX drop-in adapters
- [x] Prometheus & JSON telemetry exporters
- [ ] Redis distributed rate limiter backend (v0.3.0)
- [ ] Adaptive AI token quota estimation based on prompt size (v0.3.0)
- [ ] OpenTelemetry span auto-injection (v0.4.0)

---

## 🤝 Contributing

Contributions are welcomed! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on code standards, testing, and pull request procedures.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
