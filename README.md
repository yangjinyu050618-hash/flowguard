<div align="center">

# 🛡️ FlowGuard

**High-Performance Async Rate Limiting, Circuit Breaking & Resilience Orchestration for LLM & API Pipelines.**

[![CI Status](https://github.com/yangjinyu050618-hash/flowguard/actions/workflows/ci.yml/badge.svg)](https://github.com/yangjinyu050618-hash/flowguard/actions)
[![PyPI version](https://img.shields.io/pypi/v/flowguard-core.svg)](https://pypi.org/project/flowguard-core/)
[![Python Versions](https://img.shields.io/pypi/pyversions/flowguard-core.svg)](https://pypi.org/project/flowguard-core/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type Checked: Mypy](https://img.shields.io/badge/type_checked-mypy-blue.svg)](https://github.com/python/mypy)

[Features](#-key-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Adapters](#-ecosystem-adapters) • [Telemetry](#-telemetry--metrics) • [Contributing](#-contributing)

</div>

---

## 📖 Overview

**FlowGuard** (`flowguard-core`) is a zero-overhead, production-grade Python async resilience framework tailored for modern AI workloads, high-concurrency Microservices, and LLM API orchestrations (e.g. OpenAI, Anthropic, Gemini, DeepSeek).

When dealing with third-party LLM providers, rate limits (RPM / TPM), transient server hiccups (HTTP 429 / 503), and unpredictable downstream latency often degrade application availability. FlowGuard combines **strict FIFO token-bucket rate limiting**, **gated circuit breaking**, **jittered exponential backoff**, and **bulkhead resource partitioning** into a single composable pipeline.

---

## ✨ Key Features

- ⚡ **Strict FIFO Token-Bucket & Sliding-Window Limiters**: Exact Future-based FIFO queuing guaranteeing zero starvation and zero polling overhead.
- 🔌 **Probe-Gated Circuit Breaker**: State-machine driven (`CLOSED` → `OPEN` → `HALF_OPEN`) with explicit probe concurrency limits preventing half-open stampedes.
- 🔁 **Smart Exponential Backoff & Jitter**: Full Jitter, Equal Jitter, and Decorrelated Jitter algorithms preventing thundering herds.
- 🧱 **Bulkhead Isolation**: Asynchronous concurrency gates preventing cascaded resource exhaustion.
- 🤖 **LLM Native Adapters**: RPM & TPM rate limiting with timeout guards for OpenAI and HTTPX clients.
- 📊 **Telemetry & Exporters**: P50/P95/P99 latency histogram tracker with Prometheus text format & JSON export.
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
```

---

## 🚀 Quick Start

### Installation

```bash
pip install flowguard-core
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

## 📄 License

This project is licensed under the [MIT License](LICENSE).
