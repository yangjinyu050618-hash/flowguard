# Show HN / Reddit: FlowGuard – Zero-dependency async resilience engine & LLM gateway for Python

**GitHub**: https://github.com/yangjinyu050618-hash/flowguard  
**PyPI**: `pip install flowguard-core`  
**License**: MIT  

Hey everyone!

When building multi-LLM gateways and high-concurrency AI agents in async Python (FastAPI / asyncio), we noticed several subtle but painful production pitfalls when trying to piece together generic resilience libraries (`tenacity`, `aiolimiter`, ad-hoc circuit breakers):

1. **RPM vs. TPM Rate Limiting**: Generic limiters count requests (RPM) but ignore prompt token volume (TPM), causing unexpected 429 quota exhaustion.
2. **Hidden Retry Amplification**: Wrapping official SDKs with Tenacity often results in `2 SDK retries × 3 outer retries = 6 physical calls`, amplifying provider outages and doubling API bills.
3. **Async Cancellation Leaks**: When frontend users abort streaming requests, generic locks can orphan in-flight concurrency slots or deadlock half-open probe gates.

To solve this cleanly, we built **FlowGuard** (`flowguard-core`): a pure, zero-dependency async resilience engine tailored for Python and LLM pipelines.

---

### Key Capabilities:
- ⚡ **Dual Token-Bucket Rate Limiting**: Strict FIFO queuing enforcing both RPM and TPM token budgets without busy-polling.
- 🔁 **Sole Retry Ownership**: Automatically derives `max_retries=0` client copies for OpenAI, Anthropic, and Gemini adapters so every physical attempt is accounted for and metered exactly once.
- 🔌 **Probe-Gated Circuit Breaker**: Controlled `CLOSED` → `OPEN` → `HALF_OPEN` state transitions with bounded probe slots.
- 🔀 **Dynamic Multi-Model Fallback (`ChoiceFallback`)**: Auto-route to backup models (e.g. Claude 3.5 Sonnet or Gemini) on primary failure or query human-in-the-loop decisions.
- 🧹 **Zero-Leak Slot Recovery**: `asyncio.CancelledError` unwinds waiting queues and bulkhead slots immediately.
- 📊 **Built-in Prometheus Telemetry**: P50/P95/P99 latency histogram exporter out-of-the-box.
- 🪶 **Zero External Dependencies**: Pure standard library (`asyncio`, `typing`, `dataclasses`, `time`).

---

### Minimal Quick Start:

```python
import asyncio
from flowguard import guard

@guard(
    name="llm-gateway",
    rate_per_sec=20.0,
    burst_capacity=30.0,
    max_retries=2,
    failure_threshold=3,
    recovery_timeout=5.0,
    fallback=lambda prompt, exc=None: f"[Fallback] Cached: {prompt}"
)
async def ask_model(prompt: str) -> str:
    # Your model invocation
    return f"Response to: {prompt}"

asyncio.run(ask_model("Hello World"))
```

---

### Tests & Engineering Rigor:
- **92.24% Code Coverage** (90 automated tests).
- Verified on **Python 3.9–3.13 across Linux, macOS, and Windows** in GitHub Actions.
- Full architectural breakdown and 8-dimension comparison matrix available in [docs/WHY_FLOWGUARD.md](https://github.com/yangjinyu050618-hash/flowguard/blob/main/docs/WHY_FLOWGUARD.md).

Feedback, use-case questions, and contributions are very welcome!
