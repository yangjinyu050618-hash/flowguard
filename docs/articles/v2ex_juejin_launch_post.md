# [开源分享] 写了个纯原生 asyncio 的 LLM 网关弹性治理库 FlowGuard：解决 429、重试重复扣费与客户端断连泄漏

各位 V 友 / 掘友大家好，

最近在做高并发 LLM 聚合服务和 AI Agent 工作流时，遇到过几个大模型场景特有的生产痛点：

### 1. 为什么手搓 Resilience（Tenacity + 自带限流）容易踩坑？
- **429 与 TPM 击穿**：普通的限流器只按请求次数（RPM）限流，但大模型最容易超标的是 Token 吞吐量（TPM）。单次 Prompt 稍微长一点，RPM 没超但 TPM 瞬间被上游封禁 429。
- **重试放大与 Token 账单翻倍**：官方 SDK（如 OpenAI SDK）默认会重试 2 次；如果在外层又包了一层 `@retry(stop=stop_after_attempt(3))`，一次故障实际会发出 `2 × 3 = 6` 次真实的物理请求，不仅加剧上游拥堵，账单直接翻倍。
- **FastAPI 客户端断连（Task Cancel）的资源泄漏**：当前端用户关掉浏览器或主动取消流式请求时，抛出 `asyncio.CancelledError`。如果限流器或熔断器没有做严密的异步上下文解绑，会导致半开探针槽位死锁或并发 Bulkhead 槽位永久丢失。

---

### 2. 我们做了 FlowGuard (`flowguard-core`)

为了彻底解决这些问题，我们开源了 **FlowGuard**：一个**纯原生标准库实现（0 外部依赖）、类型安全、专为 LLM 与高并发异步网关设计的弹性编排引擎**。

- **GitHub**: https://github.com/yangjinyu050618-hash/flowguard
- **PyPI**: `pip install flowguard-core`

#### 核心特性：
1. **严格 FIFO 双令牌桶（RPM + TPM）**：按请求数与 Token 预算双重排队，基于 `asyncio.Future` 实现真正零忙轮询（Zero Busy-polling）和无饥饿调度。
2. **唯一重试所有权（Sole Retry Ownership）**：内置 Adapter（OpenAI / Anthropic / Gemini）自动派生 `max_retries=0` 的安全客户端，确保所有重试由统一引擎精准记账，杜绝隐式重复扣费。
3. **熔断与动态降级（Circuit Breaker + ChoiceFallback）**：主模型故障（503/529）时触发快速熔断，支持静态降级或人机协同动态切换备用模型（如 GPT-4o -> Claude-3.5-Sonnet -> DeepSeek-R1）。
4. **客户端取消安全（Zero-Leak Slot Recovery）**：当请求被取消时，立即无残留解绑等待队列与并发信号量，杜绝死锁。
5. **开箱即用指标遥测**：内置滚动延迟直方图（P50/P95/P99）与 Prometheus 格式导出。

---

### 3. 一行代码极速上手

```python
import asyncio
from flowguard import guard

# 自动限流 + 熔断 + 指数退避抖动重试 + 降级
@guard(
    name="llm-service",
    rate_per_sec=20.0,       # 20 RPS
    burst_capacity=30.0,     # 允许突发 30
    max_retries=2,           # 瞬态错误最多重试 2 次
    failure_threshold=3,     # 连续失败 3 次触发熔断
    recovery_timeout=5.0,    # 5 秒后进入 HALF_OPEN 探测
    fallback=lambda prompt, exc=None: f"[Fallback] Cached: {prompt}"
)
async def ask_llm(prompt: str) -> str:
    # 你的模型调用逻辑
    return f"Response for: {prompt}"

async def main():
    print(await ask_llm("Hello!"))

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 4. 项目状态与测试覆盖
- **测试覆盖率**：92.24%（90 个用例涵盖全并发竞争、超时取消、SDK 契约、FastAPI 真实 ASGI 请求）。
- **CI 矩阵**：Ubuntu / macOS / Windows × Python 3.9 ~ 3.13 18 项 CI 矩阵 100% 绿灯。
- **架构设计文档**：[Why FlowGuard? 架构边界与选型对比矩阵](https://github.com/yangjinyu050618-hash/flowguard/blob/main/docs/WHY_FLOWGUARD.md)

非常期待各位的反馈、Issue 与 PR！如果你正在折腾 FastAPI / LLM 网关，欢迎 Star 关注或在实际项目中试用体验！
