# 🛡️ FlowGuard v0.3.0 升级审查与变更报告 (For GPT / Peer Review)

> **项目名称**：`FlowGuard` (`flowguard-core` on PyPI)  
> **审查版本**：`v0.3.0` (Commit: `5303b62`)  
> **代码仓库**：[yangjinyu050618-hash/flowguard](https://github.com/yangjinyu050618-hash/flowguard)  
> **测试状态**：**60 / 60 测试通过 (100%)** | **覆盖率：92.04%** | **Mypy Strict：0 告警** | **Ruff：0 错误**  

---

## 📌 版本升级动机与核心目标

在上一版本（v0.2.2）中，FlowGuard 实现了严密的 **FIFO 令牌桶限流、防踩踏探针熔断器、指数退避重试与并发隔板隔离**。

在本次 `v0.3.0` 重大更新中，针对现代大语言模型（LLM）与分布式微服务在真实生产链路中的**弹性决策、多模型协同与人机协同（Human-in-the-Loop）**诉求，新增了三大核心架构能力：

1. **可选择式/人机协同降级路由（`ChoiceFallback`）**：当主模型（如 GPT-4o / GPT-5）发生熔断或不可用时，**不再单纯进行静默硬编码切换，而是向用户、Web 前端或决策代理发起交互式询问**，由决策者动态决定接入 Claude、DeepSeek、Gemini 或取消请求。
2. **通用优雅降级（`Fallback`）**：在重试耗尽、熔断器打开或限流超额时，支持平滑调用降级函数返回缓存或兜底响应。
3. **多主流大模型适配器矩阵**：新增 `ResilientAnthropic`（Claude 官方 SDK）与 `ResilientGemini`（Google GenAI 官方 SDK），提供统一的 RPM/TPM 双轴预占与错误自适应拦截。
4. **CI 每日自动健康巡检**：加入 GitHub Actions 定时任务（每日 00:00 UTC / 08:00 北京时间自动运行全套 15 个环境的测试矩阵）。

---

## 🌟 核心新特性与设计架构

### 1. 人机协同交互降级路由：`ChoiceFallback`

#### 场景背景
在很多高价值业务中（如代码生成、复杂合同审查、医疗/法律 AI Agent），当默认最高精度的模型发生限流或熔断故障时：
- 盲目直接切换到廉价模型可能会导致输出质量崩塌；
- 直接报错中断则导致流程完全阻断。

`ChoiceFallback` 提供了“可选择式降级机制”：它会在捕获到主流程异常时，调用外部选择器函数（`selector`），支持终端 CLI 输入、Web 前端弹窗审批、或基于规则的 Agent 仲裁引擎。

#### API 示例与用法
```python
import asyncio
from flowguard import guard, ChoiceFallback

# 1. 声明备选候选路线
async def call_claude(prompt: str) -> str:
    return f"[Claude 3.5 Sonnet]: {prompt}"

async def call_deepseek(prompt: str) -> str:
    return f"[DeepSeek R1]: {prompt}"

async def call_gemini(prompt: str) -> str:
    return f"[Gemini 2.5 Flash]: {prompt}"

# 2. 交互式选择器（询问用户或前端 UI）
async def ask_user_for_model(exc: Exception, available_options: list[str]) -> str:
    print(f"\n[FlowGuard Decision Trigger] 主模型调用故障: {type(exc).__name__} ({exc})")
    print(f"可选备用方案: {available_options}，或输入 'abort' 取消")
    
    # 实际应用中可对接 input()、WebSocket 弹窗等待、或多智能体决策器
    # 返回选中的候选名称（例如 'deepseek-r1'）
    return "deepseek-r1"

# 3. 组装 ChoiceFallback 路由器
interactive_router = ChoiceFallback(
    candidates={
        "claude-3.5-sonnet": call_claude,
        "deepseek-r1": call_deepseek,
        "gemini-2.5-flash": call_gemini,
    },
    selector=ask_user_for_model,
)

# 4. 挂载到主模型保护链路
@guard(name="gpt-primary", failure_threshold=2, fallback=interactive_router)
async def ask_primary_gpt(prompt: str) -> str:
    # 当 GPT-4o 熔断或报错时，触发 ChoiceFallback 询问用户
    raise ConnectionResetError("GPT-4o 503 Service Unavailable")
```

---

### 2. 优雅降级兜底支持（`fallback` 参数）

无论是 `@guard` 装饰器还是 `FlowGuard` 管道实例，均原生支持 `fallback` 参数：
* 支持同步函数与异步协程（`async def`）。
* 智能参数适配：降级函数可自由声明是否接收 `exc` 异常上下文（`sig.parameters` 动态内省）。
* 取消安全保护：若主任务被 `asyncio.CancelledError` 外部取消，绝不误触降级函数，安全原样向外传播取消信号。

---

### 3. 多模型生态适配器扩展

```python
from flowguard.adapters import (
    ResilientOpenAI,
    ResilientAnthropic,
    ResilientGemini,
    ResilientHTTPClient,
)

# OpenAI: 500 RPM + 60,000 TPM
openai_client = ResilientOpenAI(mock_openai, rpm_limit=500.0, tpm_limit=60_000.0)

# Anthropic Claude: 300 RPM + 40,000 TPM
claude_client = ResilientAnthropic(mock_anthropic, rpm_limit=300.0, tpm_limit=40_000.0)

# Google Gemini: 300 RPM + 60,000 TPM
gemini_client = ResilientGemini(mock_gemini, rpm_limit=300.0, tpm_limit=60_000.0)
```

---

## 🧪 验证与测试数据 (Test Suite & Coverage)

本次升级新增了 12 个专项测试用例与 3 个开箱即用示例，涵盖单机模式、交互选择模式、降级容错及多模型调用：

### 1. 本地全量测试结果
```text
============================= test session starts =============================
collected 60 items

tests/test_adapters.py::test_openai_adapter PASSED                       [  1%]
tests/test_adapters.py::test_httpx_adapter PASSED                        [  3%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_basic PASSED     [  5%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_fallback PASSED  [  6%]
tests/test_fallback.py::test_fallback_on_circuit_breaker_open PASSED     [ 43%]
tests/test_fallback.py::test_fallback_on_retry_exhaustion PASSED         [ 45%]
tests/test_fallback.py::test_fallback_decorator_integration PASSED       [ 46%]
tests/test_fallback.py::test_fallback_cancellation_does_not_trigger_fallback PASSED [ 48%]
tests/test_fallback.py::test_choice_fallback_interactive_selection PASSED [ 50%]
tests/test_fallback.py::test_choice_fallback_no_selector_and_empty PASSED [ 51%]
tests/test_gemini_adapter.py::test_gemini_adapter_basic PASSED           [ 53%]
tests/test_gemini_adapter.py::test_gemini_adapter_fallback PASSED        [ 55%]
...
=============================== tests coverage ================================
TOTAL: 892 statements, 71 missed -> 92.04% Coverage
============================= 60 passed in 2.78s ==============================
```

### 2. 代码合规性
* `ruff check src tests examples` 👉 **All checks passed (0 errors)**
* `mypy src/flowguard` (Strict Mode) 👉 **Success: no issues found in 18 source files**
* 跨平台兼容性：支持 Python 3.9 ~ 3.13，Ubuntu / Windows / macOS 矩阵。

---

## 📂 新增与修改文件清单

| 文件路径 | 说明 |
| :--- | :--- |
| `src/flowguard/core/fallback.py` | `ChoiceFallback` 交互式选择降级核心实现 |
| `src/flowguard/adapters/anthropic_adapter.py` | `ResilientAnthropic` Claude 客户端弹性适配器 |
| `src/flowguard/adapters/gemini_adapter.py` | `ResilientGemini` Google GenAI 客户端弹性适配器 |
| `tests/test_fallback.py` | Fallback 与 ChoiceFallback 单元与集成测试套件 |
| `tests/test_anthropic_adapter.py` | Anthropic 适配器测试 |
| `tests/test_gemini_adapter.py` | Gemini 适配器测试 |
| `examples/04_fallback_graceful_degradation.py` | 优雅降级实测运行示例 |
| `examples/05_multi_llm_orchestration.py` | 多模型联合编排示例 |
| `examples/06_interactive_human_in_the_loop_fallback.py` | 人机协同交互选择降级实测运行示例 |

---

## 🎯 请审查员（GPT / Peer Reviewer）关注的重点

1. **API 设计直觉性**：`ChoiceFallback` 的候选字典与选择器签名是否符合 Pythonic 规范，是否易于集成到现代 Web UI / Agent 工作流中。
2. **异常穿透与取消安全**：在协程被外部 `cancel` 时，是否做到了零误触发、零残留。
3. **参数透传与扩展性**：各模型适配器在进行降级或主调用时，入参与关键字参数的处理是否严谨无副作用。
