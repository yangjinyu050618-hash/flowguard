# 🛡️ FlowGuard v0.3.0 架构升级与深度审查修复报告 (For GPT / Peer Review)

> **项目名称**：`FlowGuard` (`flowguard-core` on PyPI)  
> **审查版本**：`v0.3.0`  
> **代码仓库**：[yangjinyu050618-hash/flowguard](https://github.com/yangjinyu050618-hash/flowguard)  
> **测试状态**：**70 / 70 测试通过 (100%)** | **覆盖率：93.04%** | **Mypy Strict：0 告警** | **Ruff：0 错误**  

---

## 📌 本次审查与架构重构总结

在本次迭代中，针对审查报告中指出的 **4 项 P1、2 项 P2 和 1 项 P3** 深度问题，进行了彻底的契约化重构与严格的防回归测试覆盖：

---

### 一、 核心缺陷修复与实现对照

#### 1. 【P1-1 修复】废除运行时 `TypeError` 猜测，杜绝 Side-Effect 重复执行
* **根因**：原实现通过 `try...except TypeError:` 尝试不同签名调用 fallback，导致当 handler 内部因业务逻辑抛出 `TypeError` 时被捕获并重入调用多达 2~3 次。
* **修复**：
  * 在执行 handler 之前，使用 `inspect.signature` 在静态阶段**单次**解析绑定参数契约。
  * 每个 handler 在单次 fallback 流程中**严格执行且仅执行 1 次**。
  * handler 内部异常（包括 `TypeError`）原样沿异常链传播（`raise fb_exc from exc`）。
* **新增测试**：`test_fallback_internal_type_error_executed_once`、`test_choice_fallback_candidate_type_error_executed_once`。

---

#### 2. 【P1-2 修复】引入不可变 `FallbackContext`，杜绝参数碰撞与信息丢失
* **根因**：原实现使用 `exc=` 关键字注入异常，当被保护函数自身包含 `exc` 参数时发生多值碰撞；且通用 `**kwargs` 无法获取异常上下文。
* **修复**：
  * 引入不可变数据类 `FallbackContext`：
    ```python
    @dataclass(frozen=True)
    class FallbackContext:
        exception: BaseException
        args: Tuple[Any, ...]
        kwargs: Mapping[str, Any]
        pipeline_name: str
    ```
  * 将故障异常与原调用参数彻底解耦，无论原函数声明何种入参（如 `exc="caller-value"`），均完整保留在 `ctx.kwargs` 中。
  * 各适配器专用元数据（如 `estimated_tokens`）完整透传至 `FallbackContext`。
* **新增测试**：`test_fallback_with_caller_exc_keyword_collision`、`test_estimated_tokens_preserved_in_fallback`、`test_concurrent_selectors_isolated`。

---

#### 3. 【P1-3 修复】修正 Google GenAI 异常层级与 `.code` 状态识别
* **根因**：Google 官方 `google.genai.errors.ClientError` 使用 `.code` 存储状态码，原 `is_permanent_client_error()` 仅检查 `.status_code`。
* **修复**：
  * `is_permanent_client_error()` 统一提取 `.status_code` 与 `.code`。
  * 对 Google GenAI 的 400（InvalidArgument）、401（Unauthenticated）、403（PermissionDenied）、404（NotFound）实行**严格快速失败（Fail-Fast，物理调用 1 次）**；对 429、500、503 实行指数退避重试。
* **新增测试**：`test_google_genai_error_classification`、`test_gemini_permanent_400_fail_fast`。

---

#### 4. 【P1-4 修复】确立 FlowGuard 为“唯一重试所有者”（Sole Retry Owner）
* **根因**：Anthropic/OpenAI 官方 SDK 内部带有默认重试，与 FlowGuard 外层重试相乘（如 $5 \times 3 = 15$ 次物理调用），导致物理请求失控且 RPM/TPM 记账失准。
* **修复**：
  * FlowGuard 作为唯一重试与计量所有者，在适配器调用底层 SDK 时显式传入 `max_retries=0`，彻底关闭 SDK 内部静默重试。
  * 保证物理请求次数严格等于 FlowGuard 计量的 attempts，杜绝不可预测的倍乘放大。
* **新增测试**：`test_sdk_max_retries_zero_enforced`。

---

#### 5. 【P2-1 修复】收紧 Gemini 客户端异步契约
* **修复**：
  * 移除对同步 `client.models.generate_content` 的错误 `await` 分支。
  * 构造时 Fail-Fast 校验异步客户端接口（`client.aio.models.generate_content` 或 `client.generate_content_async`），非异步客户端抛出清晰的 `TypeError`。
* **新增测试**：`test_gemini_sync_client_rejection`。

---

#### 6. 【P2-2 修复】收紧 `ChoiceFallback` 协议与候选字典快照隔离
* **修复**：
  * 构造时对 `candidates` 字典进行浅拷贝冻结（`self._candidates = dict(candidates)`），防止 selector 等待期间外部字典变动引发竞态。
  * 规范 `selector` 签名契约：`selector(context: FallbackContext, options: List[str]) -> Optional[str]`。
  * 使用 `None` 作为正式的取消指示，弃用易产生键名冲突的字符串魔法值（如 `"abort"`）。
* **新增测试**：`test_choice_fallback_candidate_freeze_and_none_sentinel`、`test_choice_fallback_validation`。

---

#### 7. 【P3-1 修正】文档与版本准确性校准
* `README.md` 与文档准确标明：FlowGuard v0.3.0 具备 `FallbackContext`、`ChoiceFallback` 人机协同选择、多模型适配器矩阵及每日 CI 巡检配置。

---

## 二、 公共 API 变更与迁移指南

| 组件 | 旧签名 (v0.2.x) | 新签名 (v0.3.0) | 迁移示例 |
| :--- | :--- | :--- | :--- |
| `Fallback` | 不支持或仅支持特定 kwargs | 支持 `FallbackContext` 或普通参数 | `def my_fb(ctx: FallbackContext): return ctx.kwargs` |
| `ChoiceFallback` | 隐式选择或依赖字符串标记 | `ChoiceFallback(candidates, selector)`，返回 `None` 取消 | `def sel(ctx, opts): return "deepseek-r1"` |
| `ResilientAnthropic` | 无 | `ResilientAnthropic(client, rpm_limit, tpm_limit, max_retries=4)` | 原生适配 Claude |
| `ResilientGemini` | 无 | `ResilientGemini(client, rpm_limit, tpm_limit, max_retries=4)` | 原生适配 Google GenAI `client.aio` |

---

## 三、 本地全量测试与覆盖率实测数据

```text
============================= test session starts =============================
platform win32 -- Python 3.11.15, pytest-9.0.2, pluggy-1.6.0
collected 70 items

tests/test_adapters.py::test_openai_adapter PASSED                       [  1%]
tests/test_adapters.py::test_httpx_adapter PASSED                        [  2%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_basic PASSED     [  4%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_fallback PASSED  [  5%]
tests/test_fallback.py::test_fallback_on_circuit_breaker_open PASSED     [ 37%]
tests/test_fallback.py::test_fallback_on_retry_exhaustion PASSED         [ 38%]
tests/test_fallback.py::test_fallback_decorator_integration PASSED       [ 40%]
tests/test_fallback.py::test_fallback_cancellation_does_not_trigger_fallback PASSED [ 41%]
tests/test_fallback.py::test_choice_fallback_interactive_selection PASSED [ 42%]
tests/test_fallback.py::test_choice_fallback_validation PASSED           [ 44%]
tests/test_gemini_adapter.py::test_gemini_adapter_basic PASSED           [ 45%]
tests/test_gemini_adapter.py::test_gemini_adapter_fallback PASSED        [ 47%]
tests/test_p1_p2_fixes.py::test_fallback_internal_type_error_executed_once PASSED [ 57%]
tests/test_p1_p2_fixes.py::test_choice_fallback_candidate_type_error_executed_once PASSED [ 58%]
tests/test_p1_p2_fixes.py::test_fallback_with_caller_exc_keyword_collision PASSED [ 60%]
tests/test_p1_p2_fixes.py::test_estimated_tokens_preserved_in_fallback PASSED [ 61%]
tests/test_p1_p2_fixes.py::test_concurrent_selectors_isolated PASSED     [ 62%]
tests/test_p1_p2_fixes.py::test_google_genai_error_classification PASSED [ 64%]
tests/test_p1_p2_fixes.py::test_gemini_permanent_400_fail_fast PASSED    [ 65%]
tests/test_p1_p2_fixes.py::test_sdk_max_retries_zero_enforced PASSED     [ 67%]
tests/test_p1_p2_fixes.py::test_gemini_sync_client_rejection PASSED      [ 68%]
tests/test_p1_p2_fixes.py::test_choice_fallback_candidate_freeze_and_none_sentinel PASSED [ 70%]
...
=============================== tests coverage ================================
TOTAL: 905 statements, 63 missed -> 93.04% Coverage
============================= 70 passed in 2.85s ==============================
```

* **静态类型检查**：`mypy src/flowguard` (Strict Mode) 👉 **0 告警 (18 files)**
* **代码格式与 Lint**：`ruff check src tests examples` 👉 **All checks passed (0 errors)**
* **环境兼容**：Python 3.9 ~ 3.13，跨 Ubuntu / Windows / macOS 平台。
