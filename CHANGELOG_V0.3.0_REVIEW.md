# 🛡️ FlowGuard v0.3.0 第三次验收与深度契约修复报告 (For Review)

> **项目名称**：`FlowGuard` (`flowguard-core` on PyPI)  
> **版本**：`v0.3.0`  
> **代码仓库**：[yangjinyu050618-hash/flowguard](https://github.com/yangjinyu050618-hash/flowguard)  
> **测试状态**：**78 / 78 测试通过 (100%)** | **覆盖率：92.21%** | **Mypy Strict：0 告警** | **Ruff：0 错误**  

---

## 📌 第三次验收问题闭环对照清单

根据《FlowGuard v0.3.0 第三次验收报告》，本次提交已针对 **1 个 P1 阻断问题、2 个 P2 问题和 1 个 P3 问题** 完成了彻底闭环：

### 1. 【P1 修复】彻底隔离签名检查与 Candidate 业务调用，杜绝二次执行
* **根因**：原 `fallback.py` 中将 `inspect.signature(target_func)` 与 `target_func(...)` 的实际调用置于同一 `try...except (ValueError, TypeError)` 块中，导致同步 candidate 自身抛出业务 `TypeError` 时被捕获并重复调用第 2 次。
* **修复**：
  * 将 `inspect.signature` 的静态探针完全移至业务调用外部；
  * `target_func(...)` 调用处于独立执行块中，任何内部异常（包括同步/异步 `TypeError`）**严格执行且仅执行 1 次**，并直接原样沿异常链抛出。
* **新增测试**：`test_choice_fallback_sync_candidate_type_error_executed_once`（断言抛出原始 `TypeError` 且调用次数严格为 1）。

---

### 2. 【P2 修复】支持 `from __future__ import annotations` 延迟注解协议
* **根因**：开启 `from __future__ import annotations` 后，Python 将类型注解转为字符串（`"FallbackContext"`），原 `is_context_handler` 无法通过类对象或 `__name__` 识别。
* **修复**：
  * 在 `is_context_handler` 中引入双重解析机制：
    1. 优先调用 `typing.get_type_hints(func)` 解析前向引用与延迟注解；
    2. 回退机制中检查参数注解字符串是否为 `"FallbackContext"` 或以 `".FallbackContext"` 结尾。
* **新增测试**：独立模块 `tests/test_future_annotations.py`，包含 `test_future_annotations_fallback_context` 与 `test_future_annotations_choice_fallback_candidate`。

---

### 3. 【P2 修复】Gemini 重试边界在 API Doc 与 README 中真实落地
* **修复**：
  * 在 `src/flowguard/adapters/gemini_adapter.py` 的 `ResilientGemini` 类文档中详细声明重试所有权与 `http_options` 推荐配置规范；
  * 在 `README.md` 中增加显式的 `Note on Retry Ownership` 警告说明，明确 FlowGuard 的单一重试所有者职责。

---

### 4. 【P3 修复】修正 Strict Fake 资源方法签名，移除 `**kwargs`
* **修复**：
  * `tests/test_p1_p2_fixes.py` 中的 `StrictOpenAICompletions.create` 与 `StrictAnthropicMessages.create` **完全去除了 `**kwargs` 通配符**，改为真实严格的具名关键字参数；
  * 证明适配器在调用时不向资源方法透传 `max_retries`，完全符合官方 SDK 签名规范。

---

## 🧪 测试套件与代码质量实测数据

```text
============================= test session starts =============================
platform win32 -- Python 3.11.15, pytest-9.0.2, pluggy-1.6.0
collected 78 items

tests/test_adapters.py::test_openai_adapter PASSED                       [  1%]
tests/test_adapters.py::test_httpx_adapter PASSED                        [  2%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_basic PASSED     [  4%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_fallback PASSED  [  5%]
tests/test_fallback.py::test_fallback_on_circuit_breaker_open PASSED     [ 33%]
tests/test_fallback.py::test_fallback_on_retry_exhaustion PASSED         [ 34%]
tests/test_fallback.py::test_fallback_decorator_integration PASSED       [ 35%]
tests/test_fallback.py::test_fallback_cancellation_does_not_trigger_fallback PASSED [ 37%]
tests/test_fallback.py::test_choice_fallback_interactive_selection PASSED [ 38%]
tests/test_fallback.py::test_choice_fallback_validation PASSED           [ 39%]
tests/test_future_annotations.py::test_future_annotations_fallback_context PASSED [ 41%]
tests/test_future_annotations.py::test_future_annotations_choice_fallback_candidate PASSED [ 42%]
tests/test_p1_p2_fixes.py::test_openai_strict_signature_and_sole_retry_ownership PASSED [ 53%]
tests/test_p1_p2_fixes.py::test_anthropic_strict_signature_and_sole_retry_ownership PASSED [ 55%]
tests/test_p1_p2_fixes.py::test_choice_fallback_sync_candidate_type_error_executed_once PASSED [ 56%]
tests/test_p1_p2_fixes.py::test_ordinary_business_fallback_with_context_param_name PASSED [ 57%]
tests/test_p1_p2_fixes.py::test_ordinary_business_fallback_with_ctx_param_name PASSED [ 58%]
tests/test_p1_p2_fixes.py::test_explicit_fallback_context_handler_via_annotation_or_decorator PASSED [ 60%]
tests/test_p1_p2_fixes.py::test_fallback_context_kwargs_true_immutability PASSED [ 61%]
...
=============================== tests coverage ================================
TOTAL: 950 statements, 74 missed -> 92.21% Coverage
============================= 78 passed in 2.82s ==============================
```

* **静态类型检查**：`mypy src/flowguard` (Strict Mode) 👉 **0 告警 (18 files)**
* **代码格式与 Lint**：`ruff check src tests examples` 👉 **All checks passed (0 errors)**
