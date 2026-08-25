# 🛡️ FlowGuard v0.3.0 二次验收与严格签名契约报告 (For Review)

> **项目名称**：`FlowGuard` (`flowguard-core` on PyPI)  
> **版本**：`v0.3.0`  
> **代码仓库**：[yangjinyu050618-hash/flowguard](https://github.com/yangjinyu050618-hash/flowguard)  
> **测试状态**：**75 / 75 测试通过 (100%)** | **覆盖率：92.69%** | **Mypy Strict：0 告警** | **Ruff：0 错误**  

---

## 📌 二次验收问题闭环清单

根据《FlowGuard v0.3.0 二次验收报告》，本次提交已针对 **2 个 P1 阻断问题、2 个 P2 问题和 1 个 P3 问题** 进行了全部修复并增加了严格签名测试：

### 1. 【P1-1 修复】消除资源方法中的 `max_retries` 参数，严格适配官方 SDK
* **根因**：OpenAI 的 `chat.completions.create` 与 Anthropic 的 `messages.create` 官方方法签名不接收 `max_retries` 关键字入参，设置 SDK 层重试必须在客户端层。
* **修复**：
  * 在适配器构造阶段，使用 `client.with_options(max_retries=0)` 在客户端层面禁用 SDK 内置重试，保持调用者传入的原客户端无副作用；
  * `create_chat_completion` 与 `create_message` 在调用资源方法时，**绝不向请求方法透传 `max_retries`**；
  * 编写了严格方法签名的 Fake Client 测试（不包含 `**kwargs` 放行未知参数），断言调用成功且 FlowGuard 仍是唯一重试所有者。
* **新增测试**：`test_openai_strict_signature_and_sole_retry_ownership`、`test_anthropic_strict_signature_and_sole_retry_ownership`。

---

### 2. 【P1-2 修复】废除参数名匹配，采用显式 `FallbackContext` 类型契约
* **根因**：原实现检查参数名中是否包含 `context` 或 `ctx`，导致含正常业务参数 `context`/`ctx` 的普通 fallback 函数（如 `fallback(context, prompt)`）丢失其他入参。
* **修复**：
  * 引入显式判定规则 `is_context_handler(func)`：
    1. 检查是否为 `ChoiceFallback` 实例；
    2. 检查是否使用 `@with_fallback_context` 显式装饰；
    3. 检查参数类型注解是否为 `FallbackContext`。
  * 对普通业务 fallback（即使参数名为 `context`、`ctx`、`prompt`、`user_id`），一律完整透传原始 `*args, **kwargs`，完全不破坏业务参数契约。
* **新增测试**：`test_ordinary_business_fallback_with_context_param_name`、`test_ordinary_business_fallback_with_ctx_param_name`、`test_explicit_fallback_context_handler_via_annotation_or_decorator`。

---

### 3. 【P2 修复】`FallbackContext.kwargs` 真正运行时不可变
* **修复**：在 `FallbackContext.__post_init__` 中将 `kwargs` 封装为 `types.MappingProxyType`。
* **效果**：在运行时对 `ctx.kwargs["k"] = "v"` 赋值会直接抛出 `TypeError: 'mappingproxy' object does not support item assignment`，达成真正不可变。
* **新增测试**：`test_fallback_context_kwargs_true_immutability`。

---

### 4. 【P2 修复】明确 Gemini 自定义重试边界与文档规范
* **修复**：在 `ResilientGemini` 类文档与 README 中明确注明：FlowGuard 接管外层弹性编排；官方 `google-genai` SDK 推荐使用默认配置或在 `http_options` 中避免开启重复重试。

---

### 5. 【P3 修复】README 示例同步更新为 `FallbackContext`
* **修复**：已同步更新 `README.md` 中 `ChoiceFallback` 的 selector 签名：
  ```python
  async def ask_user_for_model(context: FallbackContext, available_options: list[str]) -> str:
      print(f"Primary model failed: {context.exception}. Choose fallback from {available_options}:")
      return "deepseek-r1"
  ```

---

## 🧪 测试套件与代码质量实测数据

```text
============================= test session starts =============================
platform win32 -- Python 3.11.15, pytest-9.0.2, pluggy-1.6.0
collected 75 items

tests/test_adapters.py::test_openai_adapter PASSED                       [  1%]
tests/test_adapters.py::test_httpx_adapter PASSED                        [  2%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_basic PASSED     [  4%]
tests/test_anthropic_adapter.py::test_anthropic_adapter_fallback PASSED  [  5%]
tests/test_fallback.py::test_fallback_on_circuit_breaker_open PASSED     [ 34%]
tests/test_fallback.py::test_fallback_on_retry_exhaustion PASSED         [ 36%]
tests/test_fallback.py::test_fallback_decorator_integration PASSED       [ 37%]
tests/test_fallback.py::test_fallback_cancellation_does_not_trigger_fallback PASSED [ 38%]
tests/test_fallback.py::test_choice_fallback_interactive_selection PASSED [ 40%]
tests/test_fallback.py::test_choice_fallback_validation PASSED           [ 41%]
tests/test_p1_p2_fixes.py::test_openai_strict_signature_and_sole_retry_ownership PASSED [ 53%]
tests/test_p1_p2_fixes.py::test_anthropic_strict_signature_and_sole_retry_ownership PASSED [ 54%]
tests/test_p1_p2_fixes.py::test_ordinary_business_fallback_with_context_param_name PASSED [ 56%]
tests/test_p1_p2_fixes.py::test_ordinary_business_fallback_with_ctx_param_name PASSED [ 57%]
tests/test_p1_p2_fixes.py::test_explicit_fallback_context_handler_via_annotation_or_decorator PASSED [ 58%]
tests/test_p1_p2_fixes.py::test_fallback_context_kwargs_true_immutability PASSED [ 60%]
...
=============================== tests coverage ================================
TOTAL: 930 statements, 68 missed -> 92.69% Coverage
============================= 75 passed in 2.81s ==============================
```

* **静态检查**：`mypy src/flowguard` (Strict Mode) 👉 **0 告警**
* **Lint 检查**：`ruff check src tests examples` 👉 **All checks passed (0 errors)**
