"""Comprehensive tests for OpenTelemetry GenAI semantic convention telemetry exporter."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import patch
import pytest

from flowguard.core.circuit_breaker import CircuitState
from flowguard.metrics.collector import MetricsCollector
from flowguard.metrics.otel import (
    FLOWGUARD_CIRCUIT_STATE,
    FLOWGUARD_PIPELINE,
    FLOWGUARD_RETRY_COUNT,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SERVER_TIME_TO_FIRST_TOKEN,
    GEN_AI_SYSTEM,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_TOTAL_TOKENS,
    GenAIMetrics,
    OpenTelemetryExporter,
    export_opentelemetry,
    get_opentelemetry_trace,
    get_tracer,
    is_opentelemetry_available,
    map_genai_attributes,
)

# Detect if opentelemetry-sdk is available for full integration tests
try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.trace import StatusCode

    HAS_OTEL_SDK = True
except ImportError:
    HAS_OTEL_SDK = False


class FakeSpan:
    """Strict fake OpenTelemetry Span with exact official signatures (no permissive **kwargs)."""

    def __init__(self, name: str, start_time: Optional[int] = None) -> None:
        self.name: str = name
        self.start_time: Optional[int] = start_time
        self.end_time: Optional[int] = None
        self.attributes: Dict[str, Any] = {}
        self.status: Optional[Any] = None
        self.exceptions: List[BaseException] = []
        self.is_ended: bool = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_status(self, status: Any) -> None:
        self.status = status

    def record_exception(self, exception: BaseException) -> None:
        self.exceptions.append(exception)

    def end(self, end_time: Optional[int] = None) -> None:
        self.end_time = end_time
        self.is_ended = True


class FakeTracer:
    """Strict fake OpenTelemetry Tracer with exact official signatures."""

    def __init__(self, name: str = "mock-tracer") -> None:
        self.name: str = name
        self.created_spans: List[FakeSpan] = []

    def start_span(
        self,
        name: str,
        start_time: Optional[int] = None,
    ) -> FakeSpan:
        span = FakeSpan(name=name, start_time=start_time)
        self.created_spans.append(span)
        return span


def test_opentelemetry_dynamic_import_success() -> None:
    """Verify dynamic import succeeds when opentelemetry is available."""
    assert is_opentelemetry_available() is True
    mod = get_opentelemetry_trace()
    assert hasattr(mod, "get_tracer")
    tracer = get_tracer("test-tracer")
    assert tracer is not None


def test_opentelemetry_dynamic_import_missing() -> None:
    """Verify friendly ImportError is raised when opentelemetry is not installed."""
    with patch.dict("sys.modules", {"opentelemetry": None, "opentelemetry.trace": None}):
        assert is_opentelemetry_available() is False

        with pytest.raises(ImportError) as exc_info:
            get_opentelemetry_trace()
        msg = str(exc_info.value)
        assert "OpenTelemetry support requires" in msg
        assert "opentelemetry-api" in msg
        assert "opentelemetry-sdk" in msg

        with pytest.raises(ImportError, match="OpenTelemetry support requires"):
            get_tracer()

        with pytest.raises(ImportError, match="OpenTelemetry support requires"):
            OpenTelemetryExporter()


def test_exporter_with_custom_tracer() -> None:
    """Verify exporter can use a custom/mock tracer without importing opentelemetry."""
    fake_tracer = FakeTracer("custom-mock")
    exporter = OpenTelemetryExporter(tracer=fake_tracer, default_system="openai")
    assert exporter.tracer is fake_tracer
    assert exporter.default_system == "openai"


def test_map_genai_attributes_all_fields() -> None:
    """Verify all standard GenAI and FlowGuard attributes are mapped accurately."""
    mapped = map_genai_attributes(
        system="openai",
        model="gpt-4o",
        input_tokens=150,
        output_tokens=75,
        time_to_first_token=0.185,
        circuit_state=CircuitState.CLOSED,
        retry_count=2,
        pipeline="chat-resilience",
        response_model="gpt-4o-2024-08-06",
        custom_key="custom_val",
    )

    assert mapped[GEN_AI_SYSTEM] == "openai"
    assert mapped[GEN_AI_REQUEST_MODEL] == "gpt-4o"
    assert mapped[GEN_AI_RESPONSE_MODEL] == "gpt-4o-2024-08-06"
    assert mapped[GEN_AI_USAGE_INPUT_TOKENS] == 150
    assert mapped[GEN_AI_USAGE_OUTPUT_TOKENS] == 75
    assert mapped[GEN_AI_USAGE_TOTAL_TOKENS] == 225
    assert mapped[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.185
    assert mapped[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert mapped[FLOWGUARD_RETRY_COUNT] == 2
    assert mapped[FLOWGUARD_PIPELINE] == "chat-resilience"
    assert mapped["custom_key"] == "custom_val"


def test_map_genai_attributes_circuit_state_types() -> None:
    """Verify circuit_state works with both CircuitState enum and strings."""
    open_mapped = map_genai_attributes(circuit_state=CircuitState.OPEN)
    assert open_mapped[FLOWGUARD_CIRCUIT_STATE] == "OPEN"

    half_open_mapped = map_genai_attributes(circuit_state="HALF_OPEN")
    assert half_open_mapped[FLOWGUARD_CIRCUIT_STATE] == "HALF_OPEN"


def test_map_genai_attributes_zero_and_edge_values() -> None:
    """Verify 0 tokens, 0.0 latency, and 0 retries are preserved rather than omitted."""
    mapped = map_genai_attributes(
        input_tokens=0,
        output_tokens=0,
        time_to_first_token=0.0,
        retry_count=0,
    )
    assert mapped[GEN_AI_USAGE_INPUT_TOKENS] == 0
    assert mapped[GEN_AI_USAGE_OUTPUT_TOKENS] == 0
    assert mapped[GEN_AI_USAGE_TOTAL_TOKENS] == 0
    assert mapped[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.0
    assert mapped[FLOWGUARD_RETRY_COUNT] == 0


def test_map_genai_attributes_none_values_omitted() -> None:
    """Verify None values are not included in the exported dictionary."""
    mapped = map_genai_attributes(
        system=None,
        model=None,
        input_tokens=None,
        output_tokens=None,
        time_to_first_token=None,
        circuit_state=None,
        retry_count=None,
    )
    assert mapped == {}


def test_map_genai_attributes_from_dataclass() -> None:
    """Verify GenAIMetrics container converts to standard attributes."""
    metrics = GenAIMetrics(
        system="anthropic",
        model="claude-3-5-sonnet",
        input_tokens=1000,
        output_tokens=300,
        time_to_first_token=0.25,
        circuit_state="CLOSED",
        retry_count=1,
        extra_attributes={"flowguard.tenant": "tenant-abc"},
    )
    attrs = metrics.to_attributes()
    assert attrs[GEN_AI_SYSTEM] == "anthropic"
    assert attrs[GEN_AI_REQUEST_MODEL] == "claude-3-5-sonnet"
    assert attrs[GEN_AI_USAGE_INPUT_TOKENS] == 1000
    assert attrs[GEN_AI_USAGE_OUTPUT_TOKENS] == 300
    assert attrs[GEN_AI_USAGE_TOTAL_TOKENS] == 1300
    assert attrs[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.25
    assert attrs[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert attrs[FLOWGUARD_RETRY_COUNT] == 1
    assert attrs["flowguard.tenant"] == "tenant-abc"


def test_map_genai_attributes_from_dict() -> None:
    """Verify dictionary input with snake_case and standard keys."""
    raw_dict: Dict[str, Any] = {
        "system": "deepseek",
        "model": "deepseek-chat",
        "input_tokens": 500,
        "output_tokens": 120,
        "time_to_first_token": 0.45,
        "circuit_state": "CLOSED",
        "retry_count": 0,
        "region": "us-east-1",
    }
    mapped = map_genai_attributes(raw_dict)
    assert mapped[GEN_AI_SYSTEM] == "deepseek"
    assert mapped[GEN_AI_REQUEST_MODEL] == "deepseek-chat"
    assert mapped[GEN_AI_USAGE_INPUT_TOKENS] == 500
    assert mapped[GEN_AI_USAGE_OUTPUT_TOKENS] == 120
    assert mapped[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.45
    assert mapped[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert mapped[FLOWGUARD_RETRY_COUNT] == 0
    assert mapped["region"] == "us-east-1"


def test_fake_tracer_export_span() -> None:
    """Verify export_span with strict fake tracer."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer)

    span = exporter.export_span(
        system="gemini",
        model="gemini-1.5-pro",
        input_tokens=80,
        output_tokens=40,
        time_to_first_token=0.12,
        circuit_state=CircuitState.CLOSED,
        retry_count=1,
    )

    assert span.is_ended is True
    assert span.name == "chat gemini-1.5-pro"
    assert span.attributes[GEN_AI_SYSTEM] == "gemini"
    assert span.attributes[GEN_AI_REQUEST_MODEL] == "gemini-1.5-pro"
    assert span.attributes[GEN_AI_USAGE_INPUT_TOKENS] == 80
    assert span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS] == 40
    assert span.attributes[GEN_AI_USAGE_TOTAL_TOKENS] == 120
    assert span.attributes[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.12
    assert span.attributes[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert span.attributes[FLOWGUARD_RETRY_COUNT] == 1


def test_fake_tracer_export_span_with_error() -> None:
    """Verify export_span records error and exception with fake tracer."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer)

    err = RuntimeError("Upstream provider failure")
    span = exporter.export_span(
        name="custom.call",
        system="openai",
        error=err,
    )

    assert span.is_ended is True
    assert span.name == "custom.call"
    assert err in span.exceptions


def test_fake_tracer_context_manager() -> None:
    """Verify sync context manager trace_span with fake tracer."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer)

    with exporter.trace_span(system="openai", model="gpt-4o") as ctx:
        ctx.set_tokens(input_tokens=100, output_tokens=30)
        ctx.set_time_to_first_token(0.35)
        ctx.set_circuit_state(CircuitState.CLOSED)
        ctx.set_retry_count(0)

    assert len(tracer.created_spans) == 1
    span = tracer.created_spans[0]
    assert span.is_ended is True
    assert span.name == "chat gpt-4o"
    assert span.attributes[GEN_AI_SYSTEM] == "openai"
    assert span.attributes[GEN_AI_REQUEST_MODEL] == "gpt-4o"
    assert span.attributes[GEN_AI_USAGE_INPUT_TOKENS] == 100
    assert span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS] == 30
    assert span.attributes[GEN_AI_USAGE_TOTAL_TOKENS] == 130
    assert span.attributes[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.35
    assert span.attributes[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert span.attributes[FLOWGUARD_RETRY_COUNT] == 0


def test_fake_tracer_context_manager_exception() -> None:
    """Verify sync context manager records exception and ends span on failure."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer)

    with pytest.raises(ValueError, match="test error"):
        with exporter.trace_span(name="faulty-operation"):
            raise ValueError("test error")

    assert len(tracer.created_spans) == 1
    span = tracer.created_spans[0]
    assert span.is_ended is True
    assert len(span.exceptions) == 1
    assert isinstance(span.exceptions[0], ValueError)


@pytest.mark.asyncio
async def test_fake_tracer_async_context_manager() -> None:
    """Verify async context manager trace_span_async with fake tracer."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer)

    async with exporter.trace_span_async(system="anthropic", model="claude-3-haiku") as ctx:
        await asyncio.sleep(0.01)
        ctx.set_tokens(input_tokens=200, output_tokens=50)
        ctx.set_time_to_first_token(0.08)
        ctx.set_circuit_state("HALF_OPEN")
        ctx.set_retry_count(1)

    assert len(tracer.created_spans) == 1
    span = tracer.created_spans[0]
    assert span.is_ended is True
    assert span.attributes[GEN_AI_SYSTEM] == "anthropic"
    assert span.attributes[GEN_AI_REQUEST_MODEL] == "claude-3-haiku"
    assert span.attributes[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.08
    assert span.attributes[FLOWGUARD_CIRCUIT_STATE] == "HALF_OPEN"
    assert span.attributes[FLOWGUARD_RETRY_COUNT] == 1


@pytest.mark.asyncio
async def test_fake_tracer_async_context_manager_exception() -> None:
    """Verify async context manager records exception on coroutine failure."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer)

    with pytest.raises(ZeroDivisionError):
        async with exporter.trace_span_async(name="async-failure"):
            await asyncio.sleep(0.01)
            raise ZeroDivisionError("division by zero")

    assert len(tracer.created_spans) == 1
    span = tracer.created_spans[0]
    assert span.is_ended is True
    assert len(span.exceptions) == 1
    assert isinstance(span.exceptions[0], ZeroDivisionError)


# ==============================================================================
# OpenTelemetry SDK Integration Tests using InMemorySpanExporter
# ==============================================================================


@pytest.fixture
def otel_in_memory() -> Any:
    """Setup InMemorySpanExporter with fresh TracerProvider for each test."""
    if not HAS_OTEL_SDK:
        pytest.skip("opentelemetry-sdk is not installed")

    provider = TracerProvider()
    memory_exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    tracer = provider.get_tracer("flowguard-test")
    return tracer, memory_exporter


def test_in_memory_span_exporter_direct_export(otel_in_memory: Any) -> None:
    """Verify export_span records all attributes and ends span in InMemorySpanExporter."""
    tracer, memory_exporter = otel_in_memory
    exporter = OpenTelemetryExporter(tracer=tracer)

    exporter.export_span(
        system="openai",
        model="gpt-4o",
        input_tokens=350,
        output_tokens=120,
        time_to_first_token=0.198,
        circuit_state=CircuitState.CLOSED,
        retry_count=0,
    )

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    finished = spans[0]

    assert finished.name == "chat gpt-4o"
    assert finished.status.status_code == StatusCode.OK

    attrs = finished.attributes
    assert attrs is not None
    assert attrs[GEN_AI_SYSTEM] == "openai"
    assert attrs[GEN_AI_REQUEST_MODEL] == "gpt-4o"
    assert attrs[GEN_AI_USAGE_INPUT_TOKENS] == 350
    assert attrs[GEN_AI_USAGE_OUTPUT_TOKENS] == 120
    assert attrs[GEN_AI_USAGE_TOTAL_TOKENS] == 470
    assert attrs[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.198
    assert attrs[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert attrs[FLOWGUARD_RETRY_COUNT] == 0


def test_in_memory_span_exporter_with_error(otel_in_memory: Any) -> None:
    """Verify export_span with error sets ERROR status and records exception in InMemorySpanExporter."""
    tracer, memory_exporter = otel_in_memory
    exporter = OpenTelemetryExporter(tracer=tracer)

    err = ConnectionResetError("Connection dropped by peer")
    exporter.export_span(
        name="llm.query",
        system="anthropic",
        model="claude-3-opus",
        error=err,
        circuit_state=CircuitState.OPEN,
        retry_count=3,
    )

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    finished = spans[0]

    assert finished.name == "llm.query"
    assert finished.status.status_code == StatusCode.ERROR
    assert "Connection dropped by peer" in str(finished.status.description)

    assert finished.attributes is not None
    assert finished.attributes[GEN_AI_SYSTEM] == "anthropic"
    assert finished.attributes[GEN_AI_REQUEST_MODEL] == "claude-3-opus"
    assert finished.attributes[FLOWGUARD_CIRCUIT_STATE] == "OPEN"
    assert finished.attributes[FLOWGUARD_RETRY_COUNT] == 3

    events = [e.name for e in finished.events]
    assert "exception" in events


def test_in_memory_span_exporter_sync_context_manager(otel_in_memory: Any) -> None:
    """Verify sync context manager sets attributes and records to InMemorySpanExporter."""
    tracer, memory_exporter = otel_in_memory
    exporter = OpenTelemetryExporter(tracer=tracer)

    with exporter.trace_span(system="google", model="gemini-1.5-flash") as ctx:
        ctx.set_tokens(input_tokens=120, output_tokens=60)
        ctx.set_time_to_first_token(0.14)
        ctx.set_circuit_state(CircuitState.CLOSED)
        ctx.set_retry_count(1)

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    finished = spans[0]

    assert finished.name == "chat gemini-1.5-flash"
    assert finished.status.status_code == StatusCode.OK

    attrs = finished.attributes
    assert attrs is not None
    assert attrs[GEN_AI_SYSTEM] == "google"
    assert attrs[GEN_AI_REQUEST_MODEL] == "gemini-1.5-flash"
    assert attrs[GEN_AI_USAGE_INPUT_TOKENS] == 120
    assert attrs[GEN_AI_USAGE_OUTPUT_TOKENS] == 60
    assert attrs[GEN_AI_USAGE_TOTAL_TOKENS] == 180
    assert attrs[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.14
    assert attrs[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert attrs[FLOWGUARD_RETRY_COUNT] == 1


@pytest.mark.asyncio
async def test_in_memory_span_exporter_async_context_manager(otel_in_memory: Any) -> None:
    """Verify async context manager sets attributes and records to InMemorySpanExporter."""
    tracer, memory_exporter = otel_in_memory
    exporter = OpenTelemetryExporter(tracer=tracer)

    async with exporter.trace_span_async(system="deepseek", model="deepseek-reasoner") as ctx:
        await asyncio.sleep(0.01)
        ctx.set_tokens(input_tokens=800, output_tokens=400)
        ctx.set_time_to_first_token(0.42)
        ctx.set_circuit_state("CLOSED")
        ctx.set_retry_count(0)

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    finished = spans[0]

    assert finished.name == "chat deepseek-reasoner"
    assert finished.status.status_code == StatusCode.OK

    attrs = finished.attributes
    assert attrs is not None
    assert attrs[GEN_AI_SYSTEM] == "deepseek"
    assert attrs[GEN_AI_REQUEST_MODEL] == "deepseek-reasoner"
    assert attrs[GEN_AI_USAGE_INPUT_TOKENS] == 800
    assert attrs[GEN_AI_USAGE_OUTPUT_TOKENS] == 400
    assert attrs[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.42
    assert attrs[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert attrs[FLOWGUARD_RETRY_COUNT] == 0


def test_export_collector_to_opentelemetry(otel_in_memory: Any) -> None:
    """Verify exporting MetricsCollector summary to OpenTelemetry span."""
    tracer, memory_exporter = otel_in_memory

    collector = MetricsCollector(name="test-pipeline")
    collector.record_success(0.02)
    collector.record_success(0.04)
    collector.record_failure(0.10, "TimeoutError")
    collector.record_ttft(0.035)
    collector.record_rejected("circuit_breaker")

    export_opentelemetry(
        collector=collector,
        tracer=tracer,
        system="openai",
        model="gpt-4o",
        circuit_state=CircuitState.HALF_OPEN,
        retry_count=2,
    )

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    finished = spans[0]

    assert finished.name == "flowguard.pipeline.test-pipeline"
    attrs = finished.attributes
    assert attrs is not None
    assert attrs[GEN_AI_SYSTEM] == "openai"
    assert attrs[GEN_AI_REQUEST_MODEL] == "gpt-4o"
    assert attrs[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.035
    assert attrs[FLOWGUARD_CIRCUIT_STATE] == "HALF_OPEN"
    assert attrs[FLOWGUARD_RETRY_COUNT] == 2
    assert attrs[FLOWGUARD_PIPELINE] == "test-pipeline"
    assert attrs["flowguard.requests_total"] == 4
    assert attrs["flowguard.requests_success_total"] == 2
    assert attrs["flowguard.requests_failure_total"] == 1
    assert attrs["flowguard.rejected.circuit_breaker"] == 1


def test_span_context_methods() -> None:
    """Verify SpanContext helper methods operate on the underlying span."""
    fake_span = FakeSpan("test-span")
    from flowguard.metrics.otel import SpanContext

    ctx = SpanContext(fake_span)
    assert ctx.span is fake_span

    ctx.set_system("openai")
    ctx.set_model("gpt-4o")
    ctx.set_tokens(input_tokens=50, output_tokens=25)
    ctx.set_time_to_first_token(0.12)
    ctx.set_circuit_state(CircuitState.CLOSED)
    ctx.set_retry_count(3)
    ctx.set_attribute("custom.tag", "tag-val")
    ctx.set_attributes({"another.tag": 42})

    exc = RuntimeError("context exc")
    ctx.record_exception(exc)

    assert fake_span.attributes[GEN_AI_SYSTEM] == "openai"
    assert fake_span.attributes[GEN_AI_REQUEST_MODEL] == "gpt-4o"
    assert fake_span.attributes[GEN_AI_USAGE_INPUT_TOKENS] == 50
    assert fake_span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS] == 25
    assert fake_span.attributes[GEN_AI_USAGE_TOTAL_TOKENS] == 75
    assert fake_span.attributes[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] == 0.12
    assert fake_span.attributes[FLOWGUARD_CIRCUIT_STATE] == "CLOSED"
    assert fake_span.attributes[FLOWGUARD_RETRY_COUNT] == 3
    assert fake_span.attributes["custom.tag"] == "tag-val"
    assert fake_span.attributes["another.tag"] == 42
    assert exc in fake_span.exceptions


def test_resolve_span_name_variations() -> None:
    """Verify span name resolution when name, system, or default_system are provided."""
    tracer = FakeTracer()
    exporter = OpenTelemetryExporter(tracer=tracer, default_system="openai")

    # 1. Explicit name takes precedence
    s1 = exporter.export_span(name="explicit.name")
    assert s1.name == "explicit.name"

    # 2. Model provided -> "chat {model}"
    s2 = exporter.export_span(model="gpt-4o")
    assert s2.name == "chat gpt-4o"

    # 3. Only system provided (via default_system) -> "{system}.chat"
    s3 = exporter.export_span()
    assert s3.name == "openai.chat"

    # 4. Neither provided -> "gen_ai.chat"
    bare_exporter = OpenTelemetryExporter(tracer=tracer)
    s4 = bare_exporter.export_span()
    assert s4.name == "gen_ai.chat"


def test_map_genai_attributes_extra_non_primitive() -> None:
    """Verify non-primitive extra attributes are converted to string."""

    class CustomObj:
        def __str__(self) -> str:
            return "custom-object-str"

    mapped = map_genai_attributes(
        system="openai",
        extra_attributes={"obj": CustomObj()},
    )
    assert mapped["obj"] == "custom-object-str"


def test_export_collector_without_ttft(otel_in_memory: Any) -> None:
    """Verify export_collector works when no TTFT latencies were recorded."""
    tracer, memory_exporter = otel_in_memory
    exporter = OpenTelemetryExporter(tracer=tracer)

    collector = MetricsCollector(name="batch-worker")
    collector.record_success(0.05)

    exporter.export_collector(collector=collector)
    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    assert GEN_AI_SERVER_TIME_TO_FIRST_TOKEN not in spans[0].attributes
