"""OpenTelemetry GenAI semantic conventions and FlowGuard resilience telemetry exporter."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
import logging
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterator,
    Optional,
    Protocol,
    Union,
)

from flowguard.core.circuit_breaker import CircuitState
from flowguard.metrics.collector import MetricsCollector

logger = logging.getLogger("flowguard.metrics.otel")

# Standard OpenTelemetry GenAI Semantic Convention Attribute Keys
GEN_AI_SYSTEM = "gen_ai.system"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_RESPONSE_MODEL = "gen_ai.response.model"
GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_USAGE_TOTAL_TOKENS = "gen_ai.usage.total_tokens"
GEN_AI_SERVER_TIME_TO_FIRST_TOKEN = "gen_ai.server.time_to_first_token"

# FlowGuard Resilience Telemetry Attribute Keys
FLOWGUARD_CIRCUIT_STATE = "flowguard.circuit_state"
FLOWGUARD_RETRY_COUNT = "flowguard.retry_count"
FLOWGUARD_PIPELINE = "flowguard.pipeline"


class SpanLike(Protocol):
    """Protocol mirroring the essential OpenTelemetry Span interface."""

    def set_attribute(self, key: str, value: Union[str, int, float, bool]) -> None: ...

    def set_status(self, status: Any) -> None: ...

    def record_exception(self, exception: BaseException) -> None: ...

    def end(self, end_time: Optional[int] = None) -> None: ...


class TracerLike(Protocol):
    """Protocol mirroring the essential OpenTelemetry Tracer interface."""

    def start_span(
        self,
        name: str,
        start_time: Optional[int] = None,
        **kwargs: Any,
    ) -> Any: ...


def is_opentelemetry_available() -> bool:
    """Check if opentelemetry trace API is installed and importable."""
    try:
        import opentelemetry.trace  # noqa: F401

        return True
    except (ImportError, ModuleNotFoundError):
        return False


def get_opentelemetry_trace() -> Any:
    """
    Dynamically import and return the opentelemetry.trace module.

    Raises
    ------
    ImportError
        Friendly error if opentelemetry trace APIs are not installed.
    """
    try:
        from opentelemetry import trace

        return trace
    except (ImportError, ModuleNotFoundError) as exc:
        raise ImportError(
            "OpenTelemetry support requires the 'opentelemetry-api' and 'opentelemetry-sdk' packages. "
            "Install them via: pip install opentelemetry-api opentelemetry-sdk or pip install 'flowguard-core[otel]'"
        ) from exc


def get_tracer(
    instrumenting_module_name: str = "flowguard",
    instrumenting_library_version: Optional[str] = None,
    tracer_provider: Optional[Any] = None,
) -> Any:
    """
    Dynamically acquire an OpenTelemetry Tracer instance.

    Raises
    ------
    ImportError
        Friendly error if opentelemetry trace APIs are not installed.
    """
    trace_mod = get_opentelemetry_trace()
    return trace_mod.get_tracer(
        instrumenting_module_name=instrumenting_module_name,
        instrumenting_library_version=instrumenting_library_version,
        tracer_provider=tracer_provider,
    )


def _set_span_status(span: Any, is_error: bool, description: Optional[str] = None) -> None:
    """Helper to set span status safely without throwing if status classes are missing."""
    if not hasattr(span, "set_status"):
        return
    try:
        from opentelemetry.trace import Status, StatusCode

        if is_error:
            span.set_status(Status(StatusCode.ERROR, description or "Error"))
        else:
            span.set_status(Status(StatusCode.OK))
    except (ImportError, ModuleNotFoundError, Exception):
        # Fallback for mock spans or non-OTel status handlers
        try:
            span.set_status("ERROR" if is_error else "OK")
        except Exception:
            pass


@dataclass
class GenAIMetrics:
    """Structured container for GenAI semantic conventions and FlowGuard telemetry."""

    system: Optional[str] = None
    model: Optional[str] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    time_to_first_token: Optional[float] = None
    circuit_state: Optional[Union[CircuitState, str]] = None
    retry_count: Optional[int] = None
    total_tokens: Optional[int] = None
    response_model: Optional[str] = None
    pipeline: Optional[str] = None
    extra_attributes: Dict[str, Any] = field(default_factory=dict)

    def to_attributes(self) -> Dict[str, Union[str, int, float, bool]]:
        """Convert container into standardized OpenTelemetry attribute dictionary."""
        return map_genai_attributes(self)


def map_genai_attributes(
    metrics: Optional[Union[Dict[str, Any], GenAIMetrics]] = None,
    *,
    system: Optional[str] = None,
    model: Optional[str] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    time_to_first_token: Optional[float] = None,
    circuit_state: Optional[Union[CircuitState, str]] = None,
    retry_count: Optional[int] = None,
    pipeline: Optional[str] = None,
    total_tokens: Optional[int] = None,
    response_model: Optional[str] = None,
    extra_attributes: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Union[str, int, float, bool]]:
    """
    Map GenAI parameters and FlowGuard resilience state to standard OpenTelemetry attributes.

    Maps:
      - gen_ai.system (str)
      - gen_ai.request.model (str)
      - gen_ai.usage.input_tokens (int)
      - gen_ai.usage.output_tokens (int)
      - gen_ai.usage.total_tokens (int, optional / derived)
      - gen_ai.server.time_to_first_token (float)
      - flowguard.circuit_state (str)
      - flowguard.retry_count (int)
      - flowguard.pipeline (str, optional)
    """
    src_system = system
    src_model = model
    src_input_tokens = input_tokens
    src_output_tokens = output_tokens
    src_ttft = time_to_first_token
    src_circuit_state = circuit_state
    src_retry_count = retry_count
    src_pipeline = pipeline
    src_total_tokens = total_tokens
    src_response_model = response_model
    extras: Dict[str, Any] = {}

    if isinstance(metrics, GenAIMetrics):
        if src_system is None:
            src_system = metrics.system
        if src_model is None:
            src_model = metrics.model
        if src_input_tokens is None:
            src_input_tokens = metrics.input_tokens
        if src_output_tokens is None:
            src_output_tokens = metrics.output_tokens
        if src_ttft is None:
            src_ttft = metrics.time_to_first_token
        if src_circuit_state is None:
            src_circuit_state = metrics.circuit_state
        if src_retry_count is None:
            src_retry_count = metrics.retry_count
        if src_pipeline is None:
            src_pipeline = metrics.pipeline
        if src_total_tokens is None:
            src_total_tokens = metrics.total_tokens
        if src_response_model is None:
            src_response_model = metrics.response_model
        if metrics.extra_attributes:
            extras.update(metrics.extra_attributes)

    elif isinstance(metrics, dict):
        if src_system is None:
            val = metrics.get("system")
            if val is None:
                val = metrics.get(GEN_AI_SYSTEM)
            src_system = val
        if src_model is None:
            val = metrics.get("model")
            if val is None:
                val = metrics.get(GEN_AI_REQUEST_MODEL)
            src_model = val
        if src_input_tokens is None:
            val = metrics.get("input_tokens")
            if val is None:
                val = metrics.get(GEN_AI_USAGE_INPUT_TOKENS)
            src_input_tokens = val
        if src_output_tokens is None:
            val = metrics.get("output_tokens")
            if val is None:
                val = metrics.get(GEN_AI_USAGE_OUTPUT_TOKENS)
            src_output_tokens = val
        if src_ttft is None:
            val = metrics.get("time_to_first_token")
            if val is None:
                val = metrics.get("ttft")
            if val is None:
                val = metrics.get(GEN_AI_SERVER_TIME_TO_FIRST_TOKEN)
            src_ttft = val
        if src_circuit_state is None:
            val = metrics.get("circuit_state")
            if val is None:
                val = metrics.get(FLOWGUARD_CIRCUIT_STATE)
            src_circuit_state = val
        if src_retry_count is None:
            val = metrics.get("retry_count")
            if val is None:
                val = metrics.get(FLOWGUARD_RETRY_COUNT)
            src_retry_count = val
        if src_pipeline is None:
            val = metrics.get("pipeline")
            if val is None:
                val = metrics.get(FLOWGUARD_PIPELINE)
            src_pipeline = val
        if src_total_tokens is None:
            val = metrics.get("total_tokens")
            if val is None:
                val = metrics.get(GEN_AI_USAGE_TOTAL_TOKENS)
            src_total_tokens = val
        if src_response_model is None:
            val = metrics.get("response_model")
            if val is None:
                val = metrics.get(GEN_AI_RESPONSE_MODEL)
            src_response_model = val

        for k, v in metrics.items():
            if k not in {
                "system",
                "model",
                "input_tokens",
                "output_tokens",
                "time_to_first_token",
                "ttft",
                "circuit_state",
                "retry_count",
                "pipeline",
                "total_tokens",
                "response_model",
                GEN_AI_SYSTEM,
                GEN_AI_REQUEST_MODEL,
                GEN_AI_USAGE_INPUT_TOKENS,
                GEN_AI_USAGE_OUTPUT_TOKENS,
                GEN_AI_SERVER_TIME_TO_FIRST_TOKEN,
                FLOWGUARD_CIRCUIT_STATE,
                FLOWGUARD_RETRY_COUNT,
                FLOWGUARD_PIPELINE,
                GEN_AI_USAGE_TOTAL_TOKENS,
                GEN_AI_RESPONSE_MODEL,
            }:
                extras[k] = v

    if extra_attributes:
        extras.update(extra_attributes)
    if kwargs:
        extras.update(kwargs)

    result: Dict[str, Union[str, int, float, bool]] = {}

    if src_system is not None:
        result[GEN_AI_SYSTEM] = str(src_system)

    if src_model is not None:
        result[GEN_AI_REQUEST_MODEL] = str(src_model)

    if src_response_model is not None:
        result[GEN_AI_RESPONSE_MODEL] = str(src_response_model)

    if src_input_tokens is not None:
        result[GEN_AI_USAGE_INPUT_TOKENS] = int(src_input_tokens)

    if src_output_tokens is not None:
        result[GEN_AI_USAGE_OUTPUT_TOKENS] = int(src_output_tokens)

    if src_total_tokens is not None:
        result[GEN_AI_USAGE_TOTAL_TOKENS] = int(src_total_tokens)
    elif src_input_tokens is not None and src_output_tokens is not None:
        result[GEN_AI_USAGE_TOTAL_TOKENS] = int(src_input_tokens) + int(src_output_tokens)

    if src_ttft is not None:
        result[GEN_AI_SERVER_TIME_TO_FIRST_TOKEN] = float(src_ttft)

    if src_circuit_state is not None:
        if isinstance(src_circuit_state, CircuitState):
            result[FLOWGUARD_CIRCUIT_STATE] = src_circuit_state.value
        else:
            result[FLOWGUARD_CIRCUIT_STATE] = str(src_circuit_state)

    if src_retry_count is not None:
        result[FLOWGUARD_RETRY_COUNT] = int(src_retry_count)

    if src_pipeline is not None:
        result[FLOWGUARD_PIPELINE] = str(src_pipeline)

    for k, v in extras.items():
        if v is not None:
            if isinstance(v, (str, int, float, bool)):
                result[k] = v
            else:
                result[k] = str(v)

    return result


class SpanContext:
    """Fluent wrapper around an active OpenTelemetry span providing typed attribute setters."""

    def __init__(self, span: Any) -> None:
        self._span = span

    @property
    def span(self) -> Any:
        """Access underlying OpenTelemetry span."""
        return self._span

    def set_system(self, system: str) -> SpanContext:
        """Set gen_ai.system attribute."""
        self._span.set_attribute(GEN_AI_SYSTEM, str(system))
        return self

    def set_model(self, model: str) -> SpanContext:
        """Set gen_ai.request.model attribute."""
        self._span.set_attribute(GEN_AI_REQUEST_MODEL, str(model))
        return self

    def set_input_tokens(self, tokens: int) -> SpanContext:
        """Set gen_ai.usage.input_tokens attribute."""
        self._span.set_attribute(GEN_AI_USAGE_INPUT_TOKENS, int(tokens))
        return self

    def set_output_tokens(self, tokens: int) -> SpanContext:
        """Set gen_ai.usage.output_tokens attribute."""
        self._span.set_attribute(GEN_AI_USAGE_OUTPUT_TOKENS, int(tokens))
        return self

    def set_tokens(
        self, input_tokens: Optional[int] = None, output_tokens: Optional[int] = None
    ) -> SpanContext:
        """Set input, output, and computed total tokens."""
        if input_tokens is not None:
            self.set_input_tokens(input_tokens)
        if output_tokens is not None:
            self.set_output_tokens(output_tokens)
        if input_tokens is not None and output_tokens is not None:
            self._span.set_attribute(
                GEN_AI_USAGE_TOTAL_TOKENS, int(input_tokens) + int(output_tokens)
            )
        return self

    def set_time_to_first_token(self, ttft: float) -> SpanContext:
        """Set gen_ai.server.time_to_first_token attribute in seconds."""
        self._span.set_attribute(GEN_AI_SERVER_TIME_TO_FIRST_TOKEN, float(ttft))
        return self

    def set_circuit_state(self, state: Union[CircuitState, str]) -> SpanContext:
        """Set flowguard.circuit_state attribute."""
        val = state.value if isinstance(state, CircuitState) else str(state)
        self._span.set_attribute(FLOWGUARD_CIRCUIT_STATE, val)
        return self

    def set_retry_count(self, retry_count: int) -> SpanContext:
        """Set flowguard.retry_count attribute."""
        self._span.set_attribute(FLOWGUARD_RETRY_COUNT, int(retry_count))
        return self

    def set_attribute(self, key: str, value: Union[str, int, float, bool]) -> SpanContext:
        """Set arbitrary span attribute."""
        self._span.set_attribute(key, value)
        return self

    def set_attributes(self, attributes: Dict[str, Union[str, int, float, bool]]) -> SpanContext:
        """Set multiple attributes from dictionary."""
        for k, v in attributes.items():
            self._span.set_attribute(k, v)
        return self

    def record_metrics(
        self,
        metrics: Optional[Union[Dict[str, Any], GenAIMetrics]] = None,
        *,
        system: Optional[str] = None,
        model: Optional[str] = None,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        time_to_first_token: Optional[float] = None,
        circuit_state: Optional[Union[CircuitState, str]] = None,
        retry_count: Optional[int] = None,
        **kwargs: Any,
    ) -> SpanContext:
        """Record standard GenAI and FlowGuard metrics onto this span."""
        attrs = map_genai_attributes(
            metrics,
            system=system,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            time_to_first_token=time_to_first_token,
            circuit_state=circuit_state,
            retry_count=retry_count,
            **kwargs,
        )
        self.set_attributes(attrs)
        return self

    def record_exception(self, exception: BaseException) -> SpanContext:
        """Record exception on span if supported."""
        if hasattr(self._span, "record_exception"):
            self._span.record_exception(exception)
        return self


class OpenTelemetryExporter:
    """
    OpenTelemetry Telemetry Exporter mapping GenAI semantic conventions and FlowGuard resilience state.

    Supports:
    1. Direct span export (export_span).
    2. Synchronous context management (trace_span).
    3. Asynchronous context management (trace_span_async).
    4. FlowGuard MetricsCollector integration (export_collector).
    """

    def __init__(
        self,
        tracer: Optional[Any] = None,
        tracer_name: str = "flowguard",
        tracer_version: Optional[str] = None,
        default_system: Optional[str] = None,
    ) -> None:
        if tracer is not None:
            self.tracer = tracer
        else:
            self.tracer = get_tracer(
                instrumenting_module_name=tracer_name,
                instrumenting_library_version=tracer_version,
            )
        self.tracer_name = tracer_name
        self.default_system = default_system

    def _resolve_span_name(
        self,
        name: Optional[str],
        model: Optional[str],
        system: Optional[str],
    ) -> str:
        if name:
            return name
        eff_system = system or self.default_system
        if model:
            return f"chat {model}"
        if eff_system:
            return f"{eff_system}.chat"
        return "gen_ai.chat"

    def export_span(
        self,
        name: Optional[str] = None,
        *,
        system: Optional[str] = None,
        model: Optional[str] = None,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        time_to_first_token: Optional[float] = None,
        circuit_state: Optional[Union[CircuitState, str]] = None,
        retry_count: Optional[int] = None,
        error: Optional[BaseException] = None,
        attributes: Optional[Dict[str, Any]] = None,
        metrics: Optional[Union[Dict[str, Any], GenAIMetrics]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Export a single completed OpenTelemetry span with GenAI and FlowGuard telemetry.
        """
        eff_system = system or self.default_system
        span_name = self._resolve_span_name(name, model, eff_system)

        span = self.tracer.start_span(name=span_name, start_time=start_time)

        mapped_attrs = map_genai_attributes(
            metrics,
            system=eff_system,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            time_to_first_token=time_to_first_token,
            circuit_state=circuit_state,
            retry_count=retry_count,
            extra_attributes=attributes,
            **kwargs,
        )

        for k, v in mapped_attrs.items():
            span.set_attribute(k, v)

        if error is not None:
            if hasattr(span, "record_exception"):
                span.record_exception(error)
            _set_span_status(span, is_error=True, description=str(error))
        else:
            _set_span_status(span, is_error=False)

        span.end(end_time=end_time)
        return span

    @contextlib.contextmanager
    def trace_span(
        self,
        name: Optional[str] = None,
        *,
        system: Optional[str] = None,
        model: Optional[str] = None,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        time_to_first_token: Optional[float] = None,
        circuit_state: Optional[Union[CircuitState, str]] = None,
        retry_count: Optional[int] = None,
        attributes: Optional[Dict[str, Any]] = None,
        start_time: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[SpanContext]:
        """
        Synchronous context manager wrapping a block in an OpenTelemetry span.
        """
        eff_system = system or self.default_system
        span_name = self._resolve_span_name(name, model, eff_system)

        span = self.tracer.start_span(name=span_name, start_time=start_time)
        ctx = SpanContext(span)

        ctx.record_metrics(
            system=eff_system,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            time_to_first_token=time_to_first_token,
            circuit_state=circuit_state,
            retry_count=retry_count,
            extra_attributes=attributes,
            **kwargs,
        )

        try:
            yield ctx
            _set_span_status(span, is_error=False)
        except BaseException as exc:
            if hasattr(span, "record_exception"):
                span.record_exception(exc)
            _set_span_status(span, is_error=True, description=str(exc))
            raise
        finally:
            span.end()

    @contextlib.asynccontextmanager
    async def trace_span_async(
        self,
        name: Optional[str] = None,
        *,
        system: Optional[str] = None,
        model: Optional[str] = None,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        time_to_first_token: Optional[float] = None,
        circuit_state: Optional[Union[CircuitState, str]] = None,
        retry_count: Optional[int] = None,
        attributes: Optional[Dict[str, Any]] = None,
        start_time: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncIterator[SpanContext]:
        """
        Asynchronous context manager wrapping a coroutine in an OpenTelemetry span.
        """
        eff_system = system or self.default_system
        span_name = self._resolve_span_name(name, model, eff_system)

        span = self.tracer.start_span(name=span_name, start_time=start_time)
        ctx = SpanContext(span)

        ctx.record_metrics(
            system=eff_system,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            time_to_first_token=time_to_first_token,
            circuit_state=circuit_state,
            retry_count=retry_count,
            extra_attributes=attributes,
            **kwargs,
        )

        try:
            yield ctx
            _set_span_status(span, is_error=False)
        except BaseException as exc:
            if hasattr(span, "record_exception"):
                span.record_exception(exc)
            _set_span_status(span, is_error=True, description=str(exc))
            raise
        finally:
            span.end()

    def export_collector(
        self,
        collector: MetricsCollector,
        name: Optional[str] = None,
        *,
        system: Optional[str] = None,
        model: Optional[str] = None,
        circuit_state: Optional[Union[CircuitState, str]] = None,
        retry_count: Optional[int] = None,
    ) -> Any:
        """
        Export summary metrics from a FlowGuard MetricsCollector into an OpenTelemetry span.
        """
        summary = collector.get_summary()
        pipeline_name = summary.get("name", "default")
        span_name = name or f"flowguard.pipeline.{pipeline_name}"

        ttft_sec: Optional[float] = None
        if "ttft_p50_ms" in summary:
            ttft_sec = float(summary["ttft_p50_ms"]) / 1000.0

        collector_attrs: Dict[str, Any] = {
            FLOWGUARD_PIPELINE: pipeline_name,
            "flowguard.requests_total": summary.get("total_requests", 0),
            "flowguard.requests_success_total": summary.get("success_count", 0),
            "flowguard.requests_failure_total": summary.get("failure_count", 0),
            "flowguard.latency_avg_ms": summary.get("latency_avg_ms", 0.0),
            "flowguard.latency_p50_ms": summary.get("latency_p50_ms", 0.0),
            "flowguard.latency_p95_ms": summary.get("latency_p95_ms", 0.0),
            "flowguard.latency_p99_ms": summary.get("latency_p99_ms", 0.0),
        }

        for reason, count in summary.get("rejected_count", {}).items():
            collector_attrs[f"flowguard.rejected.{reason}"] = count

        return self.export_span(
            name=span_name,
            system=system,
            model=model,
            time_to_first_token=ttft_sec,
            circuit_state=circuit_state,
            retry_count=retry_count,
            attributes=collector_attrs,
        )


def export_opentelemetry(
    collector: MetricsCollector,
    tracer: Optional[Any] = None,
    name: Optional[str] = None,
    *,
    system: Optional[str] = None,
    model: Optional[str] = None,
    circuit_state: Optional[Union[CircuitState, str]] = None,
    retry_count: Optional[int] = None,
) -> Any:
    """
    Convenience function to export a FlowGuard MetricsCollector summary via OpenTelemetry.
    """
    exporter = OpenTelemetryExporter(tracer=tracer)
    return exporter.export_collector(
        collector=collector,
        name=name,
        system=system,
        model=model,
        circuit_state=circuit_state,
        retry_count=retry_count,
    )
