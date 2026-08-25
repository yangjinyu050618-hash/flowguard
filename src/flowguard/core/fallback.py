"""Interactive and decision-driven fallback mechanisms for FlowGuard."""

from dataclasses import dataclass
import inspect
import logging
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Tuple
from flowguard.exceptions import FlowGuardError

logger = logging.getLogger("flowguard.fallback")


@dataclass(frozen=True)
class FallbackContext:
    """
    Truly immutable context describing the failed execution attempt passed to fallback handlers.

    Attributes
    ----------
    exception : BaseException
        The exception that triggered the fallback.
    args : Tuple[Any, ...]
        The positional arguments passed to the original function call.
    kwargs : Mapping[str, Any]
        The immutable keyword arguments passed to the original function call.
    pipeline_name : str
        The name of the FlowGuard pipeline executing the call.
    """

    exception: BaseException
    args: Tuple[Any, ...]
    kwargs: Mapping[str, Any]
    pipeline_name: str

    def __post_init__(self) -> None:
        # Guarantee true runtime immutability via MappingProxyType
        if not isinstance(self.kwargs, MappingProxyType):
            object.__setattr__(self, "kwargs", MappingProxyType(dict(self.kwargs)))


def with_fallback_context(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to explicitly mark a fallback function as expecting a FallbackContext as its single argument."""
    setattr(func, "__flowguard_context_handler__", True)
    return func


def is_context_handler(func: Callable[..., Any]) -> bool:
    """Check if a callable explicitly expects FallbackContext via annotation, attribute, or ChoiceFallback type."""
    if isinstance(func, ChoiceFallback):
        return True
    if getattr(func, "__flowguard_context_handler__", False):
        return True
    try:
        sig = inspect.signature(func)
        for param in sig.parameters.values():
            if (
                param.annotation is FallbackContext
                or getattr(param.annotation, "__name__", "") == "FallbackContext"
            ):
                return True
    except (ValueError, TypeError):
        pass
    return False


class ChoiceFallback:
    """
    Interactive / Decision-driven Fallback Orchestrator (Human-in-the-Loop).

    When the primary model or service execution trips a circuit breaker or fails,
    ChoiceFallback queries a decision callback (e.g. human-in-the-loop CLI prompt,
    Web UI approval modal, or agent decision engine) to choose from a list of
    candidate alternative models or fallback routes.

    Parameters
    ----------
    candidates : Mapping[str, Callable[..., Any]]
        Mapping of route names (e.g. "claude-3.5-sonnet", "deepseek-r1") to executable callables.
        The candidate map is frozen at initialization.
    selector : Callable[[FallbackContext, List[str]], Any]
        Sync or async function that selects a candidate key given the FallbackContext and available options.
        Returning None signals cancellation, re-raising the original exception without side-effects.
    """

    def __init__(
        self,
        candidates: Mapping[str, Callable[..., Any]],
        selector: Callable[..., Any],
    ) -> None:
        if not candidates:
            raise ValueError("ChoiceFallback requires at least one candidate fallback route")
        if not callable(selector):
            raise TypeError("ChoiceFallback requires a callable selector(context, options)")
        # Freeze candidate mapping snapshot
        self._candidates: Dict[str, Callable[..., Any]] = dict(candidates)
        self._selector = selector

    @property
    def candidates(self) -> Dict[str, Callable[..., Any]]:
        """Return a copy of the candidate route dictionary."""
        return dict(self._candidates)

    async def __call__(self, context: FallbackContext, *args: Any, **kwargs: Any) -> Any:
        if not isinstance(context, FallbackContext):
            exc = kwargs.pop("__flowguard_exc__", None) or FlowGuardError("Primary service failed")
            actual_args = (context, *args) if context is not None else args
            ctx = FallbackContext(
                exception=exc,
                args=actual_args,
                kwargs=kwargs,
                pipeline_name="choice-fallback",
            )
        else:
            ctx = context

        options = list(self._candidates.keys())

        # Execute selector with explicit context and options
        try:
            choice_res = self._selector(ctx, options)
            if inspect.isawaitable(choice_res):
                selected_key = await choice_res
            else:
                selected_key = choice_res
        except Exception as select_err:
            logger.exception("Error in ChoiceFallback selector: %s", select_err)
            raise select_err from ctx.exception

        if selected_key is None:
            logger.info("Fallback execution cancelled by user decision (selector returned None).")
            raise ctx.exception

        if selected_key not in self._candidates:
            raise KeyError(
                f"Selected fallback candidate '{selected_key}' not found in available routes: {options}"
            )

        target_func = self._candidates[selected_key]
        logger.info("Executing chosen fallback route: %s", selected_key)

        # Check if candidate explicitly expects FallbackContext or original args
        if is_context_handler(target_func):
            res = target_func(ctx)
        else:
            # Bind to original args/kwargs without parameter name guessing
            try:
                sig = inspect.signature(target_func)
                if "exc" in sig.parameters and "exc" not in ctx.kwargs:
                    res = target_func(*ctx.args, exc=ctx.exception, **ctx.kwargs)
                else:
                    res = target_func(*ctx.args, **ctx.kwargs)
            except (ValueError, TypeError):
                res = target_func(*ctx.args, **ctx.kwargs)

        if inspect.isawaitable(res):
            return await res
        return res
