"""Interactive and decision-driven fallback mechanisms for FlowGuard."""

from dataclasses import dataclass
import inspect
import logging
from typing import Any, Callable, Dict, Mapping, Optional, Tuple
from flowguard.exceptions import FlowGuardError

logger = logging.getLogger("flowguard.fallback")


@dataclass(frozen=True)
class FallbackContext:
    """
    Immutable context describing the failed execution attempt passed to fallback handlers.

    Attributes
    ----------
    exception : BaseException
        The exception that triggered the fallback.
    args : Tuple[Any, ...]
        The positional arguments passed to the original function call.
    kwargs : Mapping[str, Any]
        The keyword arguments passed to the original function call.
    pipeline_name : str
        The name of the FlowGuard pipeline executing the call.
    """

    exception: BaseException
    args: Tuple[Any, ...]
    kwargs: Mapping[str, Any]
    pipeline_name: str


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
        # Freeze candidate mapping snapshot (P2-2)
        self._candidates: Dict[str, Callable[..., Any]] = dict(candidates)
        self._selector = selector

    @property
    def candidates(self) -> Dict[str, Callable[..., Any]]:
        """Return a copy of the candidate route dictionary."""
        return dict(self._candidates)

    async def __call__(
        self, context: Optional[FallbackContext] = None, *args: Any, **kwargs: Any
    ) -> Any:
        # Extract or construct FallbackContext (P1-2)
        if isinstance(context, FallbackContext):
            ctx = context
        else:
            exc = kwargs.pop("__flowguard_exc__", None) or FlowGuardError("Primary service failed")
            actual_args = (context, *args) if context is not None else args
            ctx = FallbackContext(
                exception=exc,
                args=actual_args,
                kwargs=kwargs,
                pipeline_name="choice-fallback",
            )

        options = list(self._candidates.keys())

        # Execute selector with explicit context and options (P1-1: single execution, no TypeError guessing)
        try:
            choice_res = self._selector(ctx, options)
            if inspect.isawaitable(choice_res):
                selected_key = await choice_res
            else:
                selected_key = choice_res
        except Exception as select_err:
            logger.exception("Error in ChoiceFallback selector: %s", select_err)
            raise select_err from ctx.exception

        # P2-2: None explicitly signals user cancellation
        if selected_key is None:
            logger.info("Fallback execution cancelled by user decision (selector returned None).")
            raise ctx.exception

        if selected_key not in self._candidates:
            raise KeyError(
                f"Selected fallback candidate '{selected_key}' not found in available routes: {options}"
            )

        target_func = self._candidates[selected_key]
        logger.info("Executing chosen fallback route: %s", selected_key)

        # Inspect target signature statically once (P1-1)
        sig = inspect.signature(target_func)
        params = sig.parameters

        if (
            "context" in params
            or "ctx" in params
            or (len(params) == 1 and list(params.keys())[0] in ("context", "ctx"))
        ):
            res = target_func(ctx)
        else:
            # Bind to original args/kwargs
            res = target_func(*ctx.args, **ctx.kwargs)

        if inspect.isawaitable(res):
            return await res
        return res
