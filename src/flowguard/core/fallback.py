"""Interactive and decision-driven fallback mechanisms for FlowGuard."""

import inspect
import logging
from typing import Any, Callable, Dict, Optional
from flowguard.exceptions import FlowGuardError

logger = logging.getLogger("flowguard.fallback")


class ChoiceFallback:
    """
    Interactive / Decision-driven Fallback Orchestrator.

    When the primary model or service execution trips a circuit breaker or fails,
    ChoiceFallback queries a decision callback (e.g. human-in-the-loop CLI prompt,
    Web UI approval modal, or agent decision engine) to choose from a list of
    candidate alternative models or fallback routes.

    Parameters
    ----------
    candidates : Dict[str, Callable[..., Any]]
        Dictionary mapping route names (e.g. "claude-3.5-sonnet", "deepseek-r1") to executable callables.
    selector : Optional[Callable[[BaseException, List[str]], Any]]
        Sync or async function that asks the user/system to select a candidate key.
        If selector returns None or 'abort', the original exception is re-raised.
    """

    def __init__(
        self,
        candidates: Dict[str, Callable[..., Any]],
        selector: Optional[Callable[..., Any]] = None,
    ) -> None:
        if not candidates:
            raise ValueError("ChoiceFallback requires at least one candidate fallback route")
        self.candidates = candidates
        self.selector = selector

    async def __call__(self, *args: Any, exc: Optional[BaseException] = None, **kwargs: Any) -> Any:
        options = list(self.candidates.keys())

        if self.selector is not None:
            actual_exc = exc or FlowGuardError("Primary service failed")
            try:
                sig = inspect.signature(self.selector)
                if len(sig.parameters) >= 2:
                    choice_res = self.selector(actual_exc, options)
                else:
                    choice_res = self.selector(options)

                if inspect.isawaitable(choice_res):
                    selected_key = await choice_res
                else:
                    selected_key = choice_res
            except Exception as select_err:
                logger.exception("Error during fallback candidate selection: %s", select_err)
                if exc:
                    raise exc
                raise select_err
        else:
            # Default to first candidate if no selector is provided
            selected_key = options[0]

        if not selected_key or str(selected_key).lower() in ("abort", "cancel", "none"):
            logger.info("Fallback execution cancelled by user decision.")
            if exc:
                raise exc
            raise FlowGuardError("Fallback selection cancelled by user")

        if selected_key not in self.candidates:
            raise KeyError(
                f"Selected fallback candidate '{selected_key}' not found in available routes: {options}"
            )

        target_func = self.candidates[selected_key]
        logger.info("Executing chosen fallback route: %s", selected_key)

        try:
            sig = inspect.signature(target_func)
            call_kw = dict(kwargs)
            if "exc" in sig.parameters:
                call_kw["exc"] = exc
            res = target_func(*args, **call_kw)
        except TypeError:
            res = target_func(*args, **kwargs)

        if inspect.isawaitable(res):
            return await res
        return res
