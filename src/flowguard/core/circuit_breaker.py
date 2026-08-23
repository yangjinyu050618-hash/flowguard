"""Sliding-window Circuit Breaker with Half-Open probe state machine."""

import asyncio
import enum
import time
from typing import Callable, List, Optional, Tuple, Type
from flowguard.exceptions import CircuitBreakerOpenError


class CircuitState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Resilient Circuit Breaker implementation.

    Parameters
    ----------
    failure_threshold : int
        Consecutive or windowed failures required to trip circuit OPEN. Default: 5.
    recovery_timeout : float
        Cooldown time in seconds before transitioning OPEN -> HALF_OPEN. Default: 30.0s.
    half_open_success_threshold : int
        Consecutive successful probe calls needed to close circuit. Default: 2.
    expected_exceptions : Tuple[Type[Exception], ...]
        Exceptions treated as faults. Defaults to (Exception,).
    on_state_change : Optional[Callable[[CircuitState, CircuitState], None]]
        Optional callback triggered on state transitions.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_success_threshold: int = 2,
        expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.expected_exceptions = expected_exceptions
        self.on_state_change = on_state_change

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_successes = 0
        self.opened_at: Optional[float] = None
        self._lock = asyncio.Lock()

    def _set_state(self, new_state: CircuitState) -> None:
        if self.state != new_state:
            old_state = self.state
            self.state = new_state
            if self.on_state_change:
                try:
                    self.on_state_change(old_state, new_state)
                except Exception:
                    pass

    def can_execute(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True

        now = time.monotonic()
        if self.state == CircuitState.OPEN:
            if self.opened_at and (now - self.opened_at >= self.recovery_timeout):
                self._set_state(CircuitState.HALF_OPEN)
                self.half_open_successes = 0
                return True
            return False

        if self.state == CircuitState.HALF_OPEN:
            return True

        return False

    async def before_call(self) -> None:
        async with self._lock:
            if not self.can_execute():
                remaining = 0.0
                if self.opened_at:
                    remaining = max(0.0, self.recovery_timeout - (time.monotonic() - self.opened_at))
                raise CircuitBreakerOpenError(
                    f"Circuit breaker is OPEN. Calls blocked for another {remaining:.1f}s",
                    reset_timeout=remaining,
                )

    async def record_success(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_success_threshold:
                    self.failure_count = 0
                    self.opened_at = None
                    self._set_state(CircuitState.CLOSED)
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0

    async def record_failure(self, exc: BaseException) -> None:
        if not isinstance(exc, self.expected_exceptions):
            return

        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.opened_at = time.monotonic()
                self._set_state(CircuitState.OPEN)
            elif self.state == CircuitState.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.opened_at = time.monotonic()
                    self._set_state(CircuitState.OPEN)

    def reset(self) -> None:
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_successes = 0
        self.opened_at = None
