"""Sliding-window Circuit Breaker with Half-Open probe state machine and structured logging."""

import asyncio
import enum
import logging
import time
from typing import Callable, Optional, Tuple, Type
from flowguard.exceptions import CircuitBreakerOpenError

logger = logging.getLogger("flowguard.circuit_breaker")


class CircuitState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Resilient Circuit Breaker implementation with Half-Open probe concurrency throttling.

    Parameters
    ----------
    failure_threshold : int
        Consecutive failures required to trip circuit OPEN. Default: 5.
    recovery_timeout : float
        Cooldown time in seconds before transitioning OPEN -> HALF_OPEN. Default: 30.0s.
    half_open_success_threshold : int
        Consecutive successful probe calls needed to close circuit. Default: 2.
    half_open_max_probes : Optional[int]
        Maximum concurrent probe requests allowed in HALF_OPEN state (default: half_open_success_threshold).
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
        half_open_max_probes: Optional[int] = None,
        expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.half_open_max_probes = half_open_max_probes or half_open_success_threshold
        self.expected_exceptions = expected_exceptions
        self.on_state_change = on_state_change

        self._state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_successes = 0
        self._half_open_inflight = 0
        self.opened_at: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Thread-safe snapshot of circuit state."""
        return self._state

    def can_execute(self) -> bool:
        """Safe non-mutating check if calls are allowed or recovery period has elapsed."""
        if self._state == CircuitState.CLOSED:
            return True
        now = time.monotonic()
        if self._state == CircuitState.OPEN:
            return bool(self.opened_at is not None and (now - self.opened_at >= self.recovery_timeout))
        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_inflight < self.half_open_max_probes
        return False

    def _set_state(self, new_state: CircuitState) -> None:
        if self._state != new_state:
            old_state = self._state
            self._state = new_state
            logger.info("Circuit breaker state transition: %s -> %s", old_state.value, new_state.value)
            if self.on_state_change:
                try:
                    self.on_state_change(old_state, new_state)
                except Exception:
                    logger.exception("Exception in on_state_change callback")

    async def before_call(self) -> None:
        async with self._lock:
            now = time.monotonic()

            if self._state == CircuitState.OPEN:
                if self.opened_at is not None and (now - self.opened_at >= self.recovery_timeout):
                    self._set_state(CircuitState.HALF_OPEN)
                    self.half_open_successes = 0
                    self._half_open_inflight = 0
                else:
                    remaining = 0.0
                    if self.opened_at is not None:
                        remaining = max(0.0, self.recovery_timeout - (now - self.opened_at))
                    logger.warning("CircuitBreaker is OPEN. Rejecting call (remaining: %.1fs)", remaining)
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker is OPEN. Calls blocked for another {remaining:.1f}s",
                        reset_timeout=remaining,
                    )

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_inflight >= self.half_open_max_probes:
                    logger.warning(
                        "CircuitBreaker HALF_OPEN probe limit reached (%d/%d)",
                        self._half_open_inflight,
                        self.half_open_max_probes,
                    )
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker HALF_OPEN probe limit reached ({self._half_open_inflight}/{self.half_open_max_probes})"
                    )
                self._half_open_inflight += 1

    async def record_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_success_threshold:
                    self.failure_count = 0
                    self.opened_at = None
                    self._half_open_inflight = 0
                    self._set_state(CircuitState.CLOSED)
            elif self._state == CircuitState.CLOSED:
                self.failure_count = 0

    async def record_failure(self, exc: BaseException) -> None:
        if not isinstance(exc, self.expected_exceptions):
            return

        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                self.opened_at = time.monotonic()
                logger.warning("Probe failed during HALF_OPEN: tripping circuit back to OPEN")
                self._set_state(CircuitState.OPEN)
            elif self._state == CircuitState.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.opened_at = time.monotonic()
                    logger.warning("Failure threshold (%d) reached: tripping circuit to OPEN", self.failure_threshold)
                    self._set_state(CircuitState.OPEN)

    async def reset(self) -> None:
        async with self._lock:
            self.failure_count = 0
            self.half_open_successes = 0
            self._half_open_inflight = 0
            self.opened_at = None
            self._set_state(CircuitState.CLOSED)
