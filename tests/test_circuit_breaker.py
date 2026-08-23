import asyncio
import pytest
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.exceptions import CircuitBreakerOpenError


@pytest.mark.asyncio
async def test_circuit_breaker_transitions():
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, half_open_success_threshold=1)
    assert cb.state == CircuitState.CLOSED

    await cb.record_failure(RuntimeError("fail 1"))
    assert cb.state == CircuitState.CLOSED

    await cb.record_failure(RuntimeError("fail 2"))
    assert cb.state == CircuitState.OPEN

    with pytest.raises(CircuitBreakerOpenError):
        await cb.before_call()

    # Wait for recovery timeout
    await asyncio.sleep(0.12)
    assert cb.can_execute() is True

    # before_call will transition to HALF_OPEN
    await cb.before_call()
    assert cb.state == CircuitState.HALF_OPEN

    await cb.record_success()
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_failure():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05, half_open_success_threshold=2)
    await cb.record_failure(RuntimeError("trip"))
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.06)
    await cb.before_call()
    assert cb.state == CircuitState.HALF_OPEN

    # Failure in HALF_OPEN must trip back to OPEN immediately
    await cb.record_failure(RuntimeError("probe failed"))
    assert cb.state == CircuitState.OPEN
