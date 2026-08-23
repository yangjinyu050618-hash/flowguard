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
    assert cb.state == CircuitState.HALF_OPEN

    await cb.record_success()
    assert cb.state == CircuitState.CLOSED
