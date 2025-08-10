import time

from nzbidx_api.middleware_circuit import CircuitBreaker, CircuitOpenError


def test_breaker_trips_and_recovers() -> None:
    breaker = CircuitBreaker(max_failures=1, reset_seconds=0.1, retries=0)

    def boom() -> None:
        raise RuntimeError("boom")

    try:
        breaker.call(boom)
    except RuntimeError:
        pass
    # Circuit should now be open
    try:
        breaker.call(lambda: None)
    except CircuitOpenError:
        pass
    # After reset period, circuit closes
    time.sleep(0.2)
    assert breaker.call(lambda: "ok") == "ok"
