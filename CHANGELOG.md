# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-08-20

### Added
- Added `ResilientOpenAI` adapter supporting dual-axis RPM (Requests Per Minute) and TPM (Tokens Per Minute) throttling.
- Added Prometheus text exposition format exporter (`export_prometheus`).
- Added CLI benchmark command `flowguard benchmark`.

### Changed
- Improved `TokenBucketLimiter` refill accuracy using monotonic time clocks.
- Enhanced `Bulkhead` queue handling with explicit timeout exceptions.

### Fixed
- Fixed edge case where rapid half-open circuit breaker state changes could cause race conditions under high concurrency.

## [0.2.0] - 2026-07-15

### Added
- Integrated `@guard` all-in-one pipeline decorator.
- Added `Bulkhead` concurrency governor.
- Added sliding window log rate limiter (`SlidingWindowLimiter`).
- Added in-memory latency percentiles telemetry (`MetricsCollector`).

## [0.1.1] - 2026-06-02

### Added
- Jitter strategies: Full Jitter, Equal Jitter, and Decorrelated Jitter for `ExponentialBackoff`.
- Custom exception hierarchy (`RateLimitExceededError`, `CircuitBreakerOpenError`, `BulkheadFullError`).

## [0.1.0] - 2026-05-10

### Initial Release
- Core `TokenBucketLimiter` async algorithm.
- Core `CircuitBreaker` state machine (`CLOSED`, `OPEN`, `HALF_OPEN`).
- Basic `RetryPolicy` with exponential backoff.
- MIT Open Source license and standard packaging.
