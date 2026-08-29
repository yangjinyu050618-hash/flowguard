# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-08-25

### Added
- Added `ChoiceFallback` for interactive human-in-the-loop fallback routing.
- Added immutable `FallbackContext` metadata for context-aware fallback handlers.
- Added native async adapters for Anthropic Claude and Google Gemini.
- Added graceful fallback examples and multi-LLM orchestration examples.
- Added a daily multi-platform CI health check.

### Changed
- Made FlowGuard the sole retry owner for OpenAI and Anthropic adapters by disabling SDK retries on derived clients.
- Expanded fallback dispatch to support explicit `FallbackContext` annotations without replacing ordinary business arguments.

### Fixed
- Prevented cancellation from triggering fallback work or leaking circuit-breaker probe slots.
- Prevented synchronous fallback and candidate `TypeError` exceptions from causing duplicate execution.
- Preserved caller keyword arguments and per-request token estimates in fallback context.
- Corrected Google GenAI client-error classification and async-client validation.
- Tightened context-handler detection to ignore return annotations and unrelated similarly named types.

## [0.2.2] - 2026-08-23

### Fixed
- Fixed FIFO queue ordering in `TokenBucketLimiter` and `SlidingWindowLimiter` eliminating wait-race starvation.
- Fixed `CircuitBreaker` half-open probe stampede by adding concurrent probe throttling.
- Fixed pipeline execution hierarchy to ensure rate limits and bulkhead isolation are enforced on every retry attempt.
- Fixed exception propagation by ensuring `FlowGuardError` subclasses fail fast without unintended retry loops.
- Fixed `MaxRetriesExceededError` exception cause chaining (`raise ... from last_exc`).
- Calibrated TPM burst capacity in `ResilientOpenAI` adapter.

### Added
- Added `failure_by_type` telemetry in `MetricsCollector` and Prometheus exporter.
- Added strict multi-platform CI matrix testing with ruff, mypy, and coverage verification.
- Added `SECURITY.md` and Dependabot configuration.

## [0.2.1] - 2026-08-23

### Added
- Added `ResilientOpenAI` adapter supporting dual-axis RPM and TPM throttling.
- Added Prometheus text exposition format exporter (`export_prometheus`).
- Added CLI benchmark command `flowguard benchmark`.

## [0.2.0] - 2026-08-23

### Added
- Integrated `@guard` all-in-one pipeline decorator.
- Added `Bulkhead` concurrency governor.
- Added sliding window log rate limiter (`SlidingWindowLimiter`).
- Added in-memory latency percentiles telemetry (`MetricsCollector`).

## [0.1.0] - 2026-08-23

### Initial Release
- Core `TokenBucketLimiter` async algorithm.
- Core `CircuitBreaker` state machine (`CLOSED`, `OPEN`, `HALF_OPEN`).
- Basic `RetryPolicy` with exponential backoff.
- MIT Open Source license and standard packaging.
