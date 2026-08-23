"""FlowGuard CLI utility for benchmarking and diagnostics."""

import argparse
import asyncio
import sys
import time
from flowguard import __version__, TokenBucketLimiter, FlowGuard, guard


async def run_benchmark(concurrency: int = 10, total_requests: int = 50, rate_per_sec: float = 20.0) -> None:
    print(f"\n--- FlowGuard Benchmark (Rate: {rate_per_sec} req/s, Concurrency: {concurrency}, Total: {total_requests}) ---")
    limiter = TokenBucketLimiter(rate=rate_per_sec, capacity=rate_per_sec)
    pipeline = FlowGuard(name="benchmark", limiter=limiter)

    start = time.monotonic()
    success = 0

    @pipeline
    async def sample_task(task_id: int) -> int:
        await asyncio.sleep(0.01)
        return task_id

    sem = asyncio.Semaphore(concurrency)

    async def worker(i: int) -> None:
        nonlocal success
        async with sem:
            await sample_task(i)
            success += 1

    tasks = [worker(i) for i in range(total_requests)]
    await asyncio.gather(*tasks)

    elapsed = time.monotonic() - start
    print(f"Completed {success}/{total_requests} requests in {elapsed:.3f}s ({success / elapsed:.1f} req/s)")
    summary = pipeline.metrics.get_summary()
    print(f"Metrics: p50={summary['latency_p50_ms']}ms, p95={summary['latency_p95_ms']}ms, p99={summary['latency_p99_ms']}ms\n")


def main() -> None:
    parser = argparse.ArgumentParser(prog="flowguard", description="FlowGuard CLI")
    parser.add_argument("--version", action="version", version=f"flowguard {__version__}")

    subparsers = parser.add_subparsers(dest="command")

    bench_parser = subparsers.add_parser("benchmark", help="Run token-bucket benchmark simulation")
    bench_parser.add_argument("--rate", type=float, default=20.0, help="Rate per second")
    bench_parser.add_argument("--total", type=int, default=50, help="Total requests")
    bench_parser.add_argument("--concurrency", type=int, default=10, help="Concurrent workers")

    args = parser.parse_args()

    if args.command == "benchmark":
        asyncio.run(run_benchmark(concurrency=args.concurrency, total_requests=args.total, rate_per_sec=args.rate))
    else:
        print(f"FlowGuard v{__version__} - Resilient Async Orchestration Toolkit")
        print("Run 'flowguard benchmark' or 'flowguard --help' for options.")


if __name__ == "__main__":
    main()
