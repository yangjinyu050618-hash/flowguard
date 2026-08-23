import pytest
from flowguard.cli import run_benchmark, main


@pytest.mark.asyncio
async def test_run_benchmark():
    # Run a small synthetic benchmark
    await run_benchmark(concurrency=2, total_requests=4, rate_per_sec=100.0)


def test_cli_main(capsys):
    with pytest.raises(SystemExit) as exc:
        import sys
        sys.argv = ["flowguard", "--version"]
        main()
    assert exc.value.code == 0
