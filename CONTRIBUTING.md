# Contributing to FlowGuard

Thank you for your interest in contributing to FlowGuard! We welcome contributions from everyone.

## Development Setup

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/your-username/flowguard.git
   cd flowguard
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -e ".[all,dev]"
   ```

4. **Run tests:**
   ```bash
   python -m pytest
   ```

## Code Quality Standards

- **Formatting & Linting**: We use [Ruff](https://github.com/astral-sh/ruff) for linting and formatting (`ruff check . && ruff format .`).
- **Type Checking**: All public APIs must have full type annotations verified by [Mypy](https://mypy-lang.org/) (`mypy src/flowguard`).
- **Testing**: Every new feature or bugfix should include corresponding unit tests in `tests/`.

## Submitting Pull Requests

1. Create a descriptive branch (`git checkout -b feature/redis-backend` or `git checkout -b fix/circuit-breaker-race`).
2. Commit your changes with clear commit messages.
3. Ensure all tests and lint checks pass.
4. Open a Pull Request on GitHub against the `main` branch.
