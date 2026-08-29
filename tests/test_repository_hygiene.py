"""Automated repository hygiene test: enforce zero disallowed control characters."""

import os

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CHECKED_EXTENSIONS = (".py", ".md", ".yml", ".yaml", ".toml", ".txt", ".json")
IGNORED_DIRS = {".git", ".pytest_cache", "__pycache__", ".mypy_cache", ".venv", "venv", ".egg-info"}


def test_no_disallowed_control_characters_in_repository():
    """Scan all project source and documentation files for unexpected ASCII control characters."""
    violations = {}

    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in IGNORED_DIRS and not d.endswith(".egg-info")]

        for file in files:
            if file.endswith(CHECKED_EXTENSIONS):
                file_path = os.path.join(root, file)
                with open(file_path, "rb") as f:
                    content = f.read()

                bad_bytes = []
                for idx, byte in enumerate(content):
                    # Disallow bytes < 32 except tab (9), newline (10), carriage return (13)
                    if byte < 32 and byte not in (9, 10, 13):
                        bad_bytes.append((idx, hex(byte)))

                if bad_bytes:
                    rel_path = os.path.relpath(file_path, REPO_ROOT)
                    violations[rel_path] = bad_bytes

    assert not violations, f"Disallowed control characters found in files: {violations}"
