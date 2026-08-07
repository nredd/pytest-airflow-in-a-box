"""Reject static-analysis suppression directives in Python source.

References:
    https://docs.astral.sh/ruff/linter/#error-suppression
    https://docs.astral.sh/ty/reference/configuration/
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DIRECTIVE_NAMES = (
    "my" + "py: ignore",
    "no" + "qa",
    "pylint: disable",
    "pyright: ignore",
    "ty" + ": ignore",
    "type" + ": ignore",
)
WAIVER_PATTERN = re.compile(
    r"#\s*(?:" + "|".join(re.escape(name) for name in DIRECTIVE_NAMES) + r")"
)


def main(paths: list[str]) -> int:
    """Check source files for forbidden inline waivers.

    Parameters:
        paths: list[str] containing paths to inspect.

    Returns:
        int process exit status, zero when all files are clean.
    """
    violations: list[str] = []
    for raw_path in paths:
        source_path = Path(raw_path)
        if not source_path.is_file():
            continue
        for line_number, line in enumerate(
            source_path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if WAIVER_PATTERN.search(line):
                violations.append(f"{source_path}:{line_number}: inline waiver is forbidden")

    if violations:
        sys.stdout.write("\n".join(violations) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
