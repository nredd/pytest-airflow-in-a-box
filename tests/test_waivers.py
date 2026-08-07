"""Test the inline-waiver policy gate."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "check_inline_waivers.py"


def test_waiver_check_accepts_clean_source(tmp_path: Path) -> None:
    """Accept source without suppression directives."""
    source_path = tmp_path / "clean.py"
    source_path.write_text("value: int = 1\n", encoding="utf-8")

    subprocess.check_output([sys.executable, str(SCRIPT_PATH), str(source_path)], text=True)


def test_waiver_check_rejects_suppression(tmp_path: Path) -> None:
    """Reject a source-level suppression directive."""
    source_path = tmp_path / "waived.py"
    directive = "# " + "no" + "qa"
    source_path.write_text(f"value = 1  {directive}\n", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(source_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert str(source_path) in result.stdout
