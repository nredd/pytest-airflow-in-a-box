"""Test the coverage subprocess-startup hook installer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "install_coverage_pth.py"


def _load_installer() -> Any:
    """Load the installer script as a module.

    Returns:
        Any containing the loaded installer module.
    """

    spec = importlib.util.spec_from_file_location("install_coverage_pth", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def installer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Load the installer with site-packages pointed at a temporary directory.

    Parameters:
        tmp_path: pathlib.Path containing the fake site-packages directory.
        monkeypatch: pytest.MonkeyPatch scoping the sysconfig override.

    Returns:
        Any containing the loaded installer module.
    """

    module = _load_installer()
    monkeypatch.setattr(module.sysconfig, "get_paths", lambda: {"purelib": str(tmp_path)})
    return module


def test_installs_single_line_hook(installer: Any, tmp_path: Path) -> None:
    """Write one importable hook line into site-packages."""

    installed = installer.install_coverage_pth()

    assert installed == tmp_path / installer.PTH_FILE_NAME
    assert installed.read_text(encoding="utf-8") == installer.PTH_CONTENTS
    assert installer.PTH_CONTENTS.startswith("import ")
    assert "\n" not in installer.PTH_CONTENTS.rstrip("\n")


def test_installation_is_idempotent_and_repairs_drift(installer: Any) -> None:
    """Leave a current hook alone and rewrite a stale one."""

    first = installer.install_coverage_pth()
    original_mtime = first.stat().st_mtime_ns
    second = installer.install_coverage_pth()
    assert second == first
    assert first.stat().st_mtime_ns == original_mtime

    first.write_text("import nothing_relevant\n", encoding="utf-8")
    repaired = installer.install_coverage_pth()
    assert repaired.read_text(encoding="utf-8") == installer.PTH_CONTENTS
