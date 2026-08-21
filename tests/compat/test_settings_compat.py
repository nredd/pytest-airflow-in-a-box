"""Test the deferred configuration and settings seams against the installed release."""

from __future__ import annotations

import importlib
import subprocess
import sys

import pytest

from pytest_airflow_in_a_box._compat import settings as compat_settings


def test_settings_module_import_does_not_import_airflow() -> None:
    """Keep the settings seam import-safe before Airflow bootstrap."""

    script = (
        "import sys; import pytest_airflow_in_a_box._compat.settings; "
        "raise SystemExit('airflow' in sys.modules)"
    )

    subprocess.check_output([sys.executable, "-c", script], text=True)


def test_airflow_conf_resolves_the_live_parser() -> None:
    """Resolve the installed release's global configuration parser."""

    configuration = importlib.import_module("airflow.configuration")

    assert compat_settings.airflow_conf() is configuration.conf


def test_configure_vars_delegates_to_airflow_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delegate settings recomputation to Airflow's `configure_vars` at call time."""

    settings_module = importlib.import_module("airflow.settings")
    calls: list[bool] = []
    monkeypatch.setattr(settings_module, "configure_vars", lambda: calls.append(True))

    assert compat_settings.configure_vars() is None
    assert calls == [True]
