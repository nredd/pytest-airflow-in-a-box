"""Test the certification-tier surfacing: config-time warning and canary probe."""

from __future__ import annotations

import functools
import types
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest

from pytest_airflow_in_a_box import certification
from pytest_airflow_in_a_box._compat import capabilities as capability_module
from pytest_airflow_in_a_box._compat import components as compat_components
from pytest_airflow_in_a_box._compat.capabilities import (
    CertificationTier,
    PluginsManagerShape,
    SharedModuleLoading,
    resolve_capabilities,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class _WarningRecorder:
    """Duck-typed stand-in for `pytest.Config` recording config-time warnings.

    Parameters:
        calls: list[Warning] receiving every warning issued through the seam.
    """

    def __init__(self, calls: list[Warning]) -> None:
        self._calls = calls

    def issue_config_time_warning(self, warning: Warning, stacklevel: int) -> None:
        """Record one issued warning.

        Parameters:
            warning: Warning issued by the code under test.
            stacklevel: int carrying pytest's stacklevel contract; unused here.
        """

        del stacklevel
        self._calls.append(warning)


def _recorder() -> tuple[pytest.Config, list[Warning]]:
    """Build the recording config stand-in and its call list.

    Returns:
        tuple[pytest.Config, list[Warning]] containing the duck-typed config (cast for
        the annotated signature) and the list its warnings land in.
    """

    calls: list[Warning] = []
    return cast("pytest.Config", _WarningRecorder(calls)), calls


@pytest.mark.parametrize("tier", [CertificationTier.CERTIFIED, None])
def test_warn_if_airflow_uncertified_silent_off_the_probed_tier(
    monkeypatch: pytest.MonkeyPatch, tier: CertificationTier | None
) -> None:
    """Stay silent on a certified or unclassifiable installation.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the tier classification.
        tier: CertificationTier | None returned by the faked classification.
    """

    monkeypatch.setattr(certification, "installed_certification", lambda: tier)
    config, calls = _recorder()

    certification.warn_if_airflow_uncertified(config)

    assert calls == []


def test_warn_if_airflow_uncertified_warns_once_on_the_probed_tier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue exactly one actionable `UncertifiedAirflowWarning` on the `PROBED` tier.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the tier classification and version.
    """

    monkeypatch.setattr(certification, "installed_certification", lambda: CertificationTier.PROBED)
    monkeypatch.setattr(certification.metadata, "version", lambda _name: "3.4.0")
    config, calls = _recorder()

    certification.warn_if_airflow_uncertified(config)

    assert len(calls) == 1
    assert isinstance(calls[0], certification.UncertifiedAirflowWarning)
    message = str(calls[0])
    assert "'3.4.0'" in message
    assert certification.LAST_CERTIFIED_VERSION in message
    assert "--airflow-doctor" in message


def test_installed_version_display_degrades_on_unreadable_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back to a placeholder instead of raising when the metadata read fails.

    Parameters:
        monkeypatch: pytest.MonkeyPatch making the metadata read raise.
    """

    def raising_version(distribution_name: str) -> str:
        """Raise the representative metadata failure for every distribution."""

        raise OSError(f"unreadable metadata for '{distribution_name}'")

    monkeypatch.setattr(certification.metadata, "version", raising_version)

    assert certification._installed_version_display() == "<unknown>"


def test_warning_surfaces_in_a_real_run_on_the_probed_tier(
    pytester: pytest.Pytester,
) -> None:
    """Surface the degraded-tier warning in pytest's own warnings summary.

    The conftest rebinds `certification.installed_certification` at import time, which
    happens before the plugin's `pytest_configure` fires, faking the `PROBED`
    classification for the whole subprocess session.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(
        "from pytest_airflow_in_a_box import certification\n"
        "from pytest_airflow_in_a_box._compat.capabilities import CertificationTier\n"
        "\n"
        "certification.installed_certification = lambda: CertificationTier.PROBED\n"
    )
    pytester.makepyfile(
        "def test_passes() -> None:\n"
        '    """Pass, so the only signal in the run is the config-time warning."""\n'
    )

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1, warnings=1)
    result.stdout.fnmatch_lines(["*UncertifiedAirflowWarning*"])


def test_certification_probe_report_certified_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report exit code 0 with the full capability listing on the real certified install.

    Parameters:
        monkeypatch: pytest.MonkeyPatch left unused; declared for signature symmetry
            with the sibling probe tests so the suite reads uniformly.
    """

    del monkeypatch

    text, code = certification.certification_probe_report()

    assert code == 0
    assert "CERTIFIED: release matches its certified contract." in text
    assert "certification=certified" in text
    assert "task_instance_runner=" in text
    assert "Cache-name drift against the certified tables: none" in text


def test_certification_probe_report_flags_an_uncertified_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report exit code 1 and the `UNCERTIFIED` marker on the `PROBED` tier.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the resolved capabilities.
    """

    fake_capabilities = replace(resolve_capabilities(), certification=CertificationTier.PROBED)
    monkeypatch.setattr(capability_module, "resolve_capabilities", lambda: fake_capabilities)

    text, code = certification.certification_probe_report()

    assert code == 1
    assert "UNCERTIFIED: release" in text
    assert certification.LAST_CERTIFIED_VERSION in text


def _fake_cached_module(names: tuple[str, ...], module_name: str) -> object:
    """Build a fake module whose attributes expose a callable `cache_clear`.

    Parameters:
        names: tuple[str, ...] naming the cache-clearable attributes to define.
        module_name: str naming the fake module, for diagnostics.

    Returns:
        object shaped like a module for `_cache_clearable_names` introspection.
    """

    module = types.ModuleType(module_name)
    for name in names:

        def _make(n: str) -> Callable[[], str]:
            @functools.cache
            def _fn() -> str:
                return n

            return _fn

        setattr(module, name, _make(name))
    return module


def test_certification_probe_report_flags_cache_drift_on_a_certified_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report exit code 2 and the `DRIFT` marker when a certified release drifted.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the live plugins-manager modules.
    """

    drifted_core = _fake_cached_module(("surprise_cache",), "fake_drifted_core")
    drifted_sdk = _fake_cached_module((), "fake_drifted_sdk")
    monkeypatch.setattr(
        compat_components, "_plugins_manager_modules", lambda: (drifted_core, drifted_sdk)
    )

    text, code = certification.certification_probe_report()

    assert code == 2
    assert "DRIFT: a certified release's live cache names" in text
    assert "uncertified extra ['surprise_cache']" in text


def test_cache_drift_lines_flags_a_missing_sdk_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Render the missing-SDK-module line when `cached-functions` expects one.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the live plugins-manager modules.
    """

    fake_capabilities = replace(
        resolve_capabilities(), plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS
    )
    core = _fake_cached_module((), "fake_core")
    monkeypatch.setattr(capability_module, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (core, None))

    lines = certification._cache_drift_lines()

    assert any("missing entirely" in line for line in lines)


def test_cache_drift_lines_skips_an_absent_sdk_module_on_module_globals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Audit only the core and shared modules on the 3.1.x `module-globals` shape.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the capabilities and module seams.
    """

    fake_capabilities = replace(
        resolve_capabilities(),
        plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
        shared_module_loading=SharedModuleLoading.SINGLE,
    )
    core = _fake_cached_module((), "fake_31_core")
    shared = _fake_cached_module(("_get_grouped_entry_points",), "fake_shared")
    monkeypatch.setattr(capability_module, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (core, None))
    monkeypatch.setattr(
        compat_components, "_shared_module_loading_modules", lambda _shape: (shared,)
    )

    lines = certification._cache_drift_lines()

    # The MODULE_GLOBALS core row certifies plain globals, not cache functions, so an
    # empty cache-clearable observation on the fake core is a `missing` drift line;
    # what matters here is that no SDK line appears at all.
    assert not any("sdk" in line.lower() for line in lines)


def test_cache_drift_lines_reports_nothing_on_the_2x_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report no drift on a 2.x-shaped contract, which has no cache tables at all.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking a 2.x-shaped capabilities contract.
    """

    fake_capabilities = replace(
        resolve_capabilities(), plugins_manager=None, shared_module_loading=None
    )
    monkeypatch.setattr(capability_module, "resolve_capabilities", lambda: fake_capabilities)

    assert certification._cache_drift_lines() == []
