"""Surface the certification tier: warn on degraded sessions, probe for the canary.

The probe-and-degrade contract (issue #212) lets an uncertified Airflow 3.x release
resolve by pure probing instead of hard-failing. This module is the surfacing half:
`warn_if_airflow_uncertified` fires the once-per-session configure-time warning, and
`certification_probe_report` builds the report the weekly `airflow-canary.yml` workflow
runs against the newest upstream release so certification work is filed before users
ever hit the degraded tier.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/212
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.Config.issue_config_time_warning
"""

from __future__ import annotations

from dataclasses import fields
from enum import Enum
from importlib import metadata
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import (
    AIRFLOW_DISTRIBUTION,
    SUPPORTED_RELEASES,
    CertificationTier,
    CertifiedCaches,
    installed_certification,
)

if TYPE_CHECKING:
    import pytest

    from pytest_airflow_in_a_box._compat.capabilities import AirflowCapabilities

# `max()`, not `[-1]`: the certified tuple is hand-maintained and a backport row
# appended out of order must not silently change the release this module tells users
# to pin to or the canary names as last-certified.
LAST_CERTIFIED_VERSION = ".".join(str(part) for part in max(SUPPORTED_RELEASES))


class UncertifiedAirflowWarning(RuntimeWarning):
    """Warn that the installed Airflow release resolves on the degraded `PROBED` tier."""


def _installed_version_display() -> str:
    """Read the installed 3.x core version for messages, degrading to a placeholder.

    Returns:
        str containing the metadata version, or a placeholder when the read fails --
        the caller already classified the tier, so a racing environment mutation must
        not turn a warning into a crash.
    """

    try:
        return metadata.version(AIRFLOW_DISTRIBUTION)
    except Exception:
        return "<unknown>"


def warn_if_airflow_uncertified(config: pytest.Config) -> None:
    """Warn once, at configure time, that the session runs on the degraded tier.

    Classification is metadata-only (`installed_certification`), so a certified or
    Airflow-free session pays no Airflow import cost here. A bare ``warnings.warn``
    from a plugin hook is not captured by pytest's warnings machinery;
    ``issue_config_time_warning`` is the documented seam for a configure-time warning
    that pytest's summary picks up, mirroring
    `migration_strict.warn_if_migration_strict_is_a_noop`.

    Parameters:
        config: pytest.Config for the active test session.
    """

    if installed_certification() is not CertificationTier.PROBED:
        return
    config.issue_config_time_warning(
        UncertifiedAirflowWarning(
            f"Apache Airflow '{_installed_version_display()}' has no certified "
            f"contract row in this version of `pytest-airflow-in-a-box` (last "
            f"certified release: '{LAST_CERTIFIED_VERSION}'): capabilities were "
            f"resolved by live probing and the component sandbox degrades to generic "
            f"snapshot/restore. State isolation still holds byte-for-byte; "
            f"byte-verified vetting of Airflow internals does not. Upgrade "
            f"`pytest-airflow-in-a-box` once it certifies this release, or install a "
            f"certified `apache-airflow-core` release. Run `pytest --airflow-doctor` "
            f"for details."
        ),
        2,
    )


def _capability_report_lines(capabilities: AirflowCapabilities) -> list[str]:
    """Render every resolved capability field as one stable `name=value` line.

    Parameters:
        capabilities: AirflowCapabilities containing the resolved contract.

    Returns:
        list[str] containing one `name=value` line per field, in declaration order.
    """

    lines = []
    for field in fields(capabilities):
        value = getattr(capabilities, field.name)
        rendered = value.value if isinstance(value, Enum) else value
        lines.append(f"{field.name}={rendered}")
    return lines


def _cache_function_drift(module: Any, certified: CertifiedCaches) -> str | None:
    """Diff one module's live cache-clearable names against its certified row.

    Parameters:
        module: Any containing the module to introspect.
        certified: CertifiedCaches containing the certified cache-clearable names.

    Returns:
        str | None containing a `module: missing [...], extra [...]` line, or None
        when the module matches its certified row.
    """

    # Deferred on genuine import cost: `certification.py` must stay importable
    # pre-bootstrap and this helper only runs from the Airflow-importing probe.
    from pytest_airflow_in_a_box._compat.components import _cache_clearable_names

    observed = _cache_clearable_names(module)
    missing = sorted(certified.required - observed)
    extra = sorted(observed - (certified.required | certified.optional))
    if missing or extra:
        return f"{module.__name__}: missing {missing}, uncertified extra {extra}"
    return None


def _cache_drift_lines() -> list[str]:
    """Diff live plugins-manager state against the certified rows, one line per module.

    Audits by shape, mirroring `clear_plugins_manager_caches`: the `CACHED_FUNCTIONS`
    core/SDK rows and the shared-module-loading rows diff `functools.cache` names by
    introspection, while the 3.1.x `MODULE_GLOBALS` core row certifies plain globals
    and is diffed by attribute presence instead -- a cache-function audit there would
    report all nineteen certified globals as missing on every healthy 3.1.x install.

    Returns:
        list[str] containing one drift line per drifted module, empty when every
        module matches its certified row.
    """

    # Deferred on genuine import cost: this walks live `airflow.plugins_manager`
    # modules, and `certification.py` itself must stay importable pre-bootstrap.
    from pytest_airflow_in_a_box._compat.capabilities import (
        CERTIFIED_CORE_PLUGINS_MANAGER_CACHES,
        CERTIFIED_SDK_PLUGINS_MANAGER_CACHES,
        CERTIFIED_SHARED_MODULE_LOADING_CACHES,
        PluginsManagerShape,
        resolve_capabilities,
    )
    from pytest_airflow_in_a_box._compat.components import (
        _plugins_manager_modules,
        _shared_module_loading_modules,
    )

    capabilities = resolve_capabilities()
    shape = capabilities.plugins_manager
    shared_shape = capabilities.shared_module_loading
    if shape is None or shared_shape is None:
        return []

    core_module, sdk_module = _plugins_manager_modules()
    core_certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[shape]
    lines = []
    if shape is PluginsManagerShape.CACHED_FUNCTIONS:
        core_line = _cache_function_drift(core_module, core_certified)
        if core_line is not None:
            lines.append(core_line)
        if sdk_module is None:
            lines.append(
                "airflow.sdk.plugins_manager: missing entirely where the "
                "`cached-functions` shape expects it"
            )
        else:
            sdk_line = _cache_function_drift(
                sdk_module, CERTIFIED_SDK_PLUGINS_MANAGER_CACHES[shape]
            )
            if sdk_line is not None:
                lines.append(sdk_line)
    else:
        missing = sorted(
            name for name in core_certified.required if not hasattr(core_module, name)
        )
        if missing:
            lines.append(f"{core_module.__name__}: missing module globals {missing}")
        if sdk_module is not None:
            lines.append(
                f"{sdk_module.__name__}: exists where the `module-globals` shape "
                f"expects no SDK plugins manager"
            )

    shared_certified = CERTIFIED_SHARED_MODULE_LOADING_CACHES[shared_shape]
    for module in _shared_module_loading_modules(shared_shape):
        shared_line = _cache_function_drift(module, shared_certified)
        if shared_line is not None:
            lines.append(shared_line)
    return lines


def certification_probe_report() -> tuple[str, int]:
    """Probe the installed release and report certification drift for the canary.

    Resolves the full capability contract (exercising every probe, including
    `airflow.sdk.definitions.dag._run_task` resolution through the task-runner probe)
    and diffs the live plugins-manager cache names against the certified tables. The
    canary runs this against the newest upstream release: users on that release merely
    degrade, but the canary fails loudly so re-certification work is filed first.

    Returns:
        tuple[str, int] containing the report text and the exit code -- 0 when the
        release is certified with no cache drift, 1 when the release is uncertified
        (resolved on the `PROBED` tier), 2 when a certified release shows cache-name
        drift.
    """

    # Deferred on genuine import cost: resolving capabilities imports Airflow.
    from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

    capabilities = resolve_capabilities()
    lines = ["Resolved capabilities:"]
    lines.extend(f"  {line}" for line in _capability_report_lines(capabilities))
    drift = _cache_drift_lines()
    if drift:
        lines.append("Cache-name drift against the certified tables:")
        lines.extend(f"  {line}" for line in drift)
    else:
        lines.append("Cache-name drift against the certified tables: none")

    if capabilities.certification is CertificationTier.PROBED:
        lines.append(
            f"UNCERTIFIED: release {'.'.join(str(part) for part in capabilities.release)} "
            f"resolved on the probed tier -- add a certification row "
            f"(last certified: {LAST_CERTIFIED_VERSION})."
        )
        return "\n".join(lines), 1
    if drift:
        lines.append(
            "DRIFT: a certified release's live cache names no longer match its "
            "certified row -- re-verify the tables."
        )
        return "\n".join(lines), 2
    lines.append("CERTIFIED: release matches its certified contract.")
    return "\n".join(lines), 0


__all__ = (
    "UncertifiedAirflowWarning",
    "certification_probe_report",
    "warn_if_airflow_uncertified",
)
