"""Render a one-shot diagnostics report for `--airflow-doctor`.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_cmdline_main
    https://github.com/apache/airflow/blob/2.11.2/airflow/executors/executor_loader.py
    https://github.com/apache/airflow/blob/2.11.2/airflow/config_templates/unit_tests.cfg
    https://airflow.apache.org/docs/apache-airflow/2.10.0/core-concepts/executor/index.html#using-multiple-executors-concurrently
    https://pytest-cov.readthedocs.io/en/latest/config.html
"""

from __future__ import annotations

import os
import platform
import urllib.parse
from collections.abc import Mapping
from dataclasses import fields
from enum import Enum
from pathlib import Path

import pytest

from pytest_airflow_in_a_box import __version__
from pytest_airflow_in_a_box._compat import (
    AirflowCapabilities,
    AirflowCompatibilityError,
    resolve_capabilities,
)
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, CertificationTier
from pytest_airflow_in_a_box._compat.settings import airflow_conf
from pytest_airflow_in_a_box.bootstrap import BootstrapState, get_bootstrap_state
from pytest_airflow_in_a_box.certification import LAST_CERTIFIED_VERSION
from pytest_airflow_in_a_box.collection import collection_folder
from pytest_airflow_in_a_box.fixtures.dagbag import _dag_folder
from pytest_airflow_in_a_box.ini_config import INI_OPTION_NAME, INI_OVERRIDES_KEY
from pytest_airflow_in_a_box.migration_strict import migration_strict_enabled
from pytest_airflow_in_a_box.reporting import PYTEST_COV_PLUGIN_NAME
from pytest_airflow_in_a_box.storage.provision import DbBackend

REPORT_TITLE = "# pytest-airflow-in-a-box diagnostics"
# Substrings marking a declared `airflow_config` value as a credential rather than a
# setting. Airflow's own `sensitive_config_values` is the exact answer, but reading it
# means importing the configuration parser, and this module is on `plugin.py`'s
# import path. Over-redacting a `*_url` is the safe direction to be wrong in.
_SENSITIVE_KEY_MARKERS = ("password", "secret", "key", "token", "conn", "url")
# Airflow 2.x gates SQLite compatibility on `Executor.is_single_threaded`, not a name
# list; `SequentialExecutor` and `DebugExecutor` are the two core executors setting it
# True (`ExecutorLoader.validate_database_executor_compatibility`).
_SINGLE_THREADED_EXECUTORS = frozenset({"SequentialExecutor", "DebugExecutor"})
# Airflow's own escape hatch for this exact check; set by a consumer who has already
# decided SQLite + their executor is fine for their test.
_SKIP_CHECK_ENVIRONMENT_VARIABLE = "_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"
# pytest-cov's `--cov` option destination; the option existing at all (its parsed default
# is an empty list) is how an installed-but-inactive pytest-cov is told apart from an
# absent one, without ever importing `pytest_cov`.
_COV_SOURCE_OPTION = "cov_source"
# pytest-cov's `--no-cov` kill switch; it leaves the `_cov` plugin registered but
# disables every controller, so an active-looking run may still measure nothing.
_NO_COV_OPTION = "no_cov"


def _storage_section(state: BootstrapState) -> list[str]:
    """Render the storage ladder decision and its reason.

    Parameters:
        state: BootstrapState containing the resolved run storage.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    return [
        f"- Reason: `{state.storage_reason}`",
        f"- Network filesystem: `{state.network_storage}`",
    ]


def _database_section(state: BootstrapState) -> list[str]:
    """Render the resolved `AIRFLOW_HOME`, database backend, and URL scheme.

    The full SQLAlchemy URL is deliberately omitted: a provisioned Postgres URL carries
    live credentials, and the scheme plus backend tier is all this report needs.

    Parameters:
        state: BootstrapState containing the resolved run configuration.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    scheme = urllib.parse.urlsplit(state.sql_alchemy_conn).scheme
    return [
        f"- `AIRFLOW_HOME`: `{state.root}`",
        f"- Backend tier: `{state.db_backend}`",
        f"- Database URL scheme: `{scheme}`",
    ]


def _resolve_executor() -> str:
    """Resolve Airflow's configured `core.executor` value after bootstrap.

    Returns:
        str containing the resolved executor name, import path, or multi-executor list.

    Raises:
        Exception: Airflow is not importable, or its configuration cannot be read. Left
            unnarrowed because the caller degrades on any failure here rather than
            distinguishing them.
    """

    return airflow_conf().get("core", "executor")


def _executor_section(state: BootstrapState) -> list[str]:
    """Render the resolved `core.executor` and flag the 2.x SQLite executor conflict.

    Bootstrap env-pins `SequentialExecutor` on the 2.x family (see
    `bootstrap._environment()`), so a multi-threaded executor resolving here means
    consumer configuration overrode the pin -- and Airflow 2.x's `ready_to_reschedule`
    sensor dependency rejects that combination with SQLite for poke- and
    reschedule-mode sensors alike, with no indication of the actual cause. Degrades to
    a resolution-failure bullet, never raises, matching every other section in this
    module.

    Parameters:
        state: BootstrapState containing the resolved database backend and Airflow family.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    try:
        # Broad on purpose: an unresolvable executor (bad import, a malformed override) is
        # exactly as reportable as a resolved one, and this section must never abort the
        # rest of the report over it.
        executor = _resolve_executor()
    except Exception as error:
        return [f"- `core.executor`: could not resolve: {error}"]

    lines = [f"- `core.executor`: `{executor}`"]
    # `core.executor` accepts a comma-separated multi-executor list since Airflow 2.10
    # (AIP-61); the first entry is the default `ready_to_reschedule` actually loads, and
    # may be a bare name, an `alias:module.Class` or `alias:CoreName` pair, or a bare
    # import path -- strip any alias before reducing to the class name.
    primary = executor.split(",")[0].strip()
    unaliased = primary.rpartition(":")[2]
    executor_name = unaliased.rpartition(".")[2] or unaliased
    if not executor_name:
        return lines

    is_sqlite = state.db_backend == DbBackend.SQLITE
    is_v2 = state.family == AirflowFamily.V2.value
    skip_check = os.environ.get(_SKIP_CHECK_ENVIRONMENT_VARIABLE) == "1"
    if is_v2 and is_sqlite and not skip_check and executor_name not in _SINGLE_THREADED_EXECUTORS:
        lines.append(
            f"- INCOMPATIBLE: `{executor_name}` overrides this plugin's 2.x default of "
            f"`SequentialExecutor`, and Airflow 2.x's `ready_to_reschedule` sensor "
            f"dependency rejects it with SQLite for poke- and reschedule-mode sensors "
            f"alike. Pin `core.executor` back to `SequentialExecutor` or `DebugExecutor` "
            f"with `airflow_config`, switch to `--airflow-db-backend=postgres`, or set "
            f"`{_SKIP_CHECK_ENVIRONMENT_VARIABLE}=1` (Airflow's own escape hatch, exact "
            f"string '1') if the executor is single-threaded and merely named "
            f"unconventionally."
        )
    return lines


def _format_capability_value(value: object) -> str:
    """Format one capability field value for display.

    Parameters:
        value: object containing one `AirflowCapabilities` field value.

    Returns:
        str containing an enum's plain value, a dot-joined release tuple, or the
        value's default string form.
    """

    if value is None:
        return "unprobed on this family"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, tuple):
        return ".".join(str(part) for part in value)
    return str(value)


def _capability_lines(capabilities: AirflowCapabilities) -> list[str]:
    """Render every `AirflowCapabilities` field as a Markdown bullet.

    Parameters:
        capabilities: AirflowCapabilities containing the resolved contract.

    Returns:
        list[str] containing Markdown bullet lines, one per field.
    """

    lines = []
    for field in fields(capabilities):
        value = _format_capability_value(getattr(capabilities, field.name))
        lines.append(f"- Capability `{field.name}`: `{value}`")
    return lines


def _version_section() -> list[str]:
    """Render plugin, pytest, Python, and Apache Airflow versions plus capabilities.

    A `PROBED` certification tier gets an explanatory `DEGRADED:` bullet spelling out
    what the tier means and how to leave it; the tier itself already renders as a
    capability line like every other field.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    lines = [
        f"- `pytest-airflow-in-a-box`: `{__version__}`",
        f"- `pytest`: `{pytest.__version__}`",
        f"- Python: `{platform.python_version()}`",
    ]
    try:
        capabilities = resolve_capabilities()
    except AirflowCompatibilityError as error:
        lines.append(f"- Apache Airflow: INCOMPATIBLE: {error}")
        return lines
    lines.append(f"- Apache Airflow: `{_format_capability_value(capabilities.release)}`")
    lines.extend(_capability_lines(capabilities))
    if capabilities.certification is CertificationTier.PROBED:
        lines.append(
            f"- DEGRADED: Apache Airflow "
            f"`{_format_capability_value(capabilities.release)}` is newer than the "
            f"last certified release (`{LAST_CERTIFIED_VERSION}`). Capabilities above "
            f"are live probe observations, not byte-verified certification, and the "
            f"component sandbox snapshots/restores unknown caches generically. "
            f"Upgrade `pytest-airflow-in-a-box` once it certifies this release, or "
            f"pin `apache-airflow-core<={LAST_CERTIFIED_VERSION}`."
        )
    return lines


def _migration_strict_section(config: pytest.Config, state: BootstrapState) -> list[str]:
    """Render whether migration-strict mode is enabled, and flag an off-2.x no-op.

    Parameters:
        config: pytest.Config containing plugin options and ini values.
        state: BootstrapState containing the resolved Airflow family.

    Returns:
        list[str] containing one Markdown bullet line.
    """

    if not migration_strict_enabled(config):
        return ["- `--airflow-migration-strict`: disabled"]
    if state.family != AirflowFamily.V2.value:
        return [
            "- `--airflow-migration-strict`: enabled, but a no-op off the Airflow 2.x "
            "family -- nothing to forecast"
        ]
    return ["- `--airflow-migration-strict`: enabled"]


def _cov_sources(config: pytest.Config) -> list[object] | None:
    """Read `pytest-cov`'s parsed `--cov` sources without importing `pytest_cov`.

    Parameters:
        config: pytest.Config containing plugin options.

    Returns:
        list[object] | None containing the parsed `--cov` values (path or package-name
        strings, or ``True`` for the bare everything-recording form), an empty list when
        `pytest-cov` is installed but no `--cov` was given, or ``None`` when `pytest-cov`
        is not installed and the option does not exist.

    Raises:
        pytest.UsageError: The `cov_source` option exists but did not parse to a list.
    """

    value: object = config.getoption(_COV_SOURCE_OPTION, default=None)
    if value is None:
        return None
    if not isinstance(value, list):
        raise pytest.UsageError("Option `--cov` must parse to a list of sources")
    return value


def _covering_source(dag_folder: Path, sources: list[object], invocation_dir: Path) -> str | None:
    """Find the first `--cov` source that measures files below the Dag folder.

    A bare ``--cov`` parses as ``True`` and disables source filtering entirely, so it
    covers any folder. A string source may also be an importable package name rather
    than a path; a name that does not resolve to a directory containing the Dag folder
    simply never matches, which is correct -- a Dag folder is a directory `pytest-cov`
    would receive as a path, not resolve by import.

    Parameters:
        dag_folder: pathlib.Path containing the resolved absolute Dag folder.
        sources: list[object] containing `pytest-cov`'s parsed `--cov` values.
        invocation_dir: pathlib.Path resolving relative path sources, matching the
            working directory `pytest-cov` hands them to `coverage.py` from.

    Returns:
        str | None containing the matching source's command-line form, or ``None`` when
        no source measures the Dag folder.
    """

    for source in sources:
        if not isinstance(source, str):
            return "--cov"
        if not source:
            # `--cov=` parses to one empty-string source; `coverage.py` receives it as
            # a module name, imports nothing, and records nothing -- it must never
            # count as covering the Dag folder.
            continue
        candidate = Path(source)
        if not candidate.is_absolute():
            candidate = invocation_dir / candidate
        if dag_folder.is_relative_to(Path(str(candidate)).resolve()):
            return f"--cov={source}"
    return None


def _coverage_section(config: pytest.Config, state: BootstrapState) -> list[str]:
    """Render Dag coverage diagnostics: resolved folders and `--cov` containment.

    `pytest-cov` registers its controller under the plugin name `_cov` only once a
    `--cov` option activates it, so `hasplugin` distinguishes an active coverage run
    while the `cov_source` option existing at all distinguishes an installed
    `pytest-cov`. `--no-cov` leaves `_cov` registered but disables all measurement, so
    it is checked separately. Neither check imports `pytest_cov` -- this module sits on
    `plugin.py`'s import path and must stay import-light. A misconfigured collection
    folder degrades to a bullet, matching every other section in this module.

    Parameters:
        config: pytest.Config containing plugin options, ini values, and rootpath.
        state: BootstrapState containing the scratch fallback Dag folder.

    Returns:
        list[str] containing Markdown bullet lines.

    Raises:
        pytest.UsageError: A Dag folder or `--cov` option has an invalid type or value.
    """

    dag_folder = Path(str(_dag_folder(config))).resolve()
    fallback = dag_folder == Path(str(state.dags_folder)).resolve()
    if fallback:
        lines = [
            f"- Dag folder: `{dag_folder}` (bootstrap scratch fallback -- neither "
            f"`--dag-folder` nor `airflow_dags_folder` is configured)"
        ]
    else:
        lines = [f"- Dag folder: `{dag_folder}`"]
    try:
        collect_folder = collection_folder(config)
    except pytest.UsageError as error:
        lines.append(f"- Collection folder: MISCONFIGURED: {error}")
    else:
        if collect_folder is None:
            lines.append(
                "- Collection folder: not configured (`--collect-dag-folder` / "
                "`airflow_collect_dags_folder`)"
            )
        else:
            lines.append(f"- Collection folder: `{collect_folder}`")

    sources = _cov_sources(config)
    if sources is None:
        lines.append(
            "- `pytest-cov`: not installed -- Dag files are not measured. Install it and "
            "pass `--cov=<dag folder>` (see the Dag coverage guide)"
        )
        return lines
    if not config.pluginmanager.hasplugin(PYTEST_COV_PLUGIN_NAME):
        lines.append(
            "- `pytest-cov`: installed but inactive -- no `--cov` was given, so Dag files "
            "are not measured. Pass `--cov=<dag folder> --cov-report=term-missing`"
        )
        return lines
    if config.getoption(_NO_COV_OPTION, default=False):
        lines.append(
            "- `pytest-cov`: disabled by `--no-cov` -- Dag files are not measured despite "
            "the configured `--cov` sources. Drop `--no-cov`"
        )
        return lines
    if fallback:
        lines.append(
            "- NOT MEASURABLE: the fallback Dag folder is a disposable bootstrap scratch "
            "directory; never feed it to `--cov`. Configure `--dag-folder` or "
            "`airflow_dags_folder` first, then add that folder as a `--cov` source"
        )
        return lines
    covering = _covering_source(dag_folder, sources, Path(str(config.invocation_params.dir)))
    if covering is None:
        lines.append(
            f"- NOT COVERED: the Dag folder sits outside every configured `--cov` source, "
            f"so Dag files are silently missing from the report. Add it: "
            f"`pytest --cov={dag_folder} --cov-report=term-missing`"
        )
    else:
        lines.append(f"- Dag folder covered by `{covering}`")
    return lines


def _rendered_override(key: str, value: str) -> str:
    """Render one declared override value, redacting anything that reads as a credential.

    Parameters:
        key: str naming the configuration option within its section.
        value: str containing the declared value.

    Returns:
        str containing the value, or a redaction placeholder naming its length.
    """

    if any(marker in key.lower() for marker in _SENSITIVE_KEY_MARKERS):
        return f"<redacted, {len(value)} characters>"
    return value


def _overrides_section(overrides: Mapping[tuple[str, str], str]) -> list[str]:
    """Render the Airflow configuration overrides declared through the ini option.

    Takes the already-parsed mapping rather than re-reading the ini, because a malformed
    value aborted the session during the initial parse and can never reach this report.

    Values whose key reads as a credential are replaced by their length. This report exists to
    be pasted into a bug report, and `_database_section` already declines to print a
    provisioned database URL for exactly that reason; a declared `smtp.smtp_password` or
    `celery.broker_url` deserves the same treatment. The name is always shown, because
    "which options are set" is the diagnostic question.

    Parameters:
        overrides: Mapping[tuple[str, str], str] mapping ``(section, key)`` pairs to the
            values declared by the `airflow_config` ini option.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    if not overrides:
        return [f"- No `{INI_OPTION_NAME}` overrides are declared"]
    return [
        f"- `{section}.{key}` = `{_rendered_override(key, value)}`"
        for (section, key), value in sorted(overrides.items())
    ]


def _api_server_section() -> list[str]:
    """Render the API server section, which never has live state for this invocation.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    return [
        "- Not started: this diagnostic run did not request the `api_server_url` fixture. "
        "The API server is a lazy, per-process, session-scoped subprocess with no state "
        "before a test requests it.",
    ]


def render_doctor_report(config: pytest.Config) -> str:
    """Render a one-shot, copy-pasteable diagnostics report.

    Parameters:
        config: pytest.Config for the active invocation.

    Returns:
        str containing the complete Markdown report.

    Raises:
        pytest.UsageError: Airflow bootstrap state is unavailable.
    """

    state = get_bootstrap_state(config)
    sections = (
        ("Storage", _storage_section(state)),
        ("AIRFLOW_HOME and database", _database_section(state)),
        ("Airflow config overrides", _overrides_section(config.stash[INI_OVERRIDES_KEY])),
        ("Versions and capabilities", _version_section()),
        ("Executor", _executor_section(state)),
        ("Migration-strict", _migration_strict_section(config, state)),
        ("Dag coverage", _coverage_section(config, state)),
        ("API server", _api_server_section()),
    )
    lines = [REPORT_TITLE, ""]
    for title, bullets in sections:
        lines.append(f"## {title}")
        lines.extend(bullets)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


__all__ = ("render_doctor_report",)
