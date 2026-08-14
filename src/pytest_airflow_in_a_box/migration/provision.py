"""Provision disposable `uv` virtual environments for the migration diff orchestrator.

Mirrors `compat.yml`'s two-pass Airflow 2.x install and single-pass 3.x install so the
environments this package tests against match the ones CI already certifies. Every
`uv`/network touch is deferred behind an injected seam (the `storage/postgres.py`
`DockerRunner` precedent) so this module reaches full branch coverage without a real
`uv` binary or network access.

References:
    https://github.com/apache/airflow/blob/main/.github/workflows/README.md
    https://docs.astral.sh/uv/pip/environments/
    https://docs.astral.sh/uv/reference/cli/#uv-pip-install
"""

from __future__ import annotations

import logging
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.migration.types import PluginSpecResolutionError, ProvisioningError

LOGGER = logging.getLogger(__name__)

CONSTRAINTS_URL_TEMPLATE = (
    "https://github.com/apache/airflow/raw/constraints-{version}/constraints-{python}.txt"
)
# The plugin's own floor plus the 2.x two-pass ceiling certified by #41: 2.x constraints
# pin pytest as low as 7.4.4, below `pytest>=8`, so pass two installs pytest unconstrained
# with an explicit ceiling (no constraints file governs pass two at all).
V2_UNCONSTRAINED_PYTEST_SPEC = "pytest>=8,<9"
V3_SQLITE_PROVIDER_SPEC = "apache-airflow-providers-sqlite>=4.1,<5"
_CONSTRAINTS_FETCH_TIMEOUT_SECONDS = 30

ConstraintsFetcher = Callable[[str, str], str]
SubprocessRunner = Callable[..., "subprocess.CompletedProcess[str]"]


@dataclass(frozen=True)
class ProvisionedEnvironment:
    """One provisioned `uv` virtual environment ready to run pytest.

    Parameters:
        family: AirflowFamily naming the provisioned Airflow major.
        airflow_version: str containing the installed Airflow release.
        python_version: str containing the venv's `X.Y` Python version.
        venv_dir: pathlib.Path containing the virtual environment root.
        python_path: pathlib.Path containing the venv's `python` executable.
    """

    family: AirflowFamily
    airflow_version: str
    python_version: str
    venv_dir: Path
    python_path: Path


def fetch_constraints(airflow_version: str, python_version: str) -> str:
    """Download Apache Airflow's published constraints file for one release.

    Parameters:
        airflow_version: str containing the Airflow release the constraints pin.
        python_version: str containing the `X.Y` Python version the constraints target.

    Returns:
        str containing the constraints file body.

    Raises:
        ProvisioningError: The constraints file could not be downloaded.
    """

    url = CONSTRAINTS_URL_TEMPLATE.format(version=airflow_version, python=python_version)
    try:
        with urllib.request.urlopen(url, timeout=_CONSTRAINTS_FETCH_TIMEOUT_SECONDS) as response:
            return response.read().decode("utf-8")
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        raise ProvisioningError(
            f"Could not fetch Airflow constraints from '{url}': {error}"
        ) from error


def _run_step(
    args: list[str],
    *,
    runner: SubprocessRunner,
    step_name: str,
    plugin_spec_step: bool,
    plugin_spec_is_default: bool,
) -> None:
    """Run one provisioning subprocess step, raising a typed error on failure.

    Parameters:
        args: list[str] containing the complete command to run.
        runner: SubprocessRunner executing the command.
        step_name: str naming the step for an error message.
        plugin_spec_step: bool indicating whether this step installs `--plugin-spec`.
        plugin_spec_is_default: bool indicating whether `--plugin-spec` was defaulted.

    Raises:
        PluginSpecResolutionError: A defaulted plugin spec failed to resolve.
        ProvisioningError: Any other step failed.
    """

    LOGGER.info(f"Running provisioning step '{step_name}': {' '.join(args)}")
    try:
        result = runner(args, capture_output=True, text=True, check=False)
    except OSError as error:
        raise ProvisioningError(
            f"Could not run provisioning step '{step_name}': {error}"
        ) from error
    if result.returncode == 0:
        return
    detail = (result.stderr or result.stdout or "").strip()
    if plugin_spec_step and plugin_spec_is_default:
        raise PluginSpecResolutionError(
            f"Step '{step_name}' failed to resolve the default `--plugin-spec` "
            f"(exit {result.returncode}). If this checkout's version is unreleased, it "
            f"will not resolve from an index -- pass `--plugin-spec` explicitly (a "
            f"released version, a VCS ref, or a local path). Detail: {detail}"
        )
    raise ProvisioningError(
        f"Provisioning step '{step_name}' failed (exit {result.returncode}): {detail}"
    )


def _project_install_args(python_path: Path, project_dir: Path) -> list[str] | None:
    """Select the project install command implied by `--project-dir`'s contents.

    Parameters:
        python_path: pathlib.Path containing the venv's `python` executable.
        project_dir: pathlib.Path containing the consumer project to test.

    Returns:
        list[str] | None containing the `uv pip install` arguments, or None to skip
        project installation entirely (no recognized project file was present).
    """

    if (project_dir / "pyproject.toml").is_file() or (project_dir / "setup.py").is_file():
        return [
            "pip",
            "install",
            "--python",
            str(python_path),
            "--editable",
            str(project_dir),
            "--no-deps",
        ]
    requirements = project_dir / "requirements.txt"
    if requirements.is_file():
        return ["pip", "install", "--python", str(python_path), "--requirement", str(requirements)]
    return None


def provision_environment(
    *,
    family: AirflowFamily,
    airflow_version: str,
    python_version: str,
    plugin_spec: str,
    plugin_spec_is_default: bool,
    project_dir: Path,
    work_dir: Path,
    uv_path: Path,
    constraints_fetcher: ConstraintsFetcher = fetch_constraints,
    runner: SubprocessRunner = subprocess.run,
) -> ProvisionedEnvironment:
    """Create one `uv` virtual environment with Airflow and the plugin installed.

    Airflow 2.x installs in two passes (Airflow under constraints, then the plugin's
    `airflow2` extra unconstrained with a pinned pytest ceiling); Airflow 3.x installs
    in a single pass, plugin and Airflow core together under constraints. Both then
    optionally install `--project-dir`'s own project: editable with `--no-deps` when a
    `pyproject.toml`/`setup.py` is present, a plain `requirements.txt` install when only
    that is present, or nothing at all.

    Parameters:
        family: AirflowFamily selecting the two-pass (V2) or single-pass (V3) install.
        airflow_version: str containing the Airflow release to install.
        python_version: str containing the `X.Y` Python version for the venv.
        plugin_spec: str containing the `uv pip install` spec for the plugin itself.
        plugin_spec_is_default: bool indicating whether `plugin_spec` was defaulted,
            gating whether an install failure raises `PluginSpecResolutionError`.
        project_dir: pathlib.Path containing the consumer project to test.
        work_dir: pathlib.Path containing this run's scratch directory.
        uv_path: pathlib.Path containing the resolved `uv` executable.
        constraints_fetcher: ConstraintsFetcher downloading the constraints file.
        runner: SubprocessRunner executing every `uv` invocation.

    Returns:
        ProvisionedEnvironment containing the ready-to-run virtual environment.

    Raises:
        PluginSpecResolutionError: The defaulted `--plugin-spec` failed to resolve.
        ProvisioningError: Any other provisioning step failed.
    """

    venv_dir = work_dir / f"venv-{family.name.lower()}"
    _run_step(
        [str(uv_path), "venv", "--python", python_version, str(venv_dir)],
        runner=runner,
        step_name=f"create {family.name} venv",
        plugin_spec_step=False,
        plugin_spec_is_default=plugin_spec_is_default,
    )
    python_path = (venv_dir / "bin" / "python").resolve()

    constraints_text = constraints_fetcher(airflow_version, python_version)
    constraints_path = work_dir / f"constraints-{family.name.lower()}.txt"
    constraints_path.write_text(constraints_text, encoding="utf-8")

    if family is AirflowFamily.V2:
        _run_step(
            [
                str(uv_path),
                "pip",
                "install",
                "--python",
                str(python_path),
                "--constraint",
                str(constraints_path),
                f"apache-airflow=={airflow_version}",
            ],
            runner=runner,
            step_name="install Airflow 2.x under constraints",
            plugin_spec_step=False,
            plugin_spec_is_default=plugin_spec_is_default,
        )
        _run_step(
            [
                str(uv_path),
                "pip",
                "install",
                "--python",
                str(python_path),
                f"{plugin_spec}[airflow2]",
                V2_UNCONSTRAINED_PYTEST_SPEC,
            ],
            runner=runner,
            step_name="install plugin (airflow2 extra, unconstrained)",
            plugin_spec_step=True,
            plugin_spec_is_default=plugin_spec_is_default,
        )
    else:
        _run_step(
            [
                str(uv_path),
                "pip",
                "install",
                "--python",
                str(python_path),
                "--constraint",
                str(constraints_path),
                f"{plugin_spec}[airflow3]",
                f"apache-airflow-core=={airflow_version}",
                V3_SQLITE_PROVIDER_SPEC,
            ],
            runner=runner,
            step_name="install plugin and Airflow 3.x core under constraints",
            plugin_spec_step=True,
            plugin_spec_is_default=plugin_spec_is_default,
        )

    project_install_args = _project_install_args(python_path, project_dir)
    if project_install_args is not None:
        _run_step(
            [str(uv_path), *project_install_args],
            runner=runner,
            step_name="install project under test",
            plugin_spec_step=False,
            plugin_spec_is_default=plugin_spec_is_default,
        )

    return ProvisionedEnvironment(
        family=family,
        airflow_version=airflow_version,
        python_version=python_version,
        venv_dir=venv_dir,
        python_path=python_path,
    )


__all__ = (
    "CONSTRAINTS_URL_TEMPLATE",
    "V2_UNCONSTRAINED_PYTEST_SPEC",
    "V3_SQLITE_PROVIDER_SPEC",
    "ConstraintsFetcher",
    "ProvisionedEnvironment",
    "SubprocessRunner",
    "fetch_constraints",
    "provision_environment",
)
