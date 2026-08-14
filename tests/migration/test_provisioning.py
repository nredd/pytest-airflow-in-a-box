"""Test environment provisioning through injected `uv`/network seams, no real `uv`."""

from __future__ import annotations

import subprocess
import urllib.error
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.migration.provision import (
    V2_UNCONSTRAINED_PYTEST_SPEC,
    V3_SQLITE_PROVIDER_SPEC,
    _extras_qualified_spec,
    _write_constraints,
    fetch_constraints,
    provision_environment,
    redact_command_line,
)
from pytest_airflow_in_a_box.migration.types import PluginSpecResolutionError, ProvisioningError

_UV_PATH = Path("/usr/local/bin/uv")


class _RecordingRunner:
    """Record every invocation and return a canned result per call, defaulting to ok.

    Asserting `check=False` on every call is deliberate: real `subprocess.run(...,
    check=True)` raises `CalledProcessError` (a different exception than the `OSError`
    `_run_step` catches) for any nonzero exit, so a fake that ignored `check` entirely
    could not catch a regression to `check=True` -- it would keep "tolerating" nonzero
    exits in tests while the real invocation started crashing instead.
    """

    def __init__(
        self, *, results: dict[int, subprocess.CompletedProcess[str]] | None = None
    ) -> None:
        self.calls: list[list[str]] = []
        self._results = results or {}

    def __call__(self, args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert kwargs.get("check") is False, "provisioning steps must never use check=True"
        index = len(self.calls)
        self.calls.append(args)
        return self._results.get(index, subprocess.CompletedProcess(args, 0, "", ""))


class _FakeResponse:
    """A context-managed stand-in for `urllib.request.urlopen`'s response object."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        """Return the canned response body.

        Returns:
            bytes containing the canned response body.
        """

        return self._body

    def __enter__(self) -> _FakeResponse:
        """Enter the response context.

        Returns:
            _FakeResponse containing this response.
        """

        return self

    def __exit__(self, *_args: object) -> None:
        """Exit the response context, releasing nothing."""

        return


def test_fetch_constraints_returns_decoded_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """Decode the constraints file body from the fetched response."""

    def fake_urlopen(_url: str, timeout: float) -> _FakeResponse:
        del timeout
        return _FakeResponse(b"apache-airflow==3.3.0\n")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    body = fetch_constraints("3.3.0", "3.12")

    assert body == "apache-airflow==3.3.0\n"


def test_fetch_constraints_wraps_network_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrap a network failure fetching constraints as a ProvisioningError."""

    def fake_urlopen(_url: str, timeout: float) -> None:
        del timeout
        raise urllib.error.URLError("no network")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    with pytest.raises(ProvisioningError, match="Could not fetch Airflow constraints"):
        fetch_constraints("3.3.0", "3.12")


# --- _extras_qualified_spec: PEP 508 requires extras before any version/URL part -----


def test_extras_qualified_spec_appends_to_a_bare_name() -> None:
    """Append `[extra]` directly for a bare package name (no version, no URL)."""
    assert _extras_qualified_spec("pytest-airflow-in-a-box", "airflow2") == (
        "pytest-airflow-in-a-box[airflow2]"
    )


def test_extras_qualified_spec_inserts_before_a_version_specifier() -> None:
    """Insert `[extra]` before a `==` version pin -- never after it (invalid PEP 508)."""
    assert _extras_qualified_spec("pytest-airflow-in-a-box==0.4.0", "airflow2") == (
        "pytest-airflow-in-a-box[airflow2]==0.4.0"
    )


@pytest.mark.parametrize(
    ("operator", "version"),
    [(">=", "0.4"), ("<=", "1.0"), ("~=", "0.4.0"), ("!=", "0.3.0"), (">", "0.1"), ("<", "1.0")],
)
def test_extras_qualified_spec_handles_every_pep440_operator(operator: str, version: str) -> None:
    """Insert `[extra]` before every PEP 440 version specifier operator, not just `==`."""
    spec = f"pytest-airflow-in-a-box{operator}{version}"

    assert _extras_qualified_spec(spec, "airflow3") == (
        f"pytest-airflow-in-a-box[airflow3]{operator}{version}"
    )


def test_extras_qualified_spec_inserts_before_a_direct_url_reference() -> None:
    """Insert `[extra]` before a PEP 508 `@ url` direct reference (a VCS ref)."""
    spec = "pytest-airflow-in-a-box @ git+https://github.com/nredd/pytest-airflow-in-a-box@main"

    result = _extras_qualified_spec(spec, "airflow2")

    assert result == (
        "pytest-airflow-in-a-box[airflow2] @ "
        "git+https://github.com/nredd/pytest-airflow-in-a-box@main"
    )


def test_extras_qualified_spec_appends_to_a_bare_local_path() -> None:
    """Append `[extra]` at the end for a bare local path -- the only valid syntax there."""
    assert _extras_qualified_spec("./dist/pytest_airflow_in_a_box-0.5.0.tar.gz", "airflow3") == (
        "./dist/pytest_airflow_in_a_box-0.5.0.tar.gz[airflow3]"
    )


# --- redact_command_line --------------------------------------------------------------


def test_redact_command_line_masks_embedded_credentials() -> None:
    """Redact a `scheme://user:pass@host` credential a VCS `--plugin-spec` might carry."""
    args = [
        "/opt/uv",
        "pip",
        "install",
        "pytest-airflow-in-a-box[airflow3] @ git+https://oauth2:secrettoken@github.com/x/y",
    ]

    rendered = redact_command_line(args)

    assert "secrettoken" not in rendered
    assert "oauth2" not in rendered
    assert "git+https://<redacted>@github.com/x/y" in rendered


def test_redact_command_line_leaves_plain_arguments_unchanged() -> None:
    """Leave an ordinary, credential-free command line untouched."""
    args = ["/opt/uv", "venv", "--python", "3.12", "/work/venv-v3"]

    assert redact_command_line(args) == "/opt/uv venv --python 3.12 /work/venv-v3"


# --- _write_constraints ----------------------------------------------------------------


def test_write_constraints_writes_the_fetched_body(tmp_path: Path) -> None:
    """Write the fetched constraints text to the given path."""
    constraints_path = tmp_path / "constraints.txt"

    _write_constraints(constraints_path, "apache-airflow==3.3.0\n")

    assert constraints_path.read_text(encoding="utf-8") == "apache-airflow==3.3.0\n"


def test_write_constraints_wraps_an_io_failure(tmp_path: Path) -> None:
    """Wrap a write failure (for example, a missing parent directory) as ProvisioningError."""
    constraints_path = tmp_path / "does-not-exist" / "constraints.txt"

    with pytest.raises(ProvisioningError, match="Could not write the fetched constraints file"):
        _write_constraints(constraints_path, "apache-airflow==3.3.0\n")


def test_provision_environment_v2_two_pass_install(tmp_path: Path) -> None:
    """Install Airflow 2.x under constraints, then the plugin unconstrained."""
    runner = _RecordingRunner()

    env = provision_environment(
        family=AirflowFamily.V2,
        airflow_version="2.11.2",
        python_version="3.12",
        plugin_spec="pytest-airflow-in-a-box==0.4.0",
        plugin_spec_is_default=False,
        project_dir=tmp_path,
        work_dir=tmp_path,
        uv_path=_UV_PATH,
        constraints_fetcher=lambda _version, _python: "apache-airflow==2.11.2\n",
        runner=runner,
    )

    assert env.family is AirflowFamily.V2
    assert env.airflow_version == "2.11.2"
    assert env.venv_dir == tmp_path / "venv-v2"
    # venv create, Airflow under constraints, plugin unconstrained -- no project install
    # since tmp_path (used as both project_dir and work_dir here) has no project file.
    assert len(runner.calls) == 3
    venv_call = runner.calls[0]
    assert venv_call[:2] == [str(_UV_PATH), "venv"]
    airflow_call = runner.calls[1]
    assert "apache-airflow==2.11.2" in airflow_call
    assert "--constraint" in airflow_call


def test_provision_environment_v2_second_pass_installs_plugin_unconstrained(
    tmp_path: Path,
) -> None:
    """Install the plugin's `airflow2` extra unconstrained with a pinned pytest ceiling."""
    runner = _RecordingRunner()

    provision_environment(
        family=AirflowFamily.V2,
        airflow_version="2.11.2",
        python_version="3.12",
        plugin_spec="pytest-airflow-in-a-box==0.4.0",
        plugin_spec_is_default=False,
        project_dir=tmp_path,
        work_dir=tmp_path,
        uv_path=_UV_PATH,
        constraints_fetcher=lambda _version, _python: "\n",
        runner=runner,
    )

    plugin_call = runner.calls[2]
    # Extras must precede the version specifier -- see the _extras_qualified_spec tests.
    assert "pytest-airflow-in-a-box[airflow2]==0.4.0" in plugin_call
    assert V2_UNCONSTRAINED_PYTEST_SPEC in plugin_call
    assert "--constraint" not in plugin_call


def test_provision_environment_v3_two_pass_install(tmp_path: Path) -> None:
    """Install Airflow 3.x core+provider under constraints, then the plugin unconstrained."""
    runner = _RecordingRunner()

    env = provision_environment(
        family=AirflowFamily.V3,
        airflow_version="3.3.1",
        python_version="3.12",
        plugin_spec="pytest-airflow-in-a-box==0.4.0",
        plugin_spec_is_default=False,
        project_dir=tmp_path,
        work_dir=tmp_path,
        uv_path=_UV_PATH,
        constraints_fetcher=lambda _version, _python: "\n",
        runner=runner,
    )

    assert env.venv_dir == tmp_path / "venv-v3"
    # venv create, core+provider under constraints, plugin unconstrained.
    assert len(runner.calls) == 3
    core_call = runner.calls[1]
    assert "apache-airflow-core==3.3.1" in core_call
    assert V3_SQLITE_PROVIDER_SPEC in core_call
    assert "--constraint" in core_call
    plugin_call = runner.calls[2]
    assert "pytest-airflow-in-a-box[airflow3]==0.4.0" in plugin_call
    assert "--constraint" not in plugin_call


def test_provision_environment_installs_editable_project_with_pyproject(tmp_path: Path) -> None:
    """Install `--project-dir` editable, without deps, when a `pyproject.toml` exists."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    runner = _RecordingRunner()

    provision_environment(
        family=AirflowFamily.V3,
        airflow_version="3.3.1",
        python_version="3.12",
        plugin_spec="pytest-airflow-in-a-box==0.4.0",
        plugin_spec_is_default=False,
        project_dir=project_dir,
        work_dir=tmp_path,
        uv_path=_UV_PATH,
        constraints_fetcher=lambda _version, _python: "\n",
        runner=runner,
    )

    project_call = runner.calls[-1]
    assert "--editable" in project_call
    assert str(project_dir) in project_call
    assert "--no-deps" in project_call


def test_provision_environment_installs_requirements_txt_when_no_project_file(
    tmp_path: Path,
) -> None:
    """Install a plain `requirements.txt` when neither `pyproject.toml` nor `setup.py` exists."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    runner = _RecordingRunner()

    provision_environment(
        family=AirflowFamily.V3,
        airflow_version="3.3.1",
        python_version="3.12",
        plugin_spec="pytest-airflow-in-a-box==0.4.0",
        plugin_spec_is_default=False,
        project_dir=project_dir,
        work_dir=tmp_path,
        uv_path=_UV_PATH,
        constraints_fetcher=lambda _version, _python: "\n",
        runner=runner,
    )

    project_call = runner.calls[-1]
    assert "--requirement" in project_call
    assert str(project_dir / "requirements.txt") in project_call


def test_provision_environment_skips_project_install_when_nothing_recognized(
    tmp_path: Path,
) -> None:
    """Skip project installation entirely when no recognized project file is present."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    runner = _RecordingRunner()

    provision_environment(
        family=AirflowFamily.V3,
        airflow_version="3.3.1",
        python_version="3.12",
        plugin_spec="pytest-airflow-in-a-box==0.4.0",
        plugin_spec_is_default=False,
        project_dir=project_dir,
        work_dir=tmp_path,
        uv_path=_UV_PATH,
        constraints_fetcher=lambda _version, _python: "\n",
        runner=runner,
    )

    # venv create, core+provider under constraints, plugin unconstrained -- no project step.
    assert len(runner.calls) == 3


def test_provision_environment_raises_on_venv_creation_failure(tmp_path: Path) -> None:
    """Raise ProvisioningError when `uv venv` fails."""
    runner = _RecordingRunner(
        results={0: subprocess.CompletedProcess([], 1, "", "no python 3.99")}
    )

    with pytest.raises(ProvisioningError, match=r"no python 3\.99"):
        provision_environment(
            family=AirflowFamily.V3,
            airflow_version="3.3.1",
            python_version="3.99",
            plugin_spec="pytest-airflow-in-a-box==0.4.0",
            plugin_spec_is_default=False,
            project_dir=tmp_path,
            work_dir=tmp_path,
            uv_path=_UV_PATH,
            constraints_fetcher=lambda _version, _python: "\n",
            runner=runner,
        )


def test_provision_environment_raises_when_runner_cannot_exec(tmp_path: Path) -> None:
    """Wrap an OSError from a missing/unexecutable `uv` binary as ProvisioningError."""

    def broken_runner(_args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise OSError("no such file or directory")

    with pytest.raises(ProvisioningError, match="Could not run provisioning step"):
        provision_environment(
            family=AirflowFamily.V3,
            airflow_version="3.3.1",
            python_version="3.12",
            plugin_spec="pytest-airflow-in-a-box==0.4.0",
            plugin_spec_is_default=False,
            project_dir=tmp_path,
            work_dir=tmp_path,
            uv_path=_UV_PATH,
            constraints_fetcher=lambda _version, _python: "\n",
            runner=broken_runner,
        )


def test_provision_environment_project_install_failure_raises_provisioning_error(
    tmp_path: Path,
) -> None:
    """Raise ProvisioningError, not PluginSpecResolutionError, for a project install failure."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    # venv=0, core+provider=1, plugin=2, project=3.
    runner = _RecordingRunner(
        results={3: subprocess.CompletedProcess([], 1, "", "conflicting dependency")}
    )

    with pytest.raises(ProvisioningError, match="conflicting dependency"):
        provision_environment(
            family=AirflowFamily.V3,
            airflow_version="3.3.1",
            python_version="3.12",
            plugin_spec="pytest-airflow-in-a-box==0.4.0",
            plugin_spec_is_default=True,
            project_dir=project_dir,
            work_dir=tmp_path,
            uv_path=_UV_PATH,
            constraints_fetcher=lambda _version, _python: "\n",
            runner=runner,
        )


def test_provision_environment_v2_plugin_step_failure_with_default_spec_names_the_fix(
    tmp_path: Path,
) -> None:
    """Attribute a defaulted `--plugin-spec` install failure to an unreleased version.

    This is the directed regression test for the unreleased-version resolution
    failure: `--plugin-spec` defaults to this checkout's installed version, and an
    unreleased version will not resolve from an index. The orchestrator must name
    `--plugin-spec` as the fix rather than surfacing a bare `uv` failure.
    """
    runner = _RecordingRunner(
        results={2: subprocess.CompletedProcess([], 1, "", "No solution found")}
    )

    with pytest.raises(PluginSpecResolutionError, match="--plugin-spec"):
        provision_environment(
            family=AirflowFamily.V2,
            airflow_version="2.11.2",
            python_version="3.12",
            plugin_spec="pytest-airflow-in-a-box==99.0.0",
            plugin_spec_is_default=True,
            project_dir=tmp_path,
            work_dir=tmp_path,
            uv_path=_UV_PATH,
            constraints_fetcher=lambda _version, _python: "\n",
            runner=runner,
        )


def test_provision_environment_v3_plugin_step_failure_with_explicit_spec_is_ordinary_error(
    tmp_path: Path,
) -> None:
    """Raise an ordinary ProvisioningError for an explicit (non-defaulted) plugin spec."""
    # venv=0, core+provider=1, plugin=2 (the now-separate plugin-install pass).
    runner = _RecordingRunner(
        results={2: subprocess.CompletedProcess([], 1, "", "No solution found")}
    )

    with pytest.raises(ProvisioningError) as excinfo:
        provision_environment(
            family=AirflowFamily.V3,
            airflow_version="3.3.1",
            python_version="3.12",
            plugin_spec="pytest-airflow-in-a-box==99.0.0",
            plugin_spec_is_default=False,
            project_dir=tmp_path,
            work_dir=tmp_path,
            uv_path=_UV_PATH,
            constraints_fetcher=lambda _version, _python: "\n",
            runner=runner,
        )
    assert not isinstance(excinfo.value, PluginSpecResolutionError)


def test_provision_environment_v3_core_step_failure_with_default_spec_is_not_misattributed(
    tmp_path: Path,
) -> None:
    """Never blame `--plugin-spec` for a failure in the (now-separate) core install step.

    Splitting Airflow 3.x into two passes (core+provider, then the plugin extra) is
    exactly what makes this possible: a bad `--airflow3-version` or a core/provider
    conflict fails at the core step, which is never `plugin_spec_step=True`, so it can
    never be misattributed to `--plugin-spec` even when the spec was defaulted.
    """
    runner = _RecordingRunner(
        results={1: subprocess.CompletedProcess([], 1, "", "no version 3.3.1 found")}
    )

    with pytest.raises(ProvisioningError) as excinfo:
        provision_environment(
            family=AirflowFamily.V3,
            airflow_version="3.3.1",
            python_version="3.12",
            plugin_spec="pytest-airflow-in-a-box==0.4.0",
            plugin_spec_is_default=True,
            project_dir=tmp_path,
            work_dir=tmp_path,
            uv_path=_UV_PATH,
            constraints_fetcher=lambda _version, _python: "\n",
            runner=runner,
        )
    assert not isinstance(excinfo.value, PluginSpecResolutionError)
    assert "no version 3.3.1 found" in str(excinfo.value)
