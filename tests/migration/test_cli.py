"""Test the `airflow-migration-diff` console entry point."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.migration import cli
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    CategoryCounts,
    DiffResult,
    ProvisioningError,
    RecordRunError,
    UvNotFoundError,
)


def _parse(*argv: str) -> argparse.Namespace:
    """Parse this program's own arguments through the real parser.

    Parameters:
        argv: str values containing the program's own command-line arguments.

    Returns:
        argparse.Namespace containing the parsed options.
    """

    return cli.build_parser().parse_args(list(argv))


def _env(family: AirflowFamily) -> ProvisionedEnvironment:
    """Build a canned `ProvisionedEnvironment` for one Airflow family.

    Parameters:
        family: AirflowFamily naming the provisioned Airflow major.

    Returns:
        ProvisionedEnvironment containing the constructed environment.
    """

    return ProvisionedEnvironment(
        family=family,
        airflow_version="2.11.2" if family is AirflowFamily.V2 else "3.3.1",
        python_version="3.12",
        venv_dir=Path(f"/work/venv-{family.name.lower()}"),
        python_path=Path(f"/work/venv-{family.name.lower()}/bin/python"),
    )


_NO_REGRESSIONS = DiffResult(
    counts=CategoryCounts(
        regression=0, fixed=1, broken_on_both=0, still_passing=3, gated=0, new=0, missing=0
    ),
    regression_nodeids=(),
    fixed_nodeids=("tests/test_a.py::test_fixed",),
    new_nodeids=(),
    missing_nodeids=(),
    warnings=(),
)
_WITH_REGRESSIONS = DiffResult(
    counts=CategoryCounts(
        regression=1, fixed=0, broken_on_both=0, still_passing=3, gated=0, new=0, missing=0
    ),
    regression_nodeids=("tests/test_a.py::test_regressed",),
    fixed_nodeids=(),
    new_nodeids=(),
    missing_nodeids=(),
    warnings=(),
)


# --- _split_pytest_args -------------------------------------------------------------


def test_split_pytest_args_without_separator_forwards_nothing() -> None:
    """Treat every argument as this program's own when no `--` separator is present."""
    own, pytest_args = cli._split_pytest_args(["--project-dir", "."])

    assert own == ["--project-dir", "."]
    assert pytest_args == []


def test_split_pytest_args_with_separator_splits_on_it() -> None:
    """Forward every argument after a literal `--` to pytest."""
    own, pytest_args = cli._split_pytest_args(["--project-dir", ".", "--", "-k", "smoke"])

    assert own == ["--project-dir", "."]
    assert pytest_args == ["-k", "smoke"]


# --- defaulting helpers ---------------------------------------------------------------


@pytest.mark.parametrize("family", [AirflowFamily.V2, AirflowFamily.V3])
def test_max_release_returns_a_dotted_version_string(family: AirflowFamily) -> None:
    """Render the newest certified release for a family as a dotted version string."""
    release = cli._max_release(family)

    assert release.count(".") == 2


def test_default_python_v2_uses_current_interpreter_when_in_range() -> None:
    """Default the 2.x Python version to the current interpreter when it is supported."""
    assert cli._default_python_v2((3, 11), "2.11.2") == "3.11"


def test_default_python_v2_clamps_to_the_requested_release_ceiling() -> None:
    """Clamp an over-cap interpreter to the requested release's own ceiling.

    2.11.2 reaches 3.12 while 2.7.3 and 2.8.4 cap at 3.11, so a family-wide clamp
    would provision a 3.12 environment for a release with no 3.12 constraints file.
    """
    assert cli._default_python_v2((3, 14), "2.11.2") == "3.12"
    assert cli._default_python_v2((3, 12), "2.7.3") == "3.11"
    assert cli._default_python_v2((3, 14), "2.8.4") == "3.11"


def test_default_python_v2_clamps_up_to_the_floor() -> None:
    """Clamp an interpreter below the 2.x floor up to it rather than past the ceiling.

    Unreachable through the console script -- this package's own `requires-python` is
    `>=3.10` -- but `resolve_config` accepts an injected `current_python`, so the bound
    is pinned rather than assumed.
    """
    assert cli._default_python_v2((3, 8), "2.7.3") == "3.10"


def test_default_python_v3_uses_current_interpreter_when_in_range() -> None:
    """Default the 3.x Python version to the current interpreter when it is supported."""
    assert cli._default_python_v3((3, 13)) == "3.13"


def test_default_python_v3_falls_back_when_out_of_range() -> None:
    """Fall back to the known-good Python version outside the 3.x support matrix."""
    assert cli._default_python_v3((3, 9)) == cli._FALLBACK_PYTHON


def test_default_make_temp_dir_creates_a_real_prefixed_directory() -> None:
    """Create a real, uniquely-prefixed temporary directory."""
    created = Path(cli._default_make_temp_dir())

    try:
        assert created.is_dir()
        assert created.name.startswith(cli.WORK_DIR_PREFIX)
    finally:
        created.rmdir()


def test_default_plugin_spec_pins_the_installed_version() -> None:
    """Default `--plugin-spec` to a pinned spec for this checkout's installed version."""
    from pytest_airflow_in_a_box import __version__

    assert cli._default_plugin_spec() == f"pytest-airflow-in-a-box=={__version__}"


# --- resolve_config ---------------------------------------------------------------


def test_resolve_config_uses_explicit_uv_path_without_calling_which(tmp_path: Path) -> None:
    """Skip `which` entirely when `--uv-path` is given explicitly."""
    args = _parse("--uv-path", "/opt/uv", "--work-dir", str(tmp_path))

    def unused_which(_name: str) -> str | None:
        pytest.fail("which() must not be called when --uv-path is given")

    cfg = cli.resolve_config(args, [], which=unused_which)

    assert cfg.uv_path == Path("/opt/uv")


def test_resolve_config_resolves_uv_path_from_which(tmp_path: Path) -> None:
    """Resolve `--uv-path` from `$PATH` through the injected `which` seam."""
    args = _parse("--work-dir", str(tmp_path))

    cfg = cli.resolve_config(args, [], which=lambda _name: "/usr/local/bin/uv")

    assert cfg.uv_path == Path("/usr/local/bin/uv")


def test_resolve_config_raises_uv_not_found_when_which_returns_none(tmp_path: Path) -> None:
    """Raise UvNotFoundError when `uv` is not found on `$PATH`."""
    args = _parse("--work-dir", str(tmp_path))

    with pytest.raises(UvNotFoundError, match="uv"):
        cli.resolve_config(args, [], which=lambda _name: None)


def test_resolve_config_creates_fresh_run_subdirectory_under_explicit_work_dir(
    tmp_path: Path,
) -> None:
    """Create a fresh run subdirectory inside a reused `--work-dir`, not the dir itself."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")

    first = cli.resolve_config(args, [])
    second = cli.resolve_config(args, [])

    assert first.work_dir != second.work_dir
    assert first.work_dir.parent == tmp_path
    assert second.work_dir.parent == tmp_path


def test_resolve_config_wraps_explicit_work_dir_creation_failure(tmp_path: Path) -> None:
    """Raise ProvisioningError, not a raw OSError, when `--work-dir` cannot be created.

    A raw, unwrapped OSError here would propagate out of `main()`'s `resolve_config`
    call and hit Python's default unhandled-exception exit status of 1 -- identical to
    EXIT_REGRESSIONS. This is the unit-level half of that guard; `test_main_wraps_an_
    unexpected_exception_from_resolve_config` below is the `main()`-level half.
    """
    # A file, not a directory, in the exact spot `--work-dir` names: `mkdir` on it fails.
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")
    args = _parse("--work-dir", str(blocked), "--uv-path", "/opt/uv")

    with pytest.raises(ProvisioningError, match="Could not create --work-dir"):
        cli.resolve_config(args, [])


def test_make_run_dir_wraps_mkdir_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise ProvisioningError, not a raw OSError, when the per-run subdirectory fails.

    Exercised directly against `_make_run_dir` (rather than through a real filesystem
    permission failure, which behaves inconsistently when tests run as root) with a
    monkeypatched `Path.mkdir`, isolating this from the base-directory creation branch
    `test_resolve_config_wraps_explicit_work_dir_creation_failure` already covers.
    """

    def failing_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        del self, args, kwargs
        raise OSError("disk full")

    monkeypatch.setattr(Path, "mkdir", failing_mkdir)

    with pytest.raises(ProvisioningError, match="Could not create the run's work directory"):
        cli._make_run_dir(tmp_path)


def test_resolve_config_defaults_work_dir_through_make_temp_dir_seam(tmp_path: Path) -> None:
    """Create the base work directory through the injected `make_temp_dir` seam."""
    args = _parse("--uv-path", "/opt/uv")
    base = tmp_path / "base"
    base.mkdir()

    cfg = cli.resolve_config(args, [], make_temp_dir=lambda: str(base))

    assert cfg.work_dir.parent == base


def test_resolve_config_defaults_artifact_paths_under_work_dir(tmp_path: Path) -> None:
    """Default `--baseline-artifact`/`--live-artifact` under the run's work directory."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")

    cfg = cli.resolve_config(args, [])

    assert cfg.baseline_artifact == cfg.work_dir / "baseline.json"
    assert cfg.live_artifact == cfg.work_dir / "live.json"


def test_resolve_config_honors_explicit_artifact_paths(tmp_path: Path) -> None:
    """Use explicit `--baseline-artifact`/`--live-artifact` paths verbatim."""
    baseline = tmp_path / "custom-baseline.json"
    live = tmp_path / "custom-live.json"
    args = _parse(
        "--work-dir",
        str(tmp_path),
        "--uv-path",
        "/opt/uv",
        "--baseline-artifact",
        str(baseline),
        "--live-artifact",
        str(live),
    )

    cfg = cli.resolve_config(args, [])

    assert cfg.baseline_artifact == baseline.resolve()
    assert cfg.live_artifact == live.resolve()


def test_resolve_config_marks_default_plugin_spec(tmp_path: Path) -> None:
    """Mark the plugin spec as defaulted when `--plugin-spec` is absent."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")

    cfg = cli.resolve_config(args, [])

    assert cfg.plugin_spec_is_default is True
    assert cfg.plugin_spec == cli._default_plugin_spec()


def test_resolve_config_honors_explicit_plugin_spec(tmp_path: Path) -> None:
    """Use an explicit `--plugin-spec` verbatim and mark it as not defaulted."""
    args = _parse(
        "--work-dir",
        str(tmp_path),
        "--uv-path",
        "/opt/uv",
        "--plugin-spec",
        "pytest-airflow-in-a-box==0.1.0",
    )

    cfg = cli.resolve_config(args, [])

    assert cfg.plugin_spec_is_default is False
    assert cfg.plugin_spec == "pytest-airflow-in-a-box==0.1.0"


def test_resolve_config_defaults_versions_and_python_to_certified_matrix(tmp_path: Path) -> None:
    """Default Airflow versions to the newest certified release per family."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")

    cfg = cli.resolve_config(args, [], current_python=(3, 11))

    assert cfg.airflow2_version == cli._max_release(AirflowFamily.V2)
    assert cfg.airflow3_version == cli._max_release(AirflowFamily.V3)
    assert cfg.python_airflow2 == "3.11"
    assert cfg.python_airflow3 == "3.11"


def test_resolve_config_defaults_python_v2_from_the_requested_release(tmp_path: Path) -> None:
    """Default `--python-airflow2` from `--airflow2-version`, not the family ceiling.

    Parameters:
        tmp_path: pathlib.Path anchoring the run's work directory.
    """
    args = _parse(
        "--work-dir",
        str(tmp_path),
        "--uv-path",
        "/opt/uv",
        "--airflow2-version",
        "2.7.3",
    )

    cfg = cli.resolve_config(args, [], current_python=(3, 12))

    assert cfg.airflow2_version == "2.7.3"
    assert cfg.python_airflow2 == "3.11"


def test_resolve_config_honors_explicit_versions_and_python(tmp_path: Path) -> None:
    """Use explicit version and Python overrides verbatim."""
    args = _parse(
        "--work-dir",
        str(tmp_path),
        "--uv-path",
        "/opt/uv",
        "--airflow2-version",
        "2.9.3",
        "--airflow3-version",
        "3.1.0",
        "--python-airflow2",
        "3.10",
        "--python-airflow3",
        "3.14",
    )

    cfg = cli.resolve_config(args, [])

    assert cfg.airflow2_version == "2.9.3"
    assert cfg.airflow3_version == "3.1.0"
    assert cfg.python_airflow2 == "3.10"
    assert cfg.python_airflow3 == "3.14"


def test_resolve_config_forwards_pytest_args_as_a_tuple(tmp_path: Path) -> None:
    """Forward the split-out pytest arguments as an immutable tuple."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")

    cfg = cli.resolve_config(args, ["-k", "smoke", "-x"])

    assert cfg.pytest_args == ("-k", "smoke", "-x")


def test_resolve_config_honors_keep_work_dir_and_incomplete_overrides(tmp_path: Path) -> None:
    """Carry `--keep-work-dir` and both `--allow-incomplete-*` flags through verbatim."""
    args = _parse(
        "--work-dir",
        str(tmp_path),
        "--uv-path",
        "/opt/uv",
        "--keep-work-dir",
        "--allow-incomplete-baseline",
        "--allow-incomplete-live",
    )

    cfg = cli.resolve_config(args, [])

    assert cfg.keep_work_dir is True
    assert cfg.allow_incomplete_baseline is True
    assert cfg.allow_incomplete_live is True


def test_resolve_config_defaults_project_dir_to_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Default `--project-dir` to the current working directory."""
    monkeypatch.chdir(tmp_path)
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")

    cfg = cli.resolve_config(args, [])

    assert cfg.project_dir == tmp_path.resolve()


# --- execute -----------------------------------------------------------------------


def test_execute_returns_ok_and_report_with_no_regressions(tmp_path: Path) -> None:
    """Provision both environments, record both passes, and render a clean report."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")
    cfg = cli.resolve_config(args, ["-k", "smoke"])
    provisioned: list[AirflowFamily] = []
    recorded: list[tuple[str, Path | None]] = []

    def fake_provision(*, family: AirflowFamily, **_kwargs: object) -> ProvisionedEnvironment:
        provisioned.append(family)
        return _env(family)

    def fake_run_record(
        *,
        env: ProvisionedEnvironment,
        project_dir: Path,
        record_path: Path,
        baseline_path: Path | None,
        pytest_args: tuple[str, ...],
    ) -> None:
        del project_dir, record_path
        assert pytest_args == ("-k", "smoke")
        recorded.append((env.family.name, baseline_path))

    def fake_seam(
        baseline_path: Path, live_path: Path, allow_baseline: bool, allow_live: bool
    ) -> DiffResult:
        del baseline_path, live_path, allow_baseline, allow_live
        return _NO_REGRESSIONS

    exit_code, report = cli.execute(
        cfg,
        provision_environment=fake_provision,
        run_record=fake_run_record,
        compute_category_diff=fake_seam,
    )

    assert exit_code == cli.EXIT_OK
    assert provisioned == [AirflowFamily.V2, AirflowFamily.V3]
    assert recorded == [("V2", None), ("V3", cfg.baseline_artifact)]
    assert "Result: no regressions" in report


def test_execute_returns_regressions_exit_code_when_diff_has_regressions(tmp_path: Path) -> None:
    """Return EXIT_REGRESSIONS and a REGRESSIONS FOUND report when regressions exist."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")
    cfg = cli.resolve_config(args, [])

    exit_code, report = cli.execute(
        cfg,
        provision_environment=lambda *, family, **_kwargs: _env(family),
        run_record=lambda **_kwargs: None,
        compute_category_diff=lambda *_args: _WITH_REGRESSIONS,
    )

    assert exit_code == cli.EXIT_REGRESSIONS
    assert "REGRESSIONS FOUND" in report


def test_execute_propagates_provisioning_failure(tmp_path: Path) -> None:
    """Propagate a ProvisioningError raised while provisioning either environment."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")
    cfg = cli.resolve_config(args, [])

    def failing_provision(*, family: AirflowFamily, **_kwargs: object) -> ProvisionedEnvironment:
        raise ProvisioningError(f"could not provision {family.name}")

    with pytest.raises(ProvisioningError):
        cli.execute(cfg, provision_environment=failing_provision)


def test_execute_propagates_record_run_failure(tmp_path: Path) -> None:
    """Propagate a RecordRunError raised while recording either pass."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")
    cfg = cli.resolve_config(args, [])

    def failing_run_record(**_kwargs: object) -> None:
        raise RecordRunError("no artifact")

    with pytest.raises(RecordRunError):
        cli.execute(
            cfg,
            provision_environment=lambda *, family, **_kwargs: _env(family),
            run_record=failing_run_record,
        )


def test_execute_propagates_category_diff_failure(tmp_path: Path) -> None:
    """Propagate an ArtifactError raised computing the category diff."""
    args = _parse("--work-dir", str(tmp_path), "--uv-path", "/opt/uv")
    cfg = cli.resolve_config(args, [])

    def failing_seam(*_args: object) -> DiffResult:
        raise ArtifactError("malformed artifact")

    with pytest.raises(ArtifactError):
        cli.execute(
            cfg,
            provision_environment=lambda *, family, **_kwargs: _env(family),
            run_record=lambda **_kwargs: None,
            compute_category_diff=failing_seam,
        )


# --- main ----------------------------------------------------------------------------


def test_main_returns_tooling_error_exit_code_when_resolve_config_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit 2 and print to stderr when resolving the configuration fails."""

    def failing_resolve(_args: object, _pytest_args: object, **_kwargs: object) -> None:
        raise UvNotFoundError("no uv on PATH")

    monkeypatch.setattr(cli, "resolve_config", failing_resolve)

    exit_code = cli.main([])

    assert exit_code == cli.EXIT_TOOLING_ERROR


def test_main_wraps_an_unexpected_exception_from_resolve_config(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Map any non-MigrationToolingError exception from resolve_config to exit code 2.

    Python's default unhandled-exception exit status is 1, identical to
    EXIT_REGRESSIONS -- an uncaught bug here must never be misread as "a migration
    regression was found." This is deliberately a plain RuntimeError, not one of this
    package's own typed errors, to prove the broad safety-net catch, not the specific
    MigrationToolingError path already covered above.
    """

    def buggy_resolve(_args: object, _pytest_args: object, **_kwargs: object) -> None:
        raise RuntimeError("a genuine bug, not a tooling error")

    monkeypatch.setattr(cli, "resolve_config", buggy_resolve)

    exit_code = cli.main([])

    assert exit_code == cli.EXIT_TOOLING_ERROR
    assert "a genuine bug, not a tooling error" in capsys.readouterr().err


def test_main_wraps_an_unexpected_exception_from_execute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Map any non-MigrationToolingError exception from execute() to exit code 2.

    Also proves cleanup still runs on this path: the `finally` block must remove
    `cfg.work_dir` even when `execute()` raises something this package did not define.
    """

    def buggy_execute(cfg: cli.ResolvedConfig, **_kwargs: object) -> tuple[int, str]:
        del cfg
        raise RuntimeError("a genuine bug, not a tooling error")

    monkeypatch.setattr(cli, "execute", buggy_execute)

    exit_code = cli.main(["--work-dir", str(tmp_path), "--uv-path", "/opt/uv"])

    assert exit_code == cli.EXIT_TOOLING_ERROR
    assert "a genuine bug, not a tooling error" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


def test_main_writes_resolve_config_failure_message_to_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Print the tooling error's message, prefixed with the program name, to stderr."""

    def failing_resolve(_args: object, _pytest_args: object, **_kwargs: object) -> None:
        raise UvNotFoundError("no uv on PATH")

    monkeypatch.setattr(cli, "resolve_config", failing_resolve)

    cli.main([])

    captured = capsys.readouterr()
    assert "airflow-migration-diff: no uv on PATH" in captured.err
    assert captured.out == ""


def test_main_success_writes_report_and_cleans_up_work_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Write the report to stdout, return execute's exit code, and clean up work_dir."""
    captured_pytest_args: list[tuple[str, ...]] = []

    def fake_execute(cfg: cli.ResolvedConfig, **_kwargs: object) -> tuple[int, str]:
        captured_pytest_args.append(cfg.pytest_args)
        return cli.EXIT_OK, "the report\n"

    monkeypatch.setattr(cli, "execute", fake_execute)

    exit_code = cli.main(
        ["--work-dir", str(tmp_path), "--uv-path", "/opt/uv", "--", "-k", "smoke"]
    )

    assert exit_code == cli.EXIT_OK
    captured = capsys.readouterr()
    assert captured.out == "the report\n"
    assert captured_pytest_args == [("-k", "smoke")]
    assert list(tmp_path.iterdir()) == []


def test_main_tooling_error_from_execute_still_cleans_up_work_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 2, print to stderr, and still clean up work_dir when execute() fails."""

    def failing_execute(cfg: cli.ResolvedConfig, **_kwargs: object) -> tuple[int, str]:
        del cfg
        raise ProvisioningError("boom")

    monkeypatch.setattr(cli, "execute", failing_execute)

    exit_code = cli.main(["--work-dir", str(tmp_path), "--uv-path", "/opt/uv"])

    assert exit_code == cli.EXIT_TOOLING_ERROR
    assert "boom" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


def test_main_keep_work_dir_skips_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave the run's work directory in place when `--keep-work-dir` is passed."""

    def fake_execute(cfg: cli.ResolvedConfig, **_kwargs: object) -> tuple[int, str]:
        del cfg
        return cli.EXIT_OK, "report\n"

    monkeypatch.setattr(cli, "execute", fake_execute)

    cli.main(["--work-dir", str(tmp_path), "--uv-path", "/opt/uv", "--keep-work-dir"])

    remaining = list(tmp_path.iterdir())
    assert len(remaining) == 1
    assert remaining[0].is_dir()


def test_main_reads_sys_argv_when_argv_is_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read `sys.argv[1:]` when `main()` is called without an explicit argv."""

    def fake_execute(cfg: cli.ResolvedConfig, **_kwargs: object) -> tuple[int, str]:
        del cfg
        return cli.EXIT_OK, "report\n"

    monkeypatch.setattr(cli, "execute", fake_execute)
    monkeypatch.setattr(
        sys,
        "argv",
        ["airflow-migration-diff", "--work-dir", str(tmp_path), "--uv-path", "/opt/uv"],
    )

    exit_code = cli.main()

    assert exit_code == cli.EXIT_OK
