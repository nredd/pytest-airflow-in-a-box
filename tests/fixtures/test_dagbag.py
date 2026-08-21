"""Test the public Dag bag fixture and its path selection.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag
from pytest_airflow_in_a_box._compat.introspection import SecretsLookup
from pytest_airflow_in_a_box.fixtures import dagbag as fixtures_dagbag

if TYPE_CHECKING:
    from pytest_airflow_in_a_box._compat.dagbag import DagBag


def _fake_recorder(lookups: list[SecretsLookup]) -> Any:
    """Create a `record_secrets_lookups` double yielding the given lookups.

    Parameters:
        lookups: list[SecretsLookup] the double reports as recorded.

    Returns:
        Any shaped like the `record_secrets_lookups` context manager factory.
    """

    @contextlib.contextmanager
    def recorder(_folder: Path) -> Any:
        """Yield the canned lookup list without patching anything.

        Parameters:
            _folder: pathlib.Path containing the unused Dag folder.

        Returns:
            Any yielding the canned lookup list.
        """

        yield lookups

    return recorder


def _write_dag_file(path: Path, *dag_ids: str) -> None:
    """Write one importable Airflow module containing the requested Dags.

    Parameters:
        path: pathlib.Path receiving the Python module.
        dag_ids: str identifiers assigned to Dags in the module.
    """

    definitions = "\n".join(
        f'dag_{index} = DAG(dag_id="{dag_id}")' for index, dag_id in enumerate(dag_ids)
    )
    path.write_text(f"from airflow.sdk import DAG\n\n{definitions}\n", encoding="utf-8")


def _make_dag_folder(path: Path, dag_id: str) -> None:
    """Create a Dag directory containing one valid Dag module.

    Parameters:
        path: pathlib.Path naming the new directory.
        dag_id: str identifying the Dag to create.
    """

    path.mkdir()
    _write_dag_file(path / "dag.py", dag_id)


def test_defaults_to_bootstrap_dag_folder(dag_bag: DagBag) -> None:
    """Discover the DagBag fixture and parse the empty bootstrap Dag folder."""

    assert dag_bag.dags == {}
    assert dag_bag.import_errors == {}


def test_dag_bag_parses_valid_and_invalid_files(
    pytester: pytest.Pytester,
) -> None:
    """Collect every valid Dag, retain import errors, and exclude Airflow examples."""

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    _write_dag_file(dag_folder / "single.py", "single")
    _write_dag_file(dag_folder / "multiple.py", "multiple_one", "multiple_two")
    (dag_folder / "invalid.py").write_text(
        'raise RuntimeError("invalid test Dag")\n', encoding="utf-8"
    )
    pytester.makepyfile(
        """
        def test_dags(dag_bag):
            assert set(dag_bag.dags) == {
                "single",
                "multiple_one",
                "multiple_two",
            }
            assert len(dag_bag.import_errors) == 1
            assert next(iter(dag_bag.import_errors)).endswith("invalid.py")
            assert not any(dag_id.startswith("example_") for dag_id in dag_bag.dags)
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder", str(dag_folder))

    result.assert_outcomes(passed=1)


def test_ini_dag_folder_wins_over_bootstrap(pytester: pytest.Pytester) -> None:
    """Select the configured ini directory instead of bootstrap's empty directory."""

    ini_folder = pytester.path / "ini-dags"
    _make_dag_folder(ini_folder, "from_ini")
    pytester.makeini(f"[pytest]\nairflow_dags_folder = {ini_folder}\n")
    pytester.makepyfile(
        """
        def test_dags(dag_bag):
            assert set(dag_bag.dags) == {"from_ini"}
            assert dag_bag.import_errors == {}
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_ini_dag_folder_resolves_relative_to_rootpath(tmp_path: Path) -> None:
    """Resolve a relative ini Dag folder against `config.rootpath`, not cwd."""

    rootpath = tmp_path / "project"
    rootpath.mkdir()
    config: Any = SimpleNamespace(
        getoption=lambda _name: None,
        getini=lambda _name: "dags",
        rootpath=rootpath,
    )

    assert fixtures_dagbag._dag_folder(config) == rootpath / "dags"


def test_ini_dag_folder_relative_path_ignores_cwd(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Find a relative ini Dag folder when pytest is invoked from a subdirectory."""

    _make_dag_folder(pytester.path / "dags", "from_ini")
    pytester.makeini("[pytest]\nairflow_dags_folder = dags\n")
    subdir = pytester.path / "tests"
    subdir.mkdir()
    (subdir / "test_dags.py").write_text(
        "def test_dags(dag_bag):\n"
        '    assert set(dag_bag.dags) == {"from_ini"}\n'
        "    assert dag_bag.import_errors == {}\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(subdir)

    result = pytester.runpytest_subprocess("-c", "../tox.ini", "-q", "test_dags.py")

    result.assert_outcomes(passed=1)


def test_cli_dag_folder_wins_over_ini(pytester: pytest.Pytester) -> None:
    """Give the command-line Dag directory precedence over the ini setting."""

    ini_folder = pytester.path / "ini-dags"
    cli_folder = pytester.path / "cli-dags"
    _make_dag_folder(ini_folder, "from_ini")
    _make_dag_folder(cli_folder, "from_cli")
    pytester.makeini(f"[pytest]\nairflow_dags_folder = {ini_folder}\n")
    pytester.makepyfile(
        """
        def test_dags(dag_bag):
            assert set(dag_bag.dags) == {"from_cli"}
            assert dag_bag.import_errors == {}
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder", str(cli_folder))

    result.assert_outcomes(passed=1)


def test_build_dag_bag_rejects_missing_location(tmp_path: Path) -> None:
    """Resolve and name a missing Dag location before constructing a DagBag."""

    missing = tmp_path / "missing"

    with pytest.raises(FileNotFoundError) as caught:
        build_dag_bag(missing)

    assert str(caught.value) == f"Dag location does not exist: '{missing.resolve()}'"


def test_build_dag_bag_parses_single_file(pytester: pytest.Pytester) -> None:
    """Parse exactly one Dag file when the location is a file, not a directory."""

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    _write_dag_file(dag_folder / "target.py", "target")
    _write_dag_file(dag_folder / "sibling.py", "sibling")
    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box._compat import build_dag_bag

        def test_single_file():
            dag_bag = build_dag_bag("dags/target.py")
            assert set(dag_bag.dags) == {"target"}
            assert dag_bag.import_errors == {}
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def _fake_config(
    *,
    tmp_path: Path,
    airflow_smoke: object = False,
    parse_timeout: str = "30",
    args: tuple[str, ...] = (),
) -> Any:
    """Create a minimal configuration double for `_cached_dag_bag` tests.

    Parameters:
        tmp_path: pathlib.Path used as the rootpath, bootstrap Dag folder, and invocation
            directory.
        airflow_smoke: object containing the ``airflow_smoke`` ini value.
        parse_timeout: str containing the ``airflow_dag_parse_timeout`` ini value.
        args: str positionals; non-empty selects `ArgsSource.ARGS`, matching a run pointed
            at explicit files or node IDs, so `_smoke_in_scope` re-evaluates them.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    option_values = {"dag_folder": None, "airflow_smoke": None, "airflow_parse_secrets": None}
    ini_values = {
        "airflow_dags_folder": "",
        # Parse-time secrets resolution is exercised end to end in
        # `tests/enduser/test_parse_time_secrets.py`; these unit tests are about caching
        # and the parse timeout, so the shim stays out of their way.
        "airflow_parse_secrets": "off",
        "airflow_smoke": airflow_smoke,
        "airflow_dag_parse_timeout": parse_timeout,
    }
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
        rootpath=tmp_path,
        stash=pytest.Stash(),
        args_source=pytest.Config.ArgsSource.ARGS if args else pytest.Config.ArgsSource.TESTPATHS,
        invocation_params=SimpleNamespace(dir=tmp_path),
        args=args,
        option=SimpleNamespace(markexpr=""),
    )


def test_cached_dag_bag_builds_once_per_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Parse once per worker session and reuse the stashed DagBag on later calls."""

    builds: list[object] = []
    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(tmp_path=tmp_path)
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(
        fixtures_dagbag,
        "build_dag_bag",
        lambda folder, **_kwargs: (builds.append(folder), sentinel)[1],
    )
    session: Any = SimpleNamespace(stash=pytest.Stash())

    first = fixtures_dagbag._cached_dag_bag(session, config)
    second = fixtures_dagbag._cached_dag_bag(session, config)

    assert first is sentinel
    assert second is sentinel
    assert builds == [tmp_path]


def test_cached_dag_bag_never_touches_the_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Leave metadata-database initialization to `dag_bag` alone.

    Regression test for issue #85: the shared cache must not give DB-free smoke items a
    metadata-database dependency they never had before this fix. `_cached_dag_bag` parses
    Dags only; `ensure_database` stays the caller's responsibility.
    """

    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(tmp_path=tmp_path)

    def _fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("_cached_dag_bag must not call ensure_database")

    monkeypatch.setattr(fixtures_dagbag, "ensure_database", _fail_if_called)
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(fixtures_dagbag, "build_dag_bag", lambda _folder, **_kwargs: sentinel)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    result = fixtures_dagbag._cached_dag_bag(session, config)

    assert result is sentinel


def test_cached_dag_bag_ignores_parse_timeout_when_smoke_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Leave the Dag import timeout untouched when the smoke catalog is off."""

    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(tmp_path=tmp_path, airflow_smoke=False)
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(fixtures_dagbag, "build_dag_bag", lambda _folder, **_kwargs: sentinel)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    result = fixtures_dagbag._cached_dag_bag(session, config)

    assert result is sentinel
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "999"


def test_cached_dag_bag_applies_parse_timeout_when_smoke_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Apply the smoke catalog's per-file parse timeout before an opportunistic parse.

    Regression test for issue #85: `dag_bag` may be the one to parse the Dag folder
    while the smoke catalog is enabled, and the smoke corpus builder can later reuse that
    DagBag -- the configured timeout must still be honored regardless of which one parses.
    """

    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(tmp_path=tmp_path, airflow_smoke=True, parse_timeout="5")
    lookup = SecretsLookup(kind="variable", key="k", file=None, line=None)
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(fixtures_dagbag, "build_dag_bag", lambda _folder, **_kwargs: sentinel)
    monkeypatch.setattr(
        fixtures_dagbag, "record_secrets_lookups", _fake_recorder([lookup, lookup])
    )
    session: Any = SimpleNamespace(stash=pytest.Stash())

    result = fixtures_dagbag._cached_dag_bag(session, config)

    assert result is sentinel
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "5.0"
    assert session.stash[fixtures_dagbag.LIVE_DAG_BAG_LOOKUPS_KEY] == (lookup,)


def test_cached_dag_bag_ignores_parse_timeout_when_smoke_out_of_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Leave the Dag import timeout untouched when explicit selection drops the catalog.

    Mirrors `_smoke_in_scope`: pointing pytest at a node ID scopes the run to it and drops
    the bundled catalog from this session, so the catalog's parse timeout should not apply
    even though `airflow_smoke` itself is enabled.
    """

    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(
        tmp_path=tmp_path,
        airflow_smoke=True,
        parse_timeout="5",
        args=("tests/test_x.py::test_one",),
    )
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(fixtures_dagbag, "build_dag_bag", lambda _folder, **_kwargs: sentinel)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    result = fixtures_dagbag._cached_dag_bag(session, config)

    assert result is sentinel
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "999"


@pytest.mark.parametrize(
    ("option_value", "ini_value", "match"),
    [
        (7, "", "`--dag-folder` must be a path string"),
        (None, 7, "`airflow_dags_folder` must be a path string"),
    ],
)
def test_dag_folder_rejects_non_string_configuration(
    option_value: object,
    ini_value: object,
    match: str,
) -> None:
    """Reject non-string option and ini Dag folder values."""

    config: Any = SimpleNamespace(
        getoption=lambda _name: option_value,
        getini=lambda _name: ini_value,
    )

    with pytest.raises(pytest.UsageError, match=match):
        fixtures_dagbag._dag_folder(config)
