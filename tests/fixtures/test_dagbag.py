"""Test the public full Dag bag fixture and its path selection.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag
from pytest_airflow_in_a_box.fixtures import dagbag as fixtures_dagbag

if TYPE_CHECKING:
    from pytest_airflow_in_a_box._compat.dagbag import DagBag


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


def test_defaults_to_bootstrap_dag_folder(full_dag_bag: DagBag) -> None:
    """Discover the DagBag fixture and parse the empty bootstrap Dag folder."""

    assert full_dag_bag.dags == {}
    assert full_dag_bag.import_errors == {}


def test_full_dag_bag_parses_valid_and_invalid_files(
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
        def test_dags(full_dag_bag):
            assert set(full_dag_bag.dags) == {
                "single",
                "multiple_one",
                "multiple_two",
            }
            assert len(full_dag_bag.import_errors) == 1
            assert next(iter(full_dag_bag.import_errors)).endswith("invalid.py")
            assert not any(dag_id.startswith("example_") for dag_id in full_dag_bag.dags)
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
        def test_dags(full_dag_bag):
            assert set(full_dag_bag.dags) == {"from_ini"}
            assert full_dag_bag.import_errors == {}
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
        "def test_dags(full_dag_bag):\n"
        '    assert set(full_dag_bag.dags) == {"from_ini"}\n'
        "    assert full_dag_bag.import_errors == {}\n",
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
        def test_dags(full_dag_bag):
            assert set(full_dag_bag.dags) == {"from_cli"}
            assert full_dag_bag.import_errors == {}
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
    *, tmp_path: Path, airflow_smoke: object = False, parse_timeout: str = "30"
) -> Any:
    """Create a minimal configuration double for `_cached_dag_bag` tests.

    Parameters:
        tmp_path: pathlib.Path used as both the rootpath and the bootstrap Dag folder.
        airflow_smoke: object containing the ``airflow_smoke`` ini value.
        parse_timeout: str containing the ``airflow_dag_parse_timeout`` ini value.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    option_values = {"dag_folder": None, "airflow_smoke": None}
    ini_values = {
        "airflow_dags_folder": "",
        "airflow_smoke": airflow_smoke,
        "airflow_dag_parse_timeout": parse_timeout,
    }
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
        rootpath=tmp_path,
        stash=pytest.Stash(),
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
        lambda folder: (builds.append(folder), sentinel)[1],
    )
    session: Any = SimpleNamespace(stash=pytest.Stash())

    first = fixtures_dagbag._cached_dag_bag(session, config)
    second = fixtures_dagbag._cached_dag_bag(session, config)

    assert first is sentinel
    assert second is sentinel
    assert builds == [tmp_path]


def test_cached_dag_bag_ignores_parse_timeout_when_smoke_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Leave the Dag import timeout untouched when the smoke catalog is off."""

    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(tmp_path=tmp_path, airflow_smoke=False)
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "unset")
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(fixtures_dagbag, "build_dag_bag", lambda _folder: sentinel)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    result = fixtures_dagbag._cached_dag_bag(session, config)

    assert result is sentinel
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "unset"


def test_cached_dag_bag_applies_parse_timeout_when_smoke_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Apply the smoke catalog's per-file parse timeout before an opportunistic parse.

    Regression test for issue #85: `full_dag_bag` may be the one to parse the Dag folder
    while the smoke catalog is enabled, and the smoke corpus builder can later reuse that
    DagBag -- the configured timeout must still be honored regardless of which one parses.
    """

    sentinel: Any = SimpleNamespace(dags={}, import_errors={})
    config = _fake_config(tmp_path=tmp_path, airflow_smoke=True, parse_timeout="5")
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "unset")
    monkeypatch.setattr(
        fixtures_dagbag,
        "get_bootstrap_state",
        lambda _config: SimpleNamespace(root=tmp_path, dags_folder=tmp_path),
    )
    monkeypatch.setattr(fixtures_dagbag, "build_dag_bag", lambda _folder: sentinel)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    result = fixtures_dagbag._cached_dag_bag(session, config)

    assert result is sentinel
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "5.0"


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
