"""Test the public full Dag bag fixture and its path selection.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag

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


def test_build_dag_bag_rejects_missing_folder(tmp_path: Path) -> None:
    """Resolve and name a missing Dag directory before constructing a DagBag."""

    missing = tmp_path / "missing"

    with pytest.raises(FileNotFoundError) as caught:
        build_dag_bag(missing)

    assert str(caught.value) == f"Dag folder does not exist: '{missing.resolve()}'"


def test_build_dag_bag_rejects_file(tmp_path: Path) -> None:
    """Resolve and name a non-directory Dag path before constructing a DagBag."""

    dag_file = tmp_path / "dag.py"
    dag_file.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="Dag folder is not a directory") as caught:
        build_dag_bag(dag_file)

    assert str(caught.value) == f"Dag folder is not a directory: '{dag_file.resolve()}'"
