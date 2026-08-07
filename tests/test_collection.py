"""Test opt-in Dag-file collection and its double-collection guard."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import collection

VALID_DAG = """
from airflow.sdk import DAG

collected = DAG(dag_id="collected")
"""

BROKEN_DAG = """
raise RuntimeError("broken test Dag")
"""

TEST_NAMED_DAG = """
from airflow.sdk import DAG

collided = DAG(dag_id="collided")


def test_never_run():
    raise AssertionError("the default Python collector must not collect this")
"""


def _config(
    *,
    collect_dag_folder: object = None,
    ini_folder: object = "",
    rootpath: Path | None = None,
) -> Any:
    """Create a minimal configuration double for collection tests.

    Parameters:
        collect_dag_folder: object containing the parsed CLI option value.
        ini_folder: object containing the ``airflow_collect_dags_folder`` ini value.
        rootpath: pathlib.Path | None used to resolve relative folders.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {"airflow_collect_dags_folder": ini_folder}
    return SimpleNamespace(
        getoption=lambda name: {"collect_dag_folder": collect_dag_folder}[name],
        getini=lambda name: ini_values[name],
        rootpath=rootpath or Path.cwd(),
        stash=pytest.Stash(),
    )


def _write_dags(pytester: pytest.Pytester, **files: str) -> Path:
    """Create a ``dags`` directory containing the provided Dag modules.

    Parameters:
        pytester: pytest.Pytester owning the temporary project directory.
        files: str module contents keyed by module name without suffix.

    Returns:
        pathlib.Path containing the new Dag directory.
    """

    folder = pytester.path / "dags"
    folder.mkdir()
    for name, contents in files.items():
        (folder / f"{name}.py").write_text(contents, encoding="utf-8")
    return folder


def test_collection_folder_defaults_to_disabled(tmp_path: Path) -> None:
    """Return no folder and cache the answer when neither source is set."""

    config = _config(rootpath=tmp_path)

    assert collection.collection_folder(config) is None
    assert config.stash[collection._FOLDER_KEY] is None
    assert collection.collection_folder(config) is None


def test_collection_folder_resolves_cli_option_relative_to_rootpath(tmp_path: Path) -> None:
    """Resolve a relative CLI folder against the pytest root path."""

    (tmp_path / "dags").mkdir()
    config = _config(collect_dag_folder="dags", rootpath=tmp_path)

    assert collection.collection_folder(config) == (tmp_path / "dags").resolve()


def test_collection_folder_prefers_cli_over_ini(tmp_path: Path) -> None:
    """Give the CLI option precedence over the ini value."""

    cli_folder = tmp_path / "cli-dags"
    ini_folder = tmp_path / "ini-dags"
    cli_folder.mkdir()
    ini_folder.mkdir()
    config = _config(collect_dag_folder=str(cli_folder), ini_folder=str(ini_folder))

    assert collection.collection_folder(config) == cli_folder.resolve()


def test_collection_folder_reads_ini_value(tmp_path: Path) -> None:
    """Fall back to the ini value when the CLI option is absent."""

    ini_folder = tmp_path / "ini-dags"
    ini_folder.mkdir()
    config = _config(ini_folder=str(ini_folder))

    assert collection.collection_folder(config) == ini_folder.resolve()


def test_collection_folder_caches_resolution(tmp_path: Path) -> None:
    """Resolve the folder once and serve later calls from the stash."""

    folder = tmp_path / "dags"
    folder.mkdir()
    reads: list[str] = []
    config = _config(collect_dag_folder=str(folder))
    original_getoption = config.getoption
    config.getoption = lambda name: (reads.append(name), original_getoption(name))[1]

    first = collection.collection_folder(config)
    second = collection.collection_folder(config)

    assert first == second == folder.resolve()
    assert reads == ["collect_dag_folder"]


@pytest.mark.parametrize(
    ("config", "match"),
    [
        (_config(collect_dag_folder=7), "`--collect-dag-folder` must be a path string"),
        (_config(ini_folder=7), "`airflow_collect_dags_folder` must be a path string"),
    ],
)
def test_collection_folder_rejects_non_string_values(config: Any, match: str) -> None:
    """Reject non-string option and ini values."""

    with pytest.raises(pytest.UsageError, match=match):
        collection.collection_folder(config)


def test_collection_folder_rejects_missing_directory(tmp_path: Path) -> None:
    """Reject a configured folder that does not name an existing directory."""

    config = _config(collect_dag_folder=str(tmp_path / "missing"))

    with pytest.raises(pytest.UsageError, match="is not a directory"):
        collection.collection_folder(config)


def test_dag_file_import_error_requires_errors() -> None:
    """Reject construction without at least one failing Dag file."""

    with pytest.raises(ValueError, match="at least one failing Dag file"):
        collection.DagFileImportError({})


def test_dag_file_import_error_copies_errors() -> None:
    """Snapshot the error mapping so later caller mutation is invisible."""

    errors = {"dags/a.py": "boom"}
    error = collection.DagFileImportError(errors)
    errors["dags/b.py"] = "later"

    assert error.errors == {"dags/a.py": "boom"}
    assert str(error) == "Dag import check failed for 1 file(s)"


def test_format_import_errors_sorts_sections() -> None:
    """Render one trimmed section per file in path order."""

    error = collection.DagFileImportError(
        {"dags/b.py": "second failure\n", "dags/a.py": "first failure"}
    )

    assert collection.format_import_errors(error) == (
        "Dag file import check failed: 'dags/a.py'\nfirst failure"
        "\n\n"
        "Dag file import check failed: 'dags/b.py'\nsecond failure"
    )


def test_prune_is_noop_when_collection_is_disabled(tmp_path: Path) -> None:
    """Leave items alone when Dag-file collection is not enabled."""

    config = _config(rootpath=tmp_path)
    foreign: Any = SimpleNamespace(path=tmp_path / "dags" / "test_x.py")
    items = [foreign]

    collection.prune_duplicate_items(config, items)

    assert items == [foreign]


def test_prune_drops_only_foreign_items_below_the_folder(tmp_path: Path) -> None:
    """Remove default-collector duplicates and keep everything else."""

    folder = tmp_path / "dags"
    (folder / "nested").mkdir(parents=True)
    config = _config(collect_dag_folder=str(folder))
    # __new__ bypasses pytest's node constructor, which requires a live session;
    # only the `path` attribute and the type itself participate in pruning.
    dag_item = collection.DagImportItem.__new__(collection.DagImportItem)
    dag_item.path = folder / "test_dag.py"
    duplicate: Any = SimpleNamespace(path=folder / "test_dag.py")
    nested_duplicate: Any = SimpleNamespace(path=folder / "nested" / "test_deep.py")
    outsider: Any = SimpleNamespace(path=tmp_path / "test_regular.py")
    items: list[Any] = [dag_item, duplicate, nested_duplicate, outsider]

    collection.prune_duplicate_items(config, items)

    assert items == [dag_item, outsider]


def test_collects_valid_dag_file_as_import_item(pytester: pytest.Pytester) -> None:
    """Collect one passing ``dag-import`` item per valid Dag file."""

    _write_dags(pytester, my_dag=VALID_DAG)

    result = pytester.runpytest_subprocess("-v", "--collect-dag-folder=dags")

    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*dags/my_dag.py::dag-import*PASSED*"])


def test_reports_import_errors_with_traceback_text(pytester: pytest.Pytester) -> None:
    """Fail the import item and surface the Dag file's own error."""

    _write_dags(pytester, bad_dag=BROKEN_DAG)

    result = pytester.runpytest_subprocess("-q", "--collect-dag-folder=dags")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*Dag file import check failed*", "*broken test Dag*"])


def test_reports_dag_file_without_dags(pytester: pytest.Pytester) -> None:
    """Fail a Dag file that imports cleanly but defines no Dags."""

    _write_dags(pytester, empty_dag="VALUE = 1\n")

    result = pytester.runpytest_subprocess("-q", "--collect-dag-folder=dags")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*defines no Dags*"])


def test_test_named_dag_file_is_collected_exactly_once(pytester: pytest.Pytester) -> None:
    """Prune the default Python collector's duplicates of a ``test_``-named Dag file."""

    folder = _write_dags(pytester, test_smoke=TEST_NAMED_DAG)

    scanned = pytester.runpytest_subprocess("-q", "--collect-dag-folder=dags")
    scanned.assert_outcomes(passed=1)

    direct = pytester.runpytest_subprocess(
        "-q", "--collect-dag-folder=dags", str(folder / "test_smoke.py")
    )
    direct.assert_outcomes(passed=1)


def test_db_test_marker_selects_dag_items(pytester: pytest.Pytester) -> None:
    """Auto-mark import items so ``-m`` selection works like static tests."""

    _write_dags(pytester, my_dag=VALID_DAG)

    selected = pytester.runpytest_subprocess("-q", "--collect-dag-folder=dags", "-m", "db_test")
    selected.assert_outcomes(passed=1)

    deselected = pytester.runpytest_subprocess(
        "-q", "--collect-dag-folder=dags", "-m", "not db_test"
    )
    deselected.assert_outcomes(deselected=1)


def test_disabled_collection_ignores_dag_folder(pytester: pytest.Pytester) -> None:
    """Leave the Dag directory untouched when the feature is not enabled."""

    _write_dags(pytester, my_dag=VALID_DAG)
    pytester.makepyfile(
        """
        def test_regular():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_ini_configured_folder_enables_collection(pytester: pytest.Pytester) -> None:
    """Enable Dag-file collection through the ini option alone."""

    _write_dags(pytester, my_dag=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_collect_dags_folder = dags\n")

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_missing_folder_fails_collection_loudly(pytester: pytest.Pytester) -> None:
    """Abort the run when the configured Dag folder does not exist."""

    result = pytester.runpytest_subprocess("-q", "--collect-dag-folder=missing-dags")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*Dag collection folder is not a directory*"])


def test_foreign_runtest_errors_keep_standard_reporting(pytester: pytest.Pytester) -> None:
    """Delegate non-import failures to pytest's standard failure representation."""

    _write_dags(pytester, my_dag=VALID_DAG)
    pytester.makeconftest(
        """
        import pytest_airflow_in_a_box.collection as collection


        def _explode(path):
            raise RuntimeError("unexpected parser failure")


        collection.build_dag_bag = _explode
        """
    )

    result = pytester.runpytest_subprocess("-q", "--collect-dag-folder=dags")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*RuntimeError*unexpected parser failure*"])
