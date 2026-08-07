"""Collect Dag files as pytest items that verify importability.

A configured Dag directory is collected file-by-file into ``DagImportItem``
tests that parse each file with Airflow's Dag bag and fail on import errors.
Dag files whose names match pytest's ``python_files`` patterns (or that are
passed directly on the command line, which bypasses pattern checks) are also
collected by pytest's default Python collector; ``prune_duplicate_items``
drops those duplicates after collection.

References:
    https://docs.pytest.org/en/stable/example/nonpython.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collect_file
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag

if TYPE_CHECKING:
    from collections.abc import Iterator

    from _pytest._code.code import TerminalRepr

LOGGER = logging.getLogger(__name__)

IMPORT_ITEM_NAME = "dag-import"
_FOLDER_KEY = pytest.StashKey[Path | None]()


class DagFileImportError(Exception):
    """Report Dag files that fail the collection-driven import check.

    Parameters:
        errors: dict[str, str] mapping Dag file paths to failure descriptions.

    Raises:
        ValueError: The error mapping is empty.
    """

    def __init__(self, errors: dict[str, str]) -> None:
        if not errors:
            raise ValueError("`errors` must contain at least one failing Dag file")
        super().__init__(f"Dag import check failed for {len(errors)} file(s)")
        self.errors = dict(errors)


def collection_folder(config: pytest.Config) -> Path | None:
    """Resolve and validate the opt-in Dag collection directory once.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        pathlib.Path | None containing the absolute Dag collection directory, or
        ``None`` when Dag-file collection is not enabled.

    Raises:
        pytest.UsageError: The configured value has an invalid type or does not
            name an existing directory.
    """

    if _FOLDER_KEY in config.stash:
        return config.stash[_FOLDER_KEY]

    value: str | None = None
    option_value: object = config.getoption("collect_dag_folder")
    if option_value is not None:
        if not isinstance(option_value, str):
            raise pytest.UsageError("Option `--collect-dag-folder` must be a path string")
        value = option_value
    else:
        ini_value: object = config.getini("airflow_collect_dags_folder")
        if not isinstance(ini_value, str):
            raise pytest.UsageError(
                "Ini option `airflow_collect_dags_folder` must be a path string"
            )
        if ini_value:
            value = ini_value

    if value is None:
        config.stash[_FOLDER_KEY] = None
        return None
    folder = Path(value)
    if not folder.is_absolute():
        folder = config.rootpath / folder
    resolved = Path(str(folder)).resolve()
    if not resolved.is_dir():
        raise pytest.UsageError(f"Dag collection folder is not a directory: '{resolved}'")
    config.stash[_FOLDER_KEY] = resolved
    return resolved


class DagImportItem(pytest.Item):
    """One collected import check for a single Dag file."""

    def __init__(self, *, name: str, parent: DagFile) -> None:
        """Create the item and mark it as a metadata-database test.

        Parameters:
            name: str containing the pytest item name.
            parent: DagFile that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.db_test)

    def runtest(self) -> None:
        """Parse the Dag file and fail on import errors or an empty file.

        Raises:
            DagFileImportError: The file failed to import or defines no Dags.
        """

        dag_bag = build_dag_bag(self.path)
        errors = {str(path): str(message) for path, message in dag_bag.import_errors.items()}
        if errors:
            raise DagFileImportError(errors)
        if not dag_bag.dags:
            raise DagFileImportError({str(self.path): "Dag file defines no Dags"})

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: object = None,
    ) -> str | TerminalRepr:
        """Format Dag import failures without a pytest-internal traceback.

        Parameters:
            excinfo: pytest.ExceptionInfo[BaseException] describing the failure.
            style: object containing an unused traceback style override.

        Returns:
            str | TerminalRepr containing the failure representation.
        """

        del style
        if isinstance(excinfo.value, DagFileImportError):
            return format_import_errors(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self) -> tuple[Path, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[pathlib.Path, int, str] containing path, line, and title.
        """

        return self.path, 0, f"{IMPORT_ITEM_NAME}: {self.path.name}"


class DagFile(pytest.File):
    """One Dag file collected from the configured Dag directory."""

    def collect(self) -> Iterator[DagImportItem]:
        """Yield the import check for this Dag file.

        Returns:
            Iterator[DagImportItem] containing exactly one import check item.
        """

        yield DagImportItem.from_parent(self, name=IMPORT_ITEM_NAME)


def format_import_errors(error: DagFileImportError) -> str:
    """Render one failure section per failing Dag file.

    Parameters:
        error: DagFileImportError containing per-file failure descriptions.

    Returns:
        str containing the newline-joined failure sections.
    """

    return "\n\n".join(
        f"Dag file import check failed: '{path}'\n{message.rstrip()}"
        for path, message in sorted(error.errors.items())
    )


def collect_dag_file(file_path: Path, parent: pytest.Collector) -> DagFile | None:
    """Collect one Dag file below the configured Dag collection directory.

    Parameters:
        file_path: pathlib.Path visited by pytest's collection walk.
        parent: pytest.Collector owning the new file node.

    Returns:
        DagFile | None containing the collector for an eligible Dag file.
    """

    folder = collection_folder(parent.config)
    if folder is None:
        return None
    if file_path.suffix != ".py" or file_path.name.startswith("_"):
        return None
    if folder != file_path.parent and folder not in file_path.parents:
        return None
    return DagFile.from_parent(parent, path=file_path)


def _is_foreign_dag_item(item: pytest.Item, folder: Path) -> bool:
    """Identify a default-collector duplicate of a collected Dag file.

    Parameters:
        item: pytest.Item produced by any collector.
        folder: pathlib.Path containing the Dag collection directory.

    Returns:
        bool indicating that the item duplicates Dag-file collection.
    """

    if isinstance(item, DagImportItem):
        return False
    return folder == item.path.parent or folder in item.path.parents


def prune_duplicate_items(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Drop default-collector items that duplicate Dag-file collection.

    Dag files matching ``python_files`` patterns, or passed directly on the
    command line, are also collected by pytest's Python collector; those
    duplicates are removed in place.

    Parameters:
        config: pytest.Config containing plugin options and ini values.
        items: list[pytest.Item] mutated to exclude duplicate items.
    """

    folder = collection_folder(config)
    if folder is None:
        return
    duplicates = [item for item in items if _is_foreign_dag_item(item, folder)]
    if not duplicates:
        return
    for duplicate in duplicates:
        items.remove(duplicate)
    LOGGER.debug(
        f"Pruned {len(duplicates)} duplicate item(s) below Dag collection folder '{folder}'"
    )


__all__ = (
    "DagFile",
    "DagFileImportError",
    "DagImportItem",
    "collect_dag_file",
    "collection_folder",
    "format_import_errors",
    "prune_duplicate_items",
)
