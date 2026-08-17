"""Collect Dag files as pytest items that verify importability.

A configured Dag directory is collected file-by-file into ``DagImportItem``
tests that parse each file with Airflow's Dag bag and fail on import errors.
A Dag file may additionally declare pinned param cases through a module-level
literal, read by AST without importing the file at collection time::

    PYTEST_DAG_CASES = {"smoke": {"environment": "dev"}}

Each case collects as a sibling ``dag-params[...]`` item validating the
pinned values against every Dag the file defines. Dag files whose names match
pytest's ``python_files`` patterns (or that are passed directly on the
command line, which bypasses pattern checks) are also collected by pytest's
default Python collector; ``prune_duplicate_items`` drops those duplicates
after collection.

References:
    https://docs.pytest.org/en/stable/example/nonpython.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collect_file
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import ParamsCaseError, build_dag_bag, validate_dag_params
from pytest_airflow_in_a_box.parse_secrets import parse_time_comms

if TYPE_CHECKING:
    from collections.abc import Iterator

    from _pytest._code.code import TerminalRepr

LOGGER = logging.getLogger(__name__)

IMPORT_ITEM_NAME = "dag-import"
PARAMS_ITEM_PREFIX = "dag-params"
CASES_DECLARATION_NAME = "PYTEST_DAG_CASES"
_FOLDER_KEY = pytest.StashKey[Path | None]()


class DagCasesDeclarationError(ValueError):
    """Report a malformed ``PYTEST_DAG_CASES`` declaration."""


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

        dag_bag = build_dag_bag(self.path, comms=parse_time_comms(self.config))
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


def read_declared_cases(path: Path) -> dict[str, dict[str, Any]]:
    """Read pinned param cases from a Dag file without importing it.

    Parameters:
        path: pathlib.Path containing the Dag file.

    Returns:
        dict[str, dict[str, Any]] mapping case names to pinned param values;
        empty when the file declares no cases.

    Raises:
        DagCasesDeclarationError: The declaration is not a literal mapping of
            non-empty case names to param mappings.
    """

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        # Unreadable or unparsable files fail loudly in the import item.
        return {}
    declaration: ast.expr | None = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue
        if any(
            isinstance(target, ast.Name) and target.id == CASES_DECLARATION_NAME
            for target in targets
        ):
            declaration = value
    if declaration is None:
        return {}

    try:
        literal = ast.literal_eval(declaration)
    except ValueError as error:
        raise DagCasesDeclarationError(
            f"`{CASES_DECLARATION_NAME}` must be a literal mapping: {error}"
        ) from error
    if not isinstance(literal, dict):
        raise DagCasesDeclarationError(
            f"`{CASES_DECLARATION_NAME}` must map case names to param mappings"
        )
    cases: dict[str, dict[str, Any]] = {}
    for name, conf in literal.items():
        if not isinstance(name, str) or not name:
            raise DagCasesDeclarationError(
                f"`{CASES_DECLARATION_NAME}` case names must be non-empty strings: '{name}'"
            )
        if not isinstance(conf, dict) or any(not isinstance(key, str) for key in conf):
            raise DagCasesDeclarationError(
                f"`{CASES_DECLARATION_NAME}` case `{name}` must map param names to values"
            )
        cases[name] = dict(conf)
    return cases


class DagParamsCaseItem(pytest.Item):
    """One collected pinned-param validation case for a Dag file."""

    def __init__(self, *, name: str, parent: DagFile, case_conf: dict[str, Any]) -> None:
        """Create the item and mark it as a metadata-database test.

        Parameters:
            name: str containing the pytest item name.
            parent: DagFile that collected this item.
            case_conf: dict[str, Any] containing the pinned param values.
        """

        super().__init__(name=name, parent=parent)
        self.case_conf = dict(case_conf)
        self.add_marker(pytest.mark.db_test)

    def runtest(self) -> None:
        """Validate the pinned params against every Dag in the file.

        Raises:
            DagFileImportError: The file failed to import, defines no Dags, or
                a Dag's param schema rejects the pinned values.
        """

        dag_bag = build_dag_bag(self.path, comms=parse_time_comms(self.config))
        errors = {str(path): str(message) for path, message in dag_bag.import_errors.items()}
        if errors:
            raise DagFileImportError(errors)
        if not dag_bag.dags:
            raise DagFileImportError({str(self.path): "Dag file defines no Dags"})
        failures: dict[str, str] = {}
        for dag_id, dag in sorted(dag_bag.dags.items()):
            try:
                validate_dag_params(dag, self.case_conf)
            except ParamsCaseError as error:
                failures[f"{self.path} [{dag_id}]"] = str(error)
        if failures:
            raise DagFileImportError(failures)

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: object = None,
    ) -> str | TerminalRepr:
        """Format case failures without a pytest-internal traceback.

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

        return self.path, 0, f"{self.name}: {self.path.name}"


class DagFile(pytest.File):
    """One Dag file collected from the configured Dag directory."""

    def collect(self) -> Iterator[pytest.Item]:
        """Yield the import check plus one item per declared param case.

        Returns:
            Iterator[pytest.Item] containing the collected items.

        Raises:
            pytest.Collector.CollectError: The case declaration is malformed.
        """

        yield DagImportItem.from_parent(self, name=IMPORT_ITEM_NAME)
        try:
            cases = read_declared_cases(self.path)
        except DagCasesDeclarationError as error:
            raise self.CollectError(f"{self.path}: {error}") from error
        for case_name, case_conf in cases.items():
            yield DagParamsCaseItem.from_parent(
                self,
                name=f"{PARAMS_ITEM_PREFIX}[{case_name}]",
                case_conf=case_conf,
            )


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

    if isinstance(item, (DagImportItem, DagParamsCaseItem)):
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
    "DagCasesDeclarationError",
    "DagFile",
    "DagFileImportError",
    "DagImportItem",
    "DagParamsCaseItem",
    "collect_dag_file",
    "collection_folder",
    "format_import_errors",
    "prune_duplicate_items",
    "read_declared_cases",
)
