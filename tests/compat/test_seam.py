"""Guard the `_compat` seam against new runtime Airflow imports outside it.

Any use of Airflow internals must go behind `_compat`, so a new Airflow release
lands in one package. This suite walks the shipped source with `ast`, allowing
`TYPE_CHECKING`-only imports everywhere and everything under `_compat/`, and
fails on any other `airflow` import -- an import statement anywhere, including
function bodies and `except` handlers, or a dynamic `import_module` /
`__import__` call whose target is a string literal. A dynamic import of a
computed module name is statically invisible and stays reachable only through
a `_compat`-resolved value, which is exactly where such names already live.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path

import pytest

import pytest_airflow_in_a_box

# Known leaks awaiting their own rerouting PR, keyed by path relative to the package
# root. The assertion below exact-matches this set, so rerouting a leak without
# pruning its entry fails loudly -- the allowlist only ever shrinks. Empty since the
# owned renderer replaced `airflow.cli.simple_table` (#213); the seam is sealed.
_KNOWN_LEAKS: frozenset[tuple[str, str]] = frozenset()


def _is_type_checking_guard(test: ast.expr) -> bool:
    """Report whether an `if` test is the `TYPE_CHECKING` import guard.

    Parameters:
        test: ast.expr containing the `if` statement's test expression.

    Returns:
        bool marking the test as `TYPE_CHECKING` or `typing.TYPE_CHECKING`.
    """

    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


# Callable names treated as dynamic imports when passed a string-literal module
# path -- `importlib.import_module` (however it is bound) and the `__import__`
# builtin.
_DYNAMIC_IMPORTERS = frozenset({"import_module", "__import__"})


def _is_airflow_module(module: str) -> bool:
    """Report whether a module path names Airflow or one of its submodules.

    Parameters:
        module: str containing a dotted module path.

    Returns:
        bool marking the path as `airflow` or an `airflow.` submodule.
    """

    return module == "airflow" or module.startswith("airflow.")


def _dynamic_import_target(node: ast.Call) -> str | None:
    """Extract the Airflow module path a dynamic import call names, if any.

    Parameters:
        node: ast.Call containing the call expression to inspect.

    Returns:
        str | None containing the string-literal `airflow` module path passed to
        an `import_module` / `__import__` call, or None for every other call --
        including dynamic imports of computed, statically invisible names.
    """

    func = node.func
    if isinstance(func, ast.Attribute):
        name = func.attr
    elif isinstance(func, ast.Name):
        name = func.id
    else:
        return None
    if name not in _DYNAMIC_IMPORTERS or not node.args:
        return None
    first = node.args[0]
    if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
        return None
    return first.value if _is_airflow_module(first.value) else None


def _collect_runtime_airflow_imports(nodes: Iterable[ast.AST], found: set[str]) -> None:
    """Accumulate `airflow` modules imported outside `TYPE_CHECKING` blocks.

    A `TYPE_CHECKING`-guarded `if` contributes only its `else` branch; every other
    node is walked completely -- including function bodies and `except` handlers --
    so imports deferred into them count as the runtime imports they are, and a
    dynamic `import_module` / `__import__` call naming a string-literal `airflow`
    module counts exactly like the import statement it replaces.

    Parameters:
        nodes: Iterable[ast.AST] containing the nodes to walk.
        found: set[str] receiving every runtime-imported `airflow` module path.
    """

    for node in nodes:
        if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
            _collect_runtime_airflow_imports(node.orelse, found)
            continue
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names if _is_airflow_module(alias.name))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level == 0 and _is_airflow_module(module):
                found.add(module)
        elif isinstance(node, ast.Call):
            target = _dynamic_import_target(node)
            if target is not None:
                found.add(target)
        _collect_runtime_airflow_imports(ast.iter_child_nodes(node), found)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        pytest.param("import airflow.models", {"airflow.models"}, id="plain-import"),
        pytest.param(
            "from airflow.models.pool import Pool", {"airflow.models.pool"}, id="from-import"
        ),
        pytest.param(
            "def f():\n    from airflow import settings",
            {"airflow"},
            id="function-deferred-import",
        ),
        pytest.param(
            "try:\n    x = 1\nexcept ImportError:\n    import airflow.exceptions",
            {"airflow.exceptions"},
            id="except-handler-import",
        ),
        pytest.param(
            "import importlib\nimportlib.import_module('airflow.models.pool')",
            {"airflow.models.pool"},
            id="dynamic-import-module-call",
        ),
        pytest.param(
            "__import__('airflow.settings')", {"airflow.settings"}, id="dunder-import-call"
        ),
        pytest.param(
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from airflow.sdk import DAG",
            set(),
            id="type-checking-only",
        ),
        pytest.param(
            "import typing\nif typing.TYPE_CHECKING:\n    import airflow",
            set(),
            id="qualified-type-checking-only",
        ),
        pytest.param(
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    from airflow.sdk import DAG\n"
            "else:\n"
            "    from airflow.models.dag import DAG",
            {"airflow.models.dag"},
            id="type-checking-else-branch",
        ),
        pytest.param(
            "import importlib\nimportlib.import_module(module_name)",
            set(),
            id="computed-dynamic-import-is-invisible",
        ),
        pytest.param(
            "from airflowlib import x\nimport_module('airflowlib.y')",
            set(),
            id="airflow-prefixed-foreign-package",
        ),
    ],
)
def test_collector_catches_every_runtime_import_form(source: str, expected: set[str]) -> None:
    """Pin the walker against each import form the guard must (or cannot) see.

    Parameters:
        source: str containing a synthetic module exercising one import form.
        expected: set[str] containing the `airflow` modules the walker must report.
    """

    found: set[str] = set()
    _collect_runtime_airflow_imports(ast.parse(source).body, found)

    assert found == expected


def test_no_runtime_airflow_imports_outside_compat() -> None:
    """Confine every runtime `airflow` import statement to `_compat/`.

    The observed leak set must match `_KNOWN_LEAKS` exactly: a new leak fails the
    build until it is rerouted through `_compat`, and a rerouted leak fails it until
    its allowlist entry is pruned.
    """

    package_root = Path(pytest_airflow_in_a_box.__file__).resolve().parent
    observed: set[tuple[str, str]] = set()
    for path in sorted(package_root.rglob("*.py")):
        relative = path.relative_to(package_root)
        if relative.parts[0] == "_compat":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        modules: set[str] = set()
        _collect_runtime_airflow_imports(tree.body, modules)
        observed.update((relative.as_posix(), module) for module in modules)

    assert observed == _KNOWN_LEAKS
