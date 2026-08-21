"""Guard the `_compat` seam against new runtime Airflow imports outside it.

Any use of Airflow internals must go behind `_compat`, so a new Airflow release
lands in one package. This suite walks the shipped source with `ast`, allowing
`TYPE_CHECKING`-only imports everywhere and everything under `_compat/`, and
fails on any other `airflow` import statement.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path

import pytest_airflow_in_a_box

# Known leaks awaiting their own rerouting PR, keyed by path relative to the package
# root. The assertion below exact-matches this set, so rerouting a leak without
# pruning its entry fails loudly -- the allowlist only ever shrinks.
# `airflow.cli.simple_table` is replaced by an owned renderer in issue #213's
# follow-up PR.
_KNOWN_LEAKS: frozenset[tuple[str, str]] = frozenset(
    {
        ("smoke.py", "airflow.cli.simple_table"),
    }
)


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


def _collect_runtime_airflow_imports(nodes: Iterable[ast.AST], found: set[str]) -> None:
    """Accumulate `airflow` modules imported outside `TYPE_CHECKING` blocks.

    A `TYPE_CHECKING`-guarded `if` contributes only its `else` branch; every other
    node is walked completely -- including function bodies and `except` handlers --
    so imports deferred into them count as the runtime imports they are.

    Parameters:
        nodes: Iterable[ast.AST] containing the nodes to walk.
        found: set[str] receiving every runtime-imported `airflow` module path.
    """

    for node in nodes:
        if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
            _collect_runtime_airflow_imports(node.orelse, found)
            continue
        if isinstance(node, ast.Import):
            found.update(
                alias.name
                for alias in node.names
                if alias.name == "airflow" or alias.name.startswith("airflow.")
            )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level == 0 and (module == "airflow" or module.startswith("airflow.")):
                found.add(module)
        _collect_runtime_airflow_imports(ast.iter_child_nodes(node), found)


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
