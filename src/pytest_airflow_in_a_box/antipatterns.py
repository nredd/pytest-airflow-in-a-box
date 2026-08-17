"""Static import-time anti-pattern scanning over Dag source files.

Backs the smoke catalog's top-level secrets-lookup and top-level I/O checks. Everything here is
pure ``ast`` analysis -- no Airflow import, no Dag execution -- so findings carry exact file and
line locations and the scan is safe on any parseable Python source. "Import time" means every
expression Python evaluates while the module loads: the module body, ``if``/``for``/``while``/
``try``/``with`` blocks (including a ``with DAG(...):`` block), class bodies, decorator
expressions, and default argument values. Function and lambda bodies are excluded -- they run at
task time, where these calls are fine.

References:
    https://docs.python.org/3/library/ast.html
    https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code
    https://github.com/apache/airflow/issues/42205
    https://github.com/apache/airflow/issues/43176
"""

from __future__ import annotations

import ast
import logging
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger(__name__)

# Modules whose top-level calls the I/O check flags by default. A consumer-provided
# `airflow_top_level_io_modules` list replaces this list rather than extending it.
DEFAULT_TOP_LEVEL_IO_MODULES: tuple[str, ...] = (
    "MySQLdb",
    "boto3",
    "botocore",
    "ftplib",
    "google.cloud",
    "http.client",
    "httpx",
    "psycopg2",
    "pymongo",
    "pymysql",
    "redis",
    "requests",
    "smtplib",
    "snowflake.connector",
    "socket",
    # Deliberately the engine entry points, not the whole package: `text`, `Column`,
    # `Table`, and `select` do no I/O and are normal at module scope in Dag repos.
    "sqlalchemy.create_engine",
    "sqlalchemy.engine.create_engine",
    "urllib.request",
    "urllib3",
)
# Methods on a literal `Variable` base that hit the secrets backends.
VARIABLE_LOOKUP_METHODS = frozenset({"get", "get_variable_from_secrets"})
# Methods on a literal `Connection` base that hit the secrets backends.
CONNECTION_LOOKUP_METHODS = frozenset({"get", "get_connection_from_secrets"})
# Method flagged on any base whose name ends in `Hook`.
HOOK_CONNECTION_METHOD = "get_connection"
HOOK_BASE_SUFFIX = "Hook"


@dataclass(frozen=True)
class AntipatternFinding:
    """One import-time anti-pattern call found in a Dag source file.

    Parameters:
        line: int containing the 1-indexed source line of the call.
        snippet: str containing the offending call expression's source text.
    """

    line: int
    snippet: str


def parse_dag_module(path: Path) -> tuple[ast.Module, str] | None:
    """Leniently parse one Dag file for anti-pattern scanning.

    Unreadable or unparsable files return ``None`` rather than raising: a broken Dag file
    already fails loudly in ``test_dag_bag_integrity``, so the scanners stay quiet about it.

    Parameters:
        path: pathlib.Path naming the Dag source file.

    Returns:
        tuple[ast.Module, str] | None containing the parsed module and its source text, or
        ``None`` when the file cannot be read or parsed.
    """

    try:
        source = path.read_text(encoding="utf-8")
        return ast.parse(source), source
    except (OSError, SyntaxError, ValueError) as error:
        LOGGER.debug(f"Skipping anti-pattern scan for '{path}': {error}")
        return None


class _ImportTimeVisitor(ast.NodeVisitor):
    """Collect the calls and import bindings that take effect while a module loads.

    Function and lambda bodies are never entered, but their decorator expressions and default
    argument values are -- Python evaluates those at definition time, which is import time.
    """

    def __init__(self) -> None:
        """Initialize empty call and import-binding accumulators."""

        self.calls: list[ast.Call] = []
        self.bindings: dict[str, str] = {}

    def visit_Call(self, node: ast.Call) -> None:
        """Record one import-time call and keep walking its arguments.

        Parameters:
            node: ast.Call evaluated while the module loads.
        """

        self.calls.append(node)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Record the module-level names one ``import`` statement binds.

        Parameters:
            node: ast.Import evaluated while the module loads.
        """

        for alias in node.names:
            if alias.asname:
                self.bindings[alias.asname] = alias.name
            else:
                top = alias.name.partition(".")[0]
                self.bindings[top] = top

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record the module-level names one ``from ... import`` statement binds.

        Relative and star imports are skipped: neither yields a binding this scanner can
        resolve to an absolute module path.

        Parameters:
            node: ast.ImportFrom evaluated while the module loads.
        """

        if node.level or not node.module:
            return
        for alias in node.names:
            if alias.name == "*":
                continue
            self.bindings[alias.asname or alias.name] = f"{node.module}.{alias.name}"

    def _visit_definition_time_parts(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda
    ) -> None:
        """Walk only the parts of a callable definition that execute at import time.

        Parameters:
            node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda whose body is deferred
                to call time.
        """

        for decorator in getattr(node, "decorator_list", ()):
            self.visit(decorator)
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Skip a function body but walk its decorators and defaults.

        Parameters:
            node: ast.FunctionDef whose body runs at call time, not import time.
        """

        self._visit_definition_time_parts(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Skip an async function body but walk its decorators and defaults.

        Parameters:
            node: ast.AsyncFunctionDef whose body runs at call time, not import time.
        """

        self._visit_definition_time_parts(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Skip a lambda body but walk its default argument values.

        Parameters:
            node: ast.Lambda whose body runs at call time, not import time.
        """

        self._visit_definition_time_parts(node)


def _import_time_visit(module: ast.Module) -> _ImportTimeVisitor:
    """Walk one parsed module and return its import-time calls and bindings.

    Parameters:
        module: ast.Module parsed from a Dag source file.

    Returns:
        _ImportTimeVisitor containing every import-time call and import binding.
    """

    visitor = _ImportTimeVisitor()
    visitor.visit(module)
    return visitor


def _call_snippet(call: ast.Call, source: str) -> str:
    """Render one call's source text for a failure message.

    Parameters:
        call: ast.Call to render.
        source: str containing the full module source.

    Returns:
        str containing the call's original source segment, or its unparsed form when the
        node lacks the position attributes a segment lookup needs.
    """

    return ast.get_source_segment(source, call) or ast.unparse(call)


def _is_secrets_lookup(call: ast.Call) -> bool:
    """Report whether one call is a Variable or Connection secrets lookup.

    Matching is name-based on the call's own attribute chain, so direct calls are caught with
    zero false positives from unrelated ``.get()`` methods; lookups hidden behind helper
    functions are left to the runtime interception pass.

    Parameters:
        call: ast.Call evaluated at import time.

    Returns:
        bool indicating whether the call reaches the secrets backends.
    """

    func = call.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr == "get_connection_from_secrets":
        return True
    if not isinstance(func.value, ast.Name):
        return False
    if func.value.id == "Variable" and func.attr in VARIABLE_LOOKUP_METHODS:
        return True
    if func.value.id == "Connection" and func.attr in CONNECTION_LOOKUP_METHODS:
        return True
    return func.attr == HOOK_CONNECTION_METHOD and func.value.id.endswith(HOOK_BASE_SUFFIX)


def find_secrets_lookups(module: ast.Module, source: str) -> list[AntipatternFinding]:
    """Find every import-time Variable or Connection lookup in one parsed Dag file.

    Parameters:
        module: ast.Module parsed from a Dag source file.
        source: str containing the full module source.

    Returns:
        list[AntipatternFinding] containing one finding per import-time secrets lookup, in
        source order.
    """

    return [
        AntipatternFinding(line=call.lineno, snippet=_call_snippet(call, source))
        for call in _import_time_visit(module).calls
        if _is_secrets_lookup(call)
    ]


def _dotted_call_name(call: ast.Call) -> str | None:
    """Flatten one call's attribute chain into a dotted name.

    Parameters:
        call: ast.Call whose callee to flatten.

    Returns:
        str | None containing the dotted callee name (``requests.get``), or ``None`` when the
        chain does not bottom out at a plain name.
    """

    parts: list[str] = []
    node: ast.expr = call.func
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def _module_matches(resolved: str, io_modules: tuple[str, ...]) -> bool:
    """Report whether a resolved dotted name falls under any configured I/O module.

    Parameters:
        resolved: str containing the import-resolved dotted callee name.
        io_modules: tuple[str, ...] containing configured module prefixes.

    Returns:
        bool indicating whether any configured module owns the callee.
    """

    return any(resolved == module or resolved.startswith(f"{module}.") for module in io_modules)


def find_io_calls(
    module: ast.Module, source: str, io_modules: tuple[str, ...]
) -> list[AntipatternFinding]:
    """Find every import-time call into a configured I/O module in one parsed Dag file.

    A call is flagged only when its callee's root name provably resolves, through the file's
    own top-level imports, to one of the configured modules -- one resolution hop, so aliased
    indirection escapes by design. That is the deliberate trade for a near-zero false-positive
    rate.

    Parameters:
        module: ast.Module parsed from a Dag source file.
        source: str containing the full module source.
        io_modules: tuple[str, ...] containing the module prefixes to flag.

    Returns:
        list[AntipatternFinding] containing one finding per import-time I/O call, in source
        order.
    """

    visitor = _import_time_visit(module)
    findings: list[AntipatternFinding] = []
    for call in visitor.calls:
        dotted = _dotted_call_name(call)
        if dotted is None:
            continue
        root, _, rest = dotted.partition(".")
        base = visitor.bindings.get(root)
        if base is None:
            continue
        resolved = f"{base}.{rest}" if rest else base
        if _module_matches(resolved, io_modules):
            findings.append(
                AntipatternFinding(line=call.lineno, snippet=_call_snippet(call, source))
            )
    return findings


__all__ = (
    "DEFAULT_TOP_LEVEL_IO_MODULES",
    "AntipatternFinding",
    "find_io_calls",
    "find_secrets_lookups",
    "parse_dag_module",
)
