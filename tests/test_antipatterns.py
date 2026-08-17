"""Test the static import-time anti-pattern scanners over inline Dag sources."""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

import pytest

from pytest_airflow_in_a_box import antipatterns


def _scan_secrets(source: str) -> list[int]:
    """Run the secrets scanner over one inline source and return finding lines.

    Parameters:
        source: str containing the Dag module source, dedented before parsing.

    Returns:
        list[int] containing the 1-indexed line of each finding.
    """

    text = textwrap.dedent(source)
    return [finding.line for finding in antipatterns.find_secrets_lookups(ast.parse(text), text)]


def _scan_io(source: str, modules: tuple[str, ...]) -> list[int]:
    """Run the I/O scanner over one inline source and return finding lines.

    Parameters:
        source: str containing the Dag module source, dedented before parsing.
        modules: tuple[str, ...] containing the module prefixes to flag.

    Returns:
        list[int] containing the 1-indexed line of each finding.
    """

    text = textwrap.dedent(source)
    return [finding.line for finding in antipatterns.find_io_calls(ast.parse(text), text, modules)]


@pytest.mark.parametrize(
    "source",
    [
        'Variable.get("k")',
        'Variable.get_variable_from_secrets("k")',
        'Connection.get("db")',
        'Connection.get_connection_from_secrets("db")',
        'SomeModel.get_connection_from_secrets("db")',
        'BaseHook.get_connection("db")',
        'PostgresHook.get_connection("db")',
    ],
)
def test_secrets_scanner_flags_each_lookup_form_at_module_level(source: str) -> None:
    """Flag every certified lookup spelling in the module body."""

    assert _scan_secrets(f"{source}\n") == [1]


@pytest.mark.parametrize(
    ("source", "line"),
    [
        ('with DAG("d") as dag:\n    V = Variable.get("k")\n', 2),
        ('if True:\n    V = Variable.get("k")\n', 2),
        ('for _ in range(1):\n    V = Variable.get("k")\n', 2),
        ('try:\n    V = Variable.get("k")\nexcept Exception:\n    pass\n', 2),
        ('class Config:\n    V = Variable.get("k")\n', 2),
        ('@dag(schedule=Variable.get("s"))\ndef pipeline():\n    pass\n', 1),
        ('def f(x=Variable.get("k")):\n    pass\n', 1),
        ('def f(*, a, b=Variable.get("k")):\n    pass\n', 1),
        ('g = lambda x=Variable.get("k"): x\n', 1),
        ('async def f(x=Variable.get("k")):\n    pass\n', 1),
        ('outer(Variable.get("k"))\n', 1),
    ],
)
def test_secrets_scanner_flags_import_time_execution_contexts(source: str, line: int) -> None:
    """Flag lookups inside every construct Python evaluates while the module loads."""

    assert _scan_secrets(source) == [line]


@pytest.mark.parametrize(
    "source",
    [
        'def f():\n    return Variable.get("k")\n',
        'async def f():\n    return Variable.get("k")\n',
        'g = lambda: Variable.get("k")\n',
        '@decorator\ndef f():\n    return Variable.get("k")\n',
        'cfg.get("k")\n',
        'helper.get_connection("db")\n',
        'Variable("k")\n',
        'factory().get("k")\n',
    ],
)
def test_secrets_scanner_ignores_deferred_and_unrelated_calls(source: str) -> None:
    """Ignore task-time bodies and unrelated `.get()`-shaped calls."""

    assert _scan_secrets(source) == []


@pytest.mark.parametrize(
    ("source", "line"),
    [
        ("import requests\nrequests.get('https://x')\n", 2),
        ("import boto3 as b\nb.client('s3')\n", 2),
        ("from sqlalchemy import create_engine\ncreate_engine('sqlite://')\n", 2),
        ("import urllib.request\nurllib.request.urlopen('https://x')\n", 2),
        ("import urllib.request as ur\nur.urlopen('https://x')\n", 2),
        ("from urllib.request import urlopen\nurlopen('https://x')\n", 2),
    ],
)
def test_io_scanner_flags_calls_resolving_to_configured_modules(source: str, line: int) -> None:
    """Flag calls whose root name resolves through the file's imports to a listed module."""

    assert _scan_io(source, antipatterns.DEFAULT_TOP_LEVEL_IO_MODULES) == [line]


def test_io_scanner_matches_an_exact_configured_callable() -> None:
    """Honor a configured entry naming one function rather than a whole module."""

    source = "from sqlalchemy import create_engine\ncreate_engine('sqlite://')\n"

    assert _scan_io(source, ("sqlalchemy.create_engine",)) == [2]


@pytest.mark.parametrize(
    "source",
    [
        "import json\njson.loads('{}')\n",
        "unimported.get('https://x')\n",
        "(a or b).fetch()\n",
        "import requests\ndef f():\n    return requests.get('https://x')\n",
        "from . import sibling\nsibling.fetch()\n",
        "from os import *\ngetcwd()\n",
        "import requests\nX = 1\n",
    ],
)
def test_io_scanner_ignores_unlisted_unresolved_and_deferred_calls(source: str) -> None:
    """Ignore unlisted modules, unresolvable callees, and task-time bodies."""

    assert _scan_io(source, ("requests",)) == []


def test_parse_dag_module_reads_a_valid_file(tmp_path: Path) -> None:
    """Return the parsed module and its source text for a readable file."""

    path = tmp_path / "dag.py"
    path.write_text("X = 1\n", encoding="utf-8")

    parsed = antipatterns.parse_dag_module(path)

    assert parsed is not None
    module, source = parsed
    assert isinstance(module, ast.Module)
    assert source == "X = 1\n"


@pytest.mark.parametrize(
    ("name", "payload"),
    [
        ("missing.py", None),
        ("mangled.py", b"def broken(:\n"),
        ("binary.py", b"\xff\xfe\x00\x01"),
    ],
)
def test_parse_dag_module_returns_none_for_unusable_files(
    tmp_path: Path, name: str, payload: bytes | None
) -> None:
    """Return ``None`` for missing, unparsable, and undecodable files."""

    path = tmp_path / name
    if payload is not None:
        path.write_bytes(payload)

    assert antipatterns.parse_dag_module(path) is None


def test_call_snippet_falls_back_to_unparse_without_positions() -> None:
    """Render a call lacking end-position attributes through `ast.unparse`."""

    call = ast.Call(func=ast.Name(id="Variable", ctx=ast.Load()), args=[], keywords=[])
    call.lineno = 1
    call.col_offset = 0

    assert antipatterns._call_snippet(call, "irrelevant") == "Variable()"
