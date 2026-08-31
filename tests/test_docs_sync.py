"""Guard the hand-maintained mirrors of plugin surface that live in Markdown.

`README.md` and `docs/reference/` restate names that are defined in code: the exported
fixtures, the registered markers, and the Quickstart example. Every one of those is a copy,
and the vision audit found copies drift -- the fixture table had gone stale against
`fixtures.__all__` with nothing to catch it. These tests are the guard, so a rename in code
fails here rather than in a reader's editor.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/284
"""

from __future__ import annotations

import re
from pathlib import Path
from types import ModuleType
from urllib.parse import unquote, urlparse

import pytest
import yaml

from pytest_airflow_in_a_box import bootstrap, certification, migration_strict, smoke, taskinstance
from pytest_airflow_in_a_box import components as components_module
from pytest_airflow_in_a_box import fixtures as fixtures_module
from pytest_airflow_in_a_box._compat import components as compat_components
from pytest_airflow_in_a_box.components import ComponentKind
from pytest_airflow_in_a_box.markers import MARKER_DESCRIPTIONS

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"
ACTION_METADATA = REPO_ROOT / "action.yml"
DOCS_HOME = REPO_ROOT / "docs" / "index.md"
ACTION_GUIDE = REPO_ROOT / "docs" / "guide" / "ci" / "github-action.md"
FIXTURES_PAGE = REPO_ROOT / "docs" / "reference" / "fixtures.md"
MARKERS_PAGE = REPO_ROOT / "docs" / "reference" / "markers.md"
QUICKSTART_PAGE = REPO_ROOT / "docs" / "quickstart.md"
COMPONENTS_PAGE = REPO_ROOT / "docs" / "guide" / "custom-components.md"

PUBLIC_DIAGNOSTIC_MODULES: tuple[ModuleType, ...] = (
    bootstrap,
    certification,
    components_module,
    migration_strict,
    smoke,
    taskinstance,
)

SNIPPET_START = "<!-- --8<-- [start:quickstart] -->"
SNIPPET_END = "<!-- --8<-- [end:quickstart] -->"
PITCH_START = "<!-- readme-sync:start:pitch -->"
PITCH_END = "<!-- readme-sync:end:pitch -->"
ACTION_START = "<!-- readme-sync:start:action-example -->"
ACTION_END = "<!-- readme-sync:end:action-example -->"
DOCS_BASE_URL = "https://nredd.github.io/pytest-airflow-in-a-box/"


def component_problem_codes() -> frozenset[str]:
    """Return every machine-readable component problem code defined in source.

    Returns:
        frozenset of str problem codes emitted through `ComponentProblem.code`.
    """
    prefixes = tuple(f"{kind.value}-" for kind in ComponentKind)
    return frozenset(
        value
        for name, value in vars(compat_components).items()
        if name.isupper() and isinstance(value, str) and value.startswith(prefixes)
    )


def public_diagnostic_names() -> frozenset[str]:
    """Return exported warning and error class names from public diagnostic modules.

    Returns:
        frozenset of str public class names ending in `Warning` or `Error`.
    """
    return frozenset(
        name
        for module in PUBLIC_DIAGNOSTIC_MODULES
        for name in module.__all__
        if name.endswith(("Warning", "Error"))
    )


def backticked_names(text: str) -> set[str]:
    """Return every single-backticked identifier-shaped token in `text`.

    Parameters:
        text: str of Markdown to scan.

    Returns:
        set of str identifiers found between single backticks.
    """
    return set(re.findall(r"`([a-z_][a-z0-9_]*)`", text))


def marked_block(path: Path, start: str, end: str) -> str:
    """Return the Markdown between a named pair of synchronization markers.

    Parameters:
        path: Path to the Markdown file containing the markers.
        start: str opening marker.
        end: str closing marker.

    Returns:
        str stripped content between the markers.

    Raises:
        AssertionError: either marker is missing.
    """
    text = path.read_text(encoding="utf-8")
    relative = path.relative_to(REPO_ROOT)
    assert start in text, f"{relative} lost its {start!r} marker"
    assert end in text, f"{relative} lost its {end!r} marker"
    return text.split(start, 1)[1].split(end, 1)[0].strip()


def visible_pitch(path: Path) -> str:
    """Return normalized visible prose from a marked README pitch block.

    Parameters:
        path: Path to the README or docs homepage.

    Returns:
        str visible pitch copy with renderer-specific quote markup removed.
    """
    block = marked_block(path, PITCH_START, PITCH_END)
    visible_lines = []
    for line in block.splitlines():
        if line == '!!! question ""':
            continue
        visible_lines.append(line.removeprefix("    ").removeprefix("> "))
    return " ".join(" ".join(visible_lines).split())


def heading_anchors(path: Path) -> set[str]:
    """Return MkDocs-style anchors for the headings in one Markdown file.

    Parameters:
        path: Path to the Markdown source.

    Returns:
        set of str heading anchors.
    """
    anchors = set()
    for heading in re.findall(r"^#{1,6} (.+?)\s*#*$", path.read_text(encoding="utf-8"), re.M):
        heading = re.sub(r"\[([^]]+)]\([^)]*\)", r"\1", heading)
        heading = re.sub(r"[`*_]", "", heading)
        anchor = re.sub(r"[-\s]+", "-", re.sub(r"[^\w\s-]", "", heading).strip().lower())
        anchors.add(anchor)
    return anchors


def readme_what_ships_table() -> str:
    """Return the body of `README.md`'s "What ships" fixture table.

    Returns:
        str containing the table rows.

    Raises:
        AssertionError: the section or its table is missing.
    """
    text = README.read_text(encoding="utf-8")
    assert "## What ships" in text, "README lost its `## What ships` section"
    section = text.split("## What ships", 1)[1].split("\n## ", 1)[0]
    rows = [line for line in section.splitlines() if line.startswith("|")]
    assert rows, "README's `## What ships` section no longer contains a table"
    return "\n".join(rows)


@pytest.mark.parametrize("name", sorted(fixtures_module.__all__))
def test_every_exported_fixture_is_documented(name: str) -> None:
    """Every fixture in `fixtures.__all__` appears in the fixtures reference page.

    Parameters:
        name: str fixture name exported by `pytest_airflow_in_a_box.fixtures`.
    """
    page = FIXTURES_PAGE.read_text(encoding="utf-8")
    assert f"`{name}`" in page, (
        f"`{name}` is exported from `fixtures.__all__` but absent from "
        f"{FIXTURES_PAGE.relative_to(REPO_ROOT)}"
    )


def test_readme_names_only_real_fixtures() -> None:
    """Every fixture named in the README's "What ships" table really is exported."""
    exported = set(fixtures_module.__all__)
    named = backticked_names(readme_what_ships_table())
    unknown = {name for name in named if name not in exported}
    assert not unknown, (
        f"README's `## What ships` table names {sorted(unknown)}, which are not in "
        "`fixtures.__all__`"
    )


def test_readme_names_a_fixture_from_every_group() -> None:
    """The README table is a teaser, but an empty row is a silent regression."""
    rows = [row for row in readme_what_ships_table().splitlines() if "| ---" not in row]
    body = [row for row in rows if not row.startswith("| Job")]
    for row in body:
        assert backticked_names(row), f"README `## What ships` row names no fixture: {row}"


@pytest.mark.parametrize("description", MARKER_DESCRIPTIONS)
def test_every_registered_marker_is_documented(description: str) -> None:
    """Every marker registered with pytest appears in the markers reference page.

    Parameters:
        description: str marker description as registered in `MARKER_DESCRIPTIONS`.
    """
    name = description.split("(", 1)[0].split(":", 1)[0]
    page = MARKERS_PAGE.read_text(encoding="utf-8")
    # A marker that takes arguments is documented in its call form, e.g. `environment(name)`,
    # so accept a backtick or an opening paren as the closing delimiter.
    documented = re.search(rf"`{re.escape(name)}[`(]", page)
    assert documented, (
        f"marker `{name}` is registered but absent from {MARKERS_PAGE.relative_to(REPO_ROOT)}"
    )


@pytest.mark.parametrize("code", sorted(component_problem_codes()))
def test_every_component_problem_code_is_documented(code: str) -> None:
    """Every machine-readable component problem code appears in the component guide.

    Parameters:
        code: str machine-readable `ComponentProblem.code` value.
    """
    page = COMPONENTS_PAGE.read_text(encoding="utf-8")
    assert f"`{code}`" in page, (
        f"component problem code `{code}` is absent from {COMPONENTS_PAGE.relative_to(REPO_ROOT)}"
    )


@pytest.mark.parametrize("name", sorted(public_diagnostic_names()))
def test_every_public_diagnostic_class_is_documented(name: str) -> None:
    """Every exported warning and error class is named somewhere in the guide.

    Parameters:
        name: str public warning or error class name.
    """
    guide = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((REPO_ROOT / "docs").rglob("*.md"))
    )
    assert f"`{name}`" in guide, f"public diagnostic class `{name}` is absent from docs/"


def test_readme_quickstart_matches_the_canonical_snippet() -> None:
    """`README.md`'s Quickstart is byte-identical to `docs/quickstart.md`'s snippet block.

    `docs/quickstart.md` is canonical. The README cannot use `pymdownx.snippets` -- GitHub
    renders raw Markdown -- so the fence is duplicated on purpose and guarded here.
    """
    quickstart = QUICKSTART_PAGE.read_text(encoding="utf-8")
    page_name = QUICKSTART_PAGE.relative_to(REPO_ROOT)
    assert SNIPPET_START in quickstart, f"{page_name} lost its snippet start marker"
    assert SNIPPET_END in quickstart, f"{page_name} lost its snippet end marker"
    canonical = quickstart.split(SNIPPET_START, 1)[1].split(SNIPPET_END, 1)[0].strip()

    readme = README.read_text(encoding="utf-8")
    assert "## Quickstart" in readme, "README lost its `## Quickstart` section"
    mirrored = readme.split("## Quickstart", 1)[1].split("\n`run_dag` proves", 1)[0].strip()

    assert mirrored == canonical, (
        "README's Quickstart has drifted from `docs/quickstart.md`'s snippet block"
    )


def test_readme_pitch_matches_the_docs_homepage() -> None:
    """Keep the README and PyPI pitch aligned with the docs homepage."""
    assert visible_pitch(README) == visible_pitch(DOCS_HOME)


def test_readme_action_example_matches_the_canonical_guide() -> None:
    """Keep the Marketplace-facing workflow identical to the Action guide."""
    assert marked_block(README, ACTION_START, ACTION_END) == marked_block(
        ACTION_GUIDE, ACTION_START, ACTION_END
    )


def test_readme_documentation_links_resolve_to_sources() -> None:
    """Every published-docs link in the README names a real page and heading."""
    readme = README.read_text(encoding="utf-8")
    urls = re.findall(rf"{re.escape(DOCS_BASE_URL)}[^)\s]*", readme)
    assert urls, "README no longer links to the documentation site"
    for url in urls:
        parsed = urlparse(url)
        relative_url = parsed.path.removeprefix("/pytest-airflow-in-a-box/").rstrip("/")
        source = DOCS_HOME if not relative_url else REPO_ROOT / "docs" / f"{relative_url}.md"
        assert source.is_file(), f"{url} does not map to a docs source file"
        if parsed.fragment:
            fragment = unquote(parsed.fragment)
            assert fragment in heading_anchors(source), f"{url} names no heading in {source}"


def test_action_metadata_is_marketplace_ready() -> None:
    """Pin the Marketplace identity, description limit, and badge branding."""
    metadata = yaml.safe_load(ACTION_METADATA.read_text(encoding="utf-8"))
    assert metadata["name"] == "pytest-airflow-in-a-box"
    assert len(metadata["description"]) <= 125
    assert metadata["branding"] == {"icon": "box", "color": "blue"}


def test_structlog_capture_is_a_public_typed_contract() -> None:
    """`StructlogCapture` is re-exported from `types`, as the fixtures page promises.

    `docs/reference/fixtures.md` states that every fixture's return type is a typed contract
    in `pytest_airflow_in_a_box.types`. `cap_structlog` returns a `StructlogCapture`, which
    was defined in `logging.py` and never re-exported, so the promise was false for it.
    """
    from pytest_airflow_in_a_box import types
    from pytest_airflow_in_a_box.logging import StructlogCapture

    assert "StructlogCapture" in types.__all__
    assert types.StructlogCapture is StructlogCapture
