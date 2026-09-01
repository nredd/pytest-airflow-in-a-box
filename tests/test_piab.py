"""Contract tests for the `piab` alias mirror package.

The mirror is only trustworthy while it is a perfect, thin shadow of the canonical
package: same module set, same `__all__`, identical objects. These tests are the guard
that keeps `src/piab/` honest as the public surface evolves.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/336
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

import pytest

import piab
import pytest_airflow_in_a_box

REAL_ROOT = Path(pytest_airflow_in_a_box.__file__).parent
MIRROR_ROOT = Path(piab.__file__).parent


def public_module_names(root: Path) -> set[str]:
    """Collect the dotted relative names of a package tree's public modules.

    Parameters:
        root: pathlib.Path of the package directory to walk.

    Returns:
        set[str] of dotted module names relative to `root`, subpackages included,
        underscore-prefixed files and directories excluded.
    """

    names: set[str] = set()
    for path in root.rglob("*.py"):
        relative = path.relative_to(root)
        if any(part.startswith("_") for part in relative.parts[:-1]):
            continue
        if relative.name == "__init__.py":
            if relative.parts[:-1]:
                names.add(".".join(relative.parts[:-1]))
            continue
        if relative.name.startswith("_"):
            continue
        names.add(".".join(relative.with_suffix("").parts))
    return names


def test_mirror_module_set_matches_public_modules() -> None:
    """Keep the mirror tree in lockstep with the canonical package's public tree."""

    assert public_module_names(MIRROR_ROOT) == public_module_names(REAL_ROOT)


@pytest.mark.parametrize("name", sorted(public_module_names(REAL_ROOT)))
def test_mirror_reexports_the_real_objects(name: str) -> None:
    """Verify one mirror module re-exports the canonical module wholesale.

    Parameters:
        name: str dotted module name relative to both package roots.
    """

    mirror = importlib.import_module(f"piab.{name}")
    real = importlib.import_module(f"pytest_airflow_in_a_box.{name}")

    assert mirror.__all__ == real.__all__
    for exported in real.__all__:
        assert getattr(mirror, exported) is getattr(real, exported)


def test_mirror_version_matches_canonical() -> None:
    """Source `piab.__version__` from the canonical package, never a second literal."""

    assert piab.__version__ == pytest_airflow_in_a_box.__version__


def test_bare_mirror_import_is_inert() -> None:
    """Keep `import piab` free of Airflow and of every plugin submodule."""

    script = (
        "import sys; import piab; "
        "assert 'airflow' not in sys.modules; "
        "assert not any(key.startswith('pytest_airflow_in_a_box.') for key in sys.modules); "
        "assert not any(key.startswith('piab.') for key in sys.modules)"
    )

    subprocess.check_output([sys.executable, "-c", script], text=True)


def test_mirror_alias_resolves_submodules_lazily() -> None:
    """Resolve `piab.<module>` attribute access on demand via the PEP 562 hook."""

    script = (
        "import sys; import piab; "
        "assert piab.matchers.succeeded(21) is not None; "
        "assert piab.matchers is sys.modules['piab.matchers']"
    )

    subprocess.check_output([sys.executable, "-c", script], text=True)


def test_mirror_rejects_underscore_prefixed_names() -> None:
    """Refuse attribute access to private names; the mirror has no `_compat`."""

    with pytest.raises(AttributeError, match="has no attribute '_compat'"):
        piab.__getattr__("_compat")


def test_mirror_rejects_nonexistent_names() -> None:
    """Refuse attribute access to names that resolve to no mirror module."""

    with pytest.raises(AttributeError, match="has no attribute 'nonexistent'"):
        _ = piab.nonexistent
