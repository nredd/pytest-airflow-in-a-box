"""Test `airflow_local_settings.py` generation, collision detection, and composition."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.storage import sqlite
from pytest_airflow_in_a_box.storage.sqlite import (
    check_local_settings_collision,
    create_metadata_engine,
    install_legacy_sqlite_listener,
    local_settings_path,
    resolve_local_settings_module,
    validate_local_settings_module_shape,
    write_local_settings,
)


class _FakeSpec:
    """Stand in for a `ModuleSpec` with a controlled `origin`."""

    def __init__(self, origin: str | None) -> None:
        """Record the controlled origin.

        Parameters:
            origin: str | None simulating `ModuleSpec.origin`.
        """

        self.origin = origin


class _FakeImportlibUtil:
    """Stand in for `importlib.util`, returning one fixed `find_spec` result."""

    def __init__(self, spec: object) -> None:
        """Record the fixed result.

        Parameters:
            spec: object returned by every `find_spec` call.
        """

        self._spec = spec

    def find_spec(self, name: str) -> object:
        """Return the fixed result regardless of the requested name.

        Parameters:
            name: str containing the requested module name.

        Returns:
            object containing the fixed result configured at construction.
        """

        del name
        return self._spec


class _RaisingImportlibUtil:
    """Stand in for `importlib.util`, raising `ValueError` from every `find_spec` call."""

    def find_spec(self, name: str) -> object:
        """Raise `ValueError`, matching a `sys.modules` entry with no `__spec__`.

        Parameters:
            name: str containing the requested module name.

        Raises:
            ValueError: Always.
        """

        raise ValueError(f"{name!r} has no __spec__")


@pytest.fixture(autouse=True)
def _isolate_local_settings_module() -> Iterator[None]:
    """Prevent `sys.path` mutation and cached imports from leaking between tests."""

    original_path = list(sys.path)
    yield
    sys.path[:] = original_path
    sys.modules.pop("airflow_local_settings", None)


def test_local_settings_path_derives_from_root(tmp_path: Path) -> None:
    """Derive the generated settings path as a fixed child of the run root."""

    assert local_settings_path(tmp_path) == tmp_path / "config" / "airflow_local_settings.py"


def test_write_local_settings_is_deterministic_and_importable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write stable ASCII source that installs fallback and exports the engine hook."""

    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)
    first_source = settings_path.read_text(encoding="ascii")
    write_local_settings(settings_path)

    assert settings_path.read_text(encoding="ascii") == first_source
    assert first_source.encode("ascii").decode("ascii") == first_source
    assert "password" not in first_source.lower()
    monkeypatch.syspath_prepend(str(settings_path.parent))
    monkeypatch.delitem(sys.modules, "airflow_local_settings", raising=False)
    importlib.invalidate_caches()
    local_settings = importlib.import_module("airflow_local_settings")
    assert local_settings.__all__ == ("create_metadata_engine",)
    assert local_settings.create_metadata_engine is create_metadata_engine
    assert local_settings.install_legacy_sqlite_listener is install_legacy_sqlite_listener
    monkeypatch.delitem(sys.modules, "airflow_local_settings")


def test_write_local_settings_rejects_relative_path() -> None:
    """Require bootstrap artifacts to have an unambiguous absolute location."""

    with pytest.raises(ValueError, match="must be absolute"):
        write_local_settings(Path("airflow_local_settings.py"))


def test_write_local_settings_is_ascii_and_deterministic_with_user_module(
    tmp_path: Path,
) -> None:
    """Compose a configured module reference into stable, ASCII, star-import-free source."""

    settings_path = tmp_path / "config" / "airflow_local_settings.py"

    write_local_settings(settings_path, user_module="some.dotted.module")
    first_source = settings_path.read_text(encoding="ascii")
    write_local_settings(settings_path, user_module="some.dotted.module")

    assert settings_path.read_text(encoding="ascii") == first_source
    assert first_source.encode("ascii").decode("ascii") == first_source
    assert "from importlib import import_module" in first_source
    assert 'import_module("some.dotted.module")' in first_source
    assert "import *" not in first_source


def test_write_local_settings_composes_configured_user_module_with_dunder_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compose only a configured module's `__all__`-listed names, unioned with our own."""

    user_dir = tmp_path / "userpkg_with_all"
    user_dir.mkdir()
    (user_dir / "policy_with_all.py").write_text(
        'MARKER = "with-all"\n__all__ = ("MARKER",)\nHIDDEN = "not exported"\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(user_dir))
    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    monkeypatch.syspath_prepend(str(settings_path.parent))
    write_local_settings(settings_path, user_module="policy_with_all")

    importlib.invalidate_caches()
    module = importlib.import_module("airflow_local_settings")
    try:
        assert module.MARKER == "with-all"
        assert not hasattr(module, "HIDDEN")
        assert set(module.__all__) == {"create_metadata_engine", "MARKER"}
        assert module.create_metadata_engine is create_metadata_engine
    finally:
        sys.modules.pop("policy_with_all", None)


def test_write_local_settings_composes_user_module_without_dunder_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fall back to every non-dunder attribute, mirroring Airflow's own `import_local_settings`.

    Airflow's real fallback only excludes double-underscore (dunder) names, not every
    underscore-prefixed name -- a single leading underscore is composed too.
    """

    user_dir = tmp_path / "userpkg_without_all"
    user_dir.mkdir()
    (user_dir / "policy_without_all.py").write_text(
        'PUBLIC_MARKER = "no-all"\n_underscored = "still exported"\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(user_dir))
    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    monkeypatch.syspath_prepend(str(settings_path.parent))
    write_local_settings(settings_path, user_module="policy_without_all")

    importlib.invalidate_caches()
    module = importlib.import_module("airflow_local_settings")
    try:
        assert module.PUBLIC_MARKER == "no-all"
        assert module._underscored == "still exported"
        assert set(module.__all__) == {"create_metadata_engine", "PUBLIC_MARKER", "_underscored"}
        assert "__name__" not in module.__all__
    finally:
        sys.modules.pop("policy_without_all", None)


@pytest.mark.parametrize(
    "value",
    ["./policies.py", "/abs/policies.py", "policies.py", "a/b", "a\\b"],
)
def test_validate_local_settings_module_shape_rejects_file_path_like_values(
    value: str,
) -> None:
    """Reject filesystem-path-shaped values before ever attempting to resolve them."""

    with pytest.raises(pytest.UsageError, match="must be a dotted module path, not a file path"):
        validate_local_settings_module_shape(value)


@pytest.mark.parametrize("value", ["1bad.name", "bad-name", "bad..name", ""])
def test_validate_local_settings_module_shape_rejects_invalid_identifiers(
    value: str,
) -> None:
    """Reject values with a non-identifier dotted segment."""

    with pytest.raises(pytest.UsageError, match="must be a dotted module path"):
        validate_local_settings_module_shape(value)


def test_validate_local_settings_module_shape_accepts_well_formed_values() -> None:
    """Accept plain and dotted identifiers without touching the filesystem."""

    validate_local_settings_module_shape("policies")
    validate_local_settings_module_shape("myproject.cluster_policies")


def test_resolve_local_settings_module_rejects_unresolvable_top_level_module() -> None:
    """Reject a top-level module name that resolves to nothing."""

    with pytest.raises(pytest.UsageError, match="names a module that cannot be imported"):
        resolve_local_settings_module("definitely_not_a_real_module_xyz")


def test_resolve_local_settings_module_rejects_unresolvable_parent_package() -> None:
    """Reject a dotted path whose parent package cannot be imported at all."""

    with pytest.raises(pytest.UsageError, match="names a module that cannot be imported"):
        resolve_local_settings_module("definitely_not_a_real_pkg_xyz.sub")


def test_resolve_local_settings_module_wraps_a_broken_parent_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Report a parent package's own import-time failure as one usage error, not a traceback."""

    package_dir = tmp_path / "broken_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text(
        'raise RuntimeError("boom from parent package")\n', encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    with pytest.raises(pytest.UsageError, match="names a module that cannot be imported"):
        resolve_local_settings_module("broken_pkg.sub")


def test_resolve_local_settings_module_rejects_namespace_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject an implicit namespace package with no single origin file."""

    namespace_dir = tmp_path / "a_namespace_pkg"
    namespace_dir.mkdir()
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    with pytest.raises(pytest.UsageError, match="namespace package"):
        resolve_local_settings_module("a_namespace_pkg")


def test_resolve_local_settings_module_accepts_regular_package_and_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Accept both a regular package and a submodule reached through it."""

    package_dir = tmp_path / "real_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "policies.py").write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    resolve_local_settings_module("real_pkg")
    resolve_local_settings_module("real_pkg.policies")


def test_check_local_settings_collision_passes_for_the_generated_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Accept the run's own file, including on a benign rerun with no duplicate entry.

    This test suite is itself collected under a real bootstrapped `AIRFLOW_HOME`, whose
    own generated `config` directory is already on `sys.path` (appended, once, before
    any test runs). Prepending our own path here keeps this test's assertion about
    *this* run's file from depending on that real, already-resolved entry.
    """

    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)
    monkeypatch.syspath_prepend(str(settings_path.parent))

    check_local_settings_collision(settings_path)
    check_local_settings_collision(settings_path)

    assert sys.path.count(str(settings_path.parent)) == 1


def test_check_local_settings_collision_rejects_a_foreign_module_earlier_on_sys_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail loudly when a foreign module earlier on `sys.path` would win the import race."""

    foreign_dir = tmp_path / "foreign"
    foreign_dir.mkdir()
    (foreign_dir / "airflow_local_settings.py").write_text("DECOY = True\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(foreign_dir))
    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)

    with pytest.raises(pytest.UsageError, match=r"resolves to .*not this run's generated"):
        check_local_settings_collision(settings_path)


def test_check_local_settings_collision_handles_missing_spec_defensively(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail loudly on the otherwise-unreachable case where resolution finds nothing."""

    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)
    monkeypatch.setattr(sqlite, "importlib_util", _FakeImportlibUtil(None))

    with pytest.raises(pytest.UsageError, match="did not resolve"):
        check_local_settings_collision(settings_path)


def test_check_local_settings_collision_rejects_a_namespace_package_winning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail loudly on the otherwise-unreachable case where a namespace package wins.

    A regular module anywhere on `sys.path` always outranks a namespace-package portion
    per Python's own import algorithm, so this run's own generated file (a regular
    module) makes this branch unreachable through real `sys.path` composition alone --
    it is exercised here through a controlled `find_spec` result instead.
    """

    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)
    monkeypatch.setattr(sqlite, "importlib_util", _FakeImportlibUtil(_FakeSpec(origin=None)))

    with pytest.raises(pytest.UsageError, match="namespace package"):
        check_local_settings_collision(settings_path)


def test_check_local_settings_collision_treats_a_stale_cache_value_error_as_unresolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail loudly, not with a raw traceback, when `find_spec` raises on a bad cache entry.

    `find_spec` raises `ValueError` instead of searching `sys.path` when the name is
    already in `sys.modules` with no `__spec__` -- an edge case rather than the ordinary
    "resolved to something else" collision, so it is treated the same as an outright miss.
    """

    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)
    monkeypatch.setattr(sqlite, "importlib_util", _RaisingImportlibUtil())

    with pytest.raises(pytest.UsageError, match="did not resolve"):
        check_local_settings_collision(settings_path)


def test_write_local_settings_composed_module_cannot_clobber_create_metadata_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the plugin's own `create_metadata_engine` even when the user module shadows it.

    A real Airflow cluster-policy module plausibly defines `create_metadata_engine`
    itself, since that is the exact documented Airflow override point this generated
    file also exports. Composition must never let that silently replace the plugin's
    SQLite-tuned engine factory -- the same class of silent-shadowing failure this
    feature exists to prevent, just moved from file-level to symbol-level.
    """

    user_dir = tmp_path / "userpkg_clobber"
    user_dir.mkdir()
    (user_dir / "policy_clobber.py").write_text(
        "def create_metadata_engine(*args, **kwargs):\n"
        '    raise AssertionError("user override must never run")\n'
        '__all__ = ("create_metadata_engine",)\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(user_dir))
    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    monkeypatch.syspath_prepend(str(settings_path.parent))
    write_local_settings(settings_path, user_module="policy_clobber")

    importlib.invalidate_caches()
    module = importlib.import_module("airflow_local_settings")
    try:
        assert module.create_metadata_engine is create_metadata_engine
        assert module.__all__ == ("create_metadata_engine",)
    finally:
        sys.modules.pop("policy_clobber", None)
