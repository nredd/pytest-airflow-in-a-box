"""Test the runtime anti-pattern probes with fake patch targets and operator doubles."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar

import pytest

from pytest_airflow_in_a_box._compat import introspection

FAKE_MODULE_NAME = "fake_secrets_module"


class FakeVariable:
    """Stand-in secrets entry point with a recordable classmethod."""

    calls: ClassVar[list[tuple[Any, ...]]] = []

    @classmethod
    def get(cls, key: str, default: str | None = None) -> str:
        """Return a canned value while recording the pass-through call.

        Parameters:
            key: str containing the looked-up key.
            default: str | None containing an unused default.

        Returns:
            str containing a canned value derived from the key.
        """

        cls.calls.append((key, default))
        return f"value-of-{key}"


@pytest.fixture
def fake_targets(monkeypatch: pytest.MonkeyPatch) -> type[FakeVariable]:
    """Plant a fake secrets module and point the patch targets at it.

    Parameters:
        monkeypatch: pytest.MonkeyPatch restoring module and target state afterwards.

    Returns:
        type[FakeVariable] containing the plantable entry-point class.
    """

    module: Any = ModuleType(FAKE_MODULE_NAME)
    module.FakeVariable = FakeVariable
    monkeypatch.setitem(sys.modules, FAKE_MODULE_NAME, module)
    monkeypatch.setattr(FakeVariable, "calls", [])
    monkeypatch.setattr(
        introspection,
        "SECRETS_PATCH_TARGETS",
        (("variable", FAKE_MODULE_NAME, "FakeVariable", "get"),),
    )
    return FakeVariable


def test_record_secrets_lookups_records_and_calls_through(
    fake_targets: type[FakeVariable], tmp_path: Path
) -> None:
    """Record each lookup with attribution and still return the original result."""

    original = fake_targets.__dict__["get"]
    caller = tmp_path / "caller.py"
    caller.write_text(
        f"import {FAKE_MODULE_NAME}\n"
        f"RESULT = {FAKE_MODULE_NAME}.FakeVariable.get('my_key', default='d')\n",
        encoding="utf-8",
    )

    with introspection.record_secrets_lookups(tmp_path) as recorded:
        namespace: dict[str, Any] = {}
        exec(compile(caller.read_text(encoding="utf-8"), str(caller), "exec"), namespace)

    assert namespace["RESULT"] == "value-of-my_key"
    assert fake_targets.calls == [("my_key", "d")]
    assert len(recorded) == 1
    lookup = recorded[0]
    assert lookup.kind == "variable"
    assert lookup.key == "my_key"
    assert lookup.file == str(caller.resolve())
    assert lookup.line == 2
    assert fake_targets.__dict__["get"] is original


def test_record_secrets_lookups_marks_unattributed_calls(
    fake_targets: type[FakeVariable], tmp_path: Path
) -> None:
    """Record ``None`` attribution for a call with no frame under the Dag folder."""

    with introspection.record_secrets_lookups(tmp_path / "empty") as recorded:
        fake_targets.get("direct_key")

    assert recorded == [
        introspection.SecretsLookup(kind="variable", key="direct_key", file=None, line=None)
    ]


def test_record_secrets_lookups_restores_on_body_failure(
    fake_targets: type[FakeVariable], tmp_path: Path
) -> None:
    """Restore the original descriptor even when the guarded body raises."""

    original = fake_targets.__dict__["get"]

    with (
        pytest.raises(RuntimeError, match="parse exploded"),
        introspection.record_secrets_lookups(tmp_path),
    ):
        raise RuntimeError("parse exploded")

    assert fake_targets.__dict__["get"] is original


def test_record_secrets_lookups_patches_duplicate_targets_once(
    monkeypatch: pytest.MonkeyPatch, fake_targets: type[FakeVariable], tmp_path: Path
) -> None:
    """Skip a duplicate `(class, attribute)` target so restoration stays faithful."""

    monkeypatch.setattr(
        introspection,
        "SECRETS_PATCH_TARGETS",
        (
            ("variable", FAKE_MODULE_NAME, "FakeVariable", "get"),
            ("variable", FAKE_MODULE_NAME, "FakeVariable", "get"),
        ),
    )
    original = fake_targets.__dict__["get"]

    with introspection.record_secrets_lookups(tmp_path) as recorded:
        fake_targets.get("once")

    assert len(recorded) == 1
    assert fake_targets.__dict__["get"] is original


def test_record_secrets_lookups_skips_unresolvable_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Skip a missing module and a class-absent attribute without failing."""

    class Inheritor(FakeVariable):
        """Subclass whose `get` lives only on the parent class."""

    module: Any = ModuleType(FAKE_MODULE_NAME)
    module.Inheritor = Inheritor
    monkeypatch.setitem(sys.modules, FAKE_MODULE_NAME, module)
    monkeypatch.setattr(
        introspection,
        "SECRETS_PATCH_TARGETS",
        (
            ("variable", "definitely_not_a_module", "Variable", "get"),
            ("variable", FAKE_MODULE_NAME, "Inheritor", "get"),
        ),
    )

    with introspection.record_secrets_lookups(tmp_path) as recorded:
        pass

    assert recorded == []


@pytest.mark.parametrize(
    ("args", "kwargs", "expected"),
    [
        (("my_key",), {}, "my_key"),
        ((), {"key": "kw_key"}, "kw_key"),
        ((), {"conn_id": "db"}, "db"),
        ((object(),), {}, "<unknown>"),
    ],
)
def test_lookup_key_extraction(
    args: tuple[Any, ...], kwargs: dict[str, Any], expected: str
) -> None:
    """Prefer known keywords, fall back to the first string positional, then a marker."""

    assert introspection._lookup_key(args, kwargs) == expected


def test_mapped_expansion_reports_unmapped_tasks() -> None:
    """Report an operator without mapping as unmapped."""

    assert introspection.mapped_expansion(SimpleNamespace(task_id="t")) == (False, False, None)


def test_mapped_expansion_classifies_literal_and_runtime_sources() -> None:
    """Distinguish literal expansion values from runtime data in either input slot."""

    literal = SimpleNamespace(
        task_id="literal",
        is_mapped=True,
        expand_input=SimpleNamespace(value={"x": [1, "a", (True, None)], "y": {b"k": 1.5}}),
        max_active_tis_per_dag=None,
        partial_kwargs={},
    )
    runtime = SimpleNamespace(
        task_id="runtime",
        is_mapped=True,
        expand_input=SimpleNamespace(value={}),
        op_kwargs_expand_input=SimpleNamespace(value={"x": object()}),
        max_active_tis_per_dag=None,
        partial_kwargs={},
    )

    assert introspection.mapped_expansion(literal) == (True, False, None)
    assert introspection.mapped_expansion(runtime) == (True, True, None)


def test_mapped_expansion_reads_the_cap_from_either_home() -> None:
    """Read the concurrency cap from the attribute or from `partial_kwargs`."""

    direct = SimpleNamespace(
        task_id="direct",
        is_mapped=True,
        expand_input=SimpleNamespace(value={}),
        max_active_tis_per_dag=3,
        partial_kwargs={},
    )
    partial = SimpleNamespace(
        task_id="partial",
        is_mapped=True,
        expand_input=SimpleNamespace(value={}),
        max_active_tis_per_dag=None,
        partial_kwargs={"max_active_tis_per_dag": 7},
    )
    malformed = SimpleNamespace(
        task_id="malformed",
        is_mapped=True,
        expand_input=SimpleNamespace(value={}),
        max_active_tis_per_dag="three",
        partial_kwargs=None,
    )
    capless = SimpleNamespace(
        task_id="capless",
        is_mapped=True,
        expand_input=SimpleNamespace(value={}),
        max_active_tis_per_dag=None,
        partial_kwargs=None,
    )

    assert introspection.mapped_expansion(direct) == (True, False, 3)
    assert introspection.mapped_expansion(partial) == (True, False, 7)
    assert introspection.mapped_expansion(malformed) == (True, False, None)
    assert introspection.mapped_expansion(capless) == (True, False, None)


def test_mapped_expansion_downgrades_on_inspection_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Report a task whose internals raise as unmapped, with a warning."""

    class Explosive:
        """Operator double whose expansion internals raise on access."""

        task_id = "explosive"
        is_mapped = True

        @property
        def expand_input(self) -> Any:
            """Raise to simulate an Airflow release moving the attribute's contract.

            Returns:
                Any, never returned.

            Raises:
                RuntimeError: Always.
            """

            raise RuntimeError("moved in a future release")

    with caplog.at_level("WARNING", logger=introspection.LOGGER.name):
        result = introspection.mapped_expansion(Explosive())

    assert result == (False, False, None)
    assert "Could not inspect mapped expansion for task `explosive`" in caplog.text
