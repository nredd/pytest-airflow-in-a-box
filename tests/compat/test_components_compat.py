"""Test component-check registry invariants and version/family branching."""

from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace

import pytest

from pytest_airflow_in_a_box._compat import components as compat_components
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, ExecutorContract
from pytest_airflow_in_a_box.components import ComponentKind


def test_components_compat_import_does_not_import_airflow() -> None:
    """Keep the private check registry import-safe before Airflow bootstrap."""

    script = (
        "import sys; import pytest_airflow_in_a_box._compat.components; "
        "raise SystemExit('airflow' in sys.modules)"
    )

    subprocess.check_output([sys.executable, "-c", script], text=True)


def test_check_registry_kinds_match_public_enum() -> None:
    """Key every registry row and classifier by a value `ComponentKind` also has."""

    registry_kinds = {kind for kind, _name, _checker in compat_components.CHECK_REGISTRY}
    classifier_kinds = set(compat_components.KIND_CLASSIFIERS)
    enum_values = {member.value for member in ComponentKind}

    assert registry_kinds <= enum_values
    assert classifier_kinds == enum_values


def test_check_registry_names_are_unique() -> None:
    """Register each check name exactly once."""

    names = [name for _kind, name, _checker in compat_components.CHECK_REGISTRY]

    assert len(names) == len(set(names))


def test_check_registry_rows_are_callable() -> None:
    """Register only real callables as checkers."""

    for _kind, _name, checker in compat_components.CHECK_REGISTRY:
        assert callable(checker)


@pytest.mark.parametrize(
    ("contract", "correct_name", "correct_type", "wrong_name"),
    [
        (ExecutorContract.V3_1, "supports_sentry", True, "sentry_integration"),
        (ExecutorContract.V3_2, "sentry_integration", "custom-tool", "supports_sentry"),
        (ExecutorContract.V3_3, "sentry_integration", "custom-tool", "supports_sentry"),
    ],
)
def test_executor_flag_wrong_type_follows_the_resolved_contract(
    monkeypatch: pytest.MonkeyPatch,
    contract: ExecutorContract,
    correct_name: str,
    correct_type: object,
    wrong_name: str,
) -> None:
    """Accept the contract-correct flag and flag the other release's flag name.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake resolved contract.
        contract: ExecutorContract to resolve for this case.
        correct_name: str naming the attribute this contract expects.
        correct_type: object containing a correctly typed value for `correct_name`.
        wrong_name: str naming the other release's attribute name.
    """

    monkeypatch.setattr(
        compat_components,
        "resolve_capabilities",
        lambda: SimpleNamespace(executor_contract=contract),
    )

    correct = type("CorrectExecutor", (), {correct_name: correct_type})
    wrong_name_only = type("WrongNameExecutor", (), {wrong_name: object()})

    assert tuple(compat_components._check_executor_flag_wrong_type(correct)) == ()
    (problem,) = compat_components._check_executor_flag_wrong_type(wrong_name_only)
    assert problem.code == "executor-flag-wrong-type"
    assert wrong_name in problem.message


def test_executor_flag_wrong_type_flags_a_mistyped_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flag the contract-correct attribute name carrying the wrong value type."""

    monkeypatch.setattr(
        compat_components,
        "resolve_capabilities",
        lambda: SimpleNamespace(executor_contract=ExecutorContract.V3_3),
    )

    mistyped = type("MistypedExecutor", (), {"sentry_integration": True})

    (problem,) = compat_components._check_executor_flag_wrong_type(mistyped)

    assert problem.code == "executor-flag-wrong-type"
    assert "sentry_integration" in problem.message


def test_executor_flag_wrong_type_skips_without_a_resolved_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing on 2.x, where `executor_contract` resolves to None."""

    monkeypatch.setattr(
        compat_components,
        "resolve_capabilities",
        lambda: SimpleNamespace(executor_contract=None),
    )

    broken = type("Broken", (), {"sentry_integration": True, "supports_sentry": False})

    assert tuple(compat_components._check_executor_flag_wrong_type(broken)) == ()


@pytest.mark.parametrize(
    "checker_name",
    [
        "_check_listener_no_matching_hookspec",
        "_check_listener_unknown_argument",
        "_check_listener_core_manager_only",
        "_check_listener_sdk_manager_only",
        "_check_executor_missing_override",
        "_check_executor_stale_attribute",
    ],
)
def test_v3_only_checks_are_inapplicable_on_2x(
    monkeypatch: pytest.MonkeyPatch, checker_name: str
) -> None:
    """Report nothing on 2.x, whose listener and executor internals are out of scope.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake 2.x family.
        checker_name: str naming the module-level checker function under test.
    """

    monkeypatch.setattr(compat_components, "installed_family", lambda: AirflowFamily.V2)
    checker = getattr(compat_components, checker_name)

    # A component carrying every offending shape at once; on 3.x every one of these
    # checkers would report at least one problem for it.
    class ObviouslyBroken:
        is_single_threaded = True
        supports_pickling = True
        change_sensitivity = True
        execute_async = True

        def on_totally_made_up_event(self) -> None:
            pass

    assert tuple(checker(ObviouslyBroken)) == ()


def _fail_if_called() -> AirflowFamily:
    """Raise, so a caller of this fake proves a check gates on family unexpectedly.

    Returns:
        AirflowFamily is never actually returned; this always raises.

    Raises:
        AssertionError: Always.
    """

    raise AssertionError("timetable checks must not gate on family")


def test_timetable_checks_run_regardless_of_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep timetable checks family-agnostic: no `installed_family` gate exists."""

    monkeypatch.setattr(compat_components, "installed_family", _fail_if_called)

    from airflow.timetables.base import Timetable

    class Local(Timetable):
        pass

    problems = list(compat_components._check_timetable_local_qualname(Local))

    assert problems[0].code == "timetable-local-qualname"


def test_timetable_serialize_not_json_skips_a_non_callable_serialize_attribute() -> None:
    """Report nothing when `serialize` is absent or shadowed by a non-callable."""

    instance = type("NoSerialize", (), {"serialize": "not-a-method"})()

    assert tuple(compat_components._check_timetable_serialize_not_json(instance)) == ()


def test_listener_hookimpls_keeps_a_non_self_first_parameter() -> None:
    """Keep every declared parameter when the first one is not literally `self`."""

    from airflow.listeners import hookimpl

    class OddReceiver:
        @hookimpl
        def on_starting(this, component: object) -> None:
            del this, component

    impls = compat_components._listener_hookimpls(OddReceiver)

    assert impls["on_starting"] == ("this", "component")


def test_listener_checks_report_nothing_without_any_hookimpl() -> None:
    """Report nothing for a component that defines no `@hookimpl`-marked method."""

    class NotAListener:
        def on_starting(self, component: object) -> None:
            del component

    for checker_name in (
        "_check_listener_no_matching_hookspec",
        "_check_listener_unknown_argument",
        "_check_listener_core_manager_only",
        "_check_listener_sdk_manager_only",
    ):
        checker = getattr(compat_components, checker_name)
        assert tuple(checker(NotAListener)) == ()
