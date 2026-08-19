"""Test component-check registry invariants and version/family branching."""

from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box._compat import components as compat_components
from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowCompatibilityError,
    AirflowFamily,
    ExecutorContract,
)
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


def test_executor_flag_wrong_type_reports_nothing_on_an_uncertified_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing, never raise, when `resolve_capabilities()` itself fails.

    Every other checker in the registry gates on the non-raising `installed_family()`;
    this one additionally needs `resolve_capabilities()` for `executor_contract`, which
    can raise `AirflowCompatibilityError` for an installed release that is not certified
    -- a reason unrelated to the checked executor. That must not escape `check_component`.
    """

    def _raise() -> object:
        raise AirflowCompatibilityError("installed release not certified")

    monkeypatch.setattr(compat_components, "resolve_capabilities", _raise)

    clean = type("CleanExecutor", (), {})

    assert tuple(compat_components._check_executor_flag_wrong_type(clean)) == ()


def test_executor_flag_wrong_type_is_gated_on_v3_like_its_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing on 2.x, gated the same way as the other two executor checks."""

    monkeypatch.setattr(compat_components, "installed_family", lambda: AirflowFamily.V2)

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

    Checked against real, unpatched 3.x first, so the emptiness asserted after patching
    to 2.x is proven to be the `installed_family()` gate closing rather than an
    already-empty result for an unrelated reason.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake 2.x family.
        checker_name: str naming the module-level checker function under test.
    """

    from airflow.listeners import hookimpl

    checker = getattr(compat_components, checker_name)

    # A component carrying every offending shape at once: a stale attribute and an
    # unenforced missing override for the executor checks, an unmatched hookimpl name,
    # an unknown hookimpl argument, and a core-only hookspec for the listener checks. No
    # real hookspec exists only in the SDK manager today (its hookspec set is a strict
    # subset of the core one), so `sdk_manager_only` has nothing to genuinely fire on and
    # is exercised for emptiness under the 2.x gate only.
    class ObviouslyBroken:
        is_single_threaded = True
        supports_pickling = True
        change_sensitivity = True
        execute_async = True

        @hookimpl
        def on_totally_made_up_event(self) -> None:
            pass

        @hookimpl
        def on_task_instance_success(
            self, previous_state: object, task_instance: object, bogus: object
        ) -> None:
            del previous_state, task_instance, bogus

        @hookimpl
        def on_dag_run_success(self, dag_run: object, msg: object) -> None:
            del dag_run, msg

    if checker_name != "_check_listener_sdk_manager_only":
        assert tuple(checker(ObviouslyBroken)) != (), "expected a real problem on unpatched 3.x"

    monkeypatch.setattr(compat_components, "installed_family", lambda: AirflowFamily.V2)

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

    (info,) = compat_components._listener_hookimpls(OddReceiver)

    assert info.method_name == "on_starting"
    assert info.hookspec_name == "on_starting"
    assert info.params == ("this", "component")


def test_listener_hookimpls_excludes_varargs_kwargs_keyword_only_and_defaulted() -> None:
    """Match pluggy's own `varnames()`: these are never validated against a hookspec.

    Reproduces the reported false positive directly: a hookimpl this shaped registers
    and fires without error against a real `pluggy.PluginManager` with the real
    `on_task_instance_success` hookspec, so recording `args`/`kwargs`/`only_kwarg`/
    `defaulted` here would make `listener-unknown-argument` misfire on legal code.
    """

    from airflow.listeners import hookimpl

    class KwargsListener:
        @hookimpl
        def on_task_instance_success(
            self,
            previous_state: object,
            task_instance: object,
            *args: object,
            only_kwarg: object = None,
            defaulted: object = None,
            **kwargs: object,
        ) -> None:
            del previous_state, task_instance, args, only_kwarg, defaulted, kwargs

    (info,) = compat_components._listener_hookimpls(KwargsListener)

    assert info.params == ("previous_state", "task_instance")


def test_listener_hookimpls_reads_specname() -> None:
    """Key a `@hookimpl(specname=...)` method by its target hookspec, not its own name."""

    from airflow.listeners import hookimpl

    class RenamedListener:
        @hookimpl(specname="on_task_instance_success")
        def handle_success(self, previous_state: object, task_instance: object) -> None:
            del previous_state, task_instance

    (info,) = compat_components._listener_hookimpls(RenamedListener)

    assert info.method_name == "handle_success"
    assert info.hookspec_name == "on_task_instance_success"
    assert tuple(compat_components._check_listener_no_matching_hookspec(RenamedListener)) == ()
    assert tuple(compat_components._check_listener_unknown_argument(RenamedListener)) == ()


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


def test_listener_manager_scope_skips_when_the_other_manager_does_not_exist() -> None:
    """Report nothing when `other_scope` resolves no hookspecs at all.

    Reproduces the real Airflow 3.1.0 scenario directly: on 3.1.x, the SDK listener
    manager does not exist, so `_SDK_LISTENER_SPEC_MODULES` resolves zero hookspecs, not
    "a manager that exists but does not register this particular hook." Before this fix,
    an empty `other_scope` made `only_this` equal to the *entire* `this_scope` hookspec
    set, so every real hookimpl on a plain, correctly written 3.1.x listener (e.g.
    `on_task_instance_success`) was wrongly flagged `listener-core-manager-only` --
    caught by real CI on the 3.1.x compat matrix legs, not by this repo's own
    single-Airflow-version local gate.
    """

    from airflow.listeners import hookimpl

    class CleanListener:
        @hookimpl
        def on_task_instance_success(self, previous_state: object, task_instance: object) -> None:
            del previous_state, task_instance

    problems = compat_components._check_listener_manager_scope(
        CleanListener,
        this_scope=compat_components._CORE_LISTENER_SPEC_MODULES,
        other_scope=("pytest_airflow_in_a_box._no_such_module",),
        code="listener-core-manager-only",
        other_manager="airflow.sdk.listener",
    )

    assert tuple(problems) == ()


def test_listener_hookspecs_skips_an_unimportable_module() -> None:
    """Degrade conservatively rather than raise when a hookspec module cannot import.

    These listener checks deliberately validate against the light, non-raising
    `installed_family()` instead of a certified release, so a module renamed or removed
    on an uncertified installed release must not raise out of `check_component` --
    mirroring every other capability probe in `_compat/`.
    """

    specs = compat_components._listener_hookspecs(
        ("airflow._shared.listeners.spec.lifecycle", "pytest_airflow_in_a_box._no_such_module")
    )

    assert "on_starting" in specs


def test_timetable_serialize_pair_incomplete_ignores_airflows_own_override() -> None:
    """Do not flag a subclass of Airflow's own timetables for an Airflow-authored gap.

    `airflow.timetables.simple.NullTimetable` redefines `deserialize` identically to the
    Protocol default and inherits `serialize` -- a complete, correct pair Airflow itself
    ships. Subclassing it must not surface that as a user gap.
    """

    from airflow.timetables.simple import NullTimetable

    class MyNullTimetable(NullTimetable):
        """Trivial subclass adding no serialization behavior of its own."""

    assert (
        tuple(compat_components._check_timetable_serialize_pair_incomplete(MyNullTimetable)) == ()
    )


def test_timetable_serialize_pair_incomplete_still_flags_a_real_user_gap() -> None:
    """Flag a user override that only patches `serialize`, even over an Airflow base."""

    from airflow.timetables.simple import NullTimetable

    class BadTimetable(NullTimetable):
        """Add custom serialized state without teaching `deserialize` to read it back."""

        def serialize(self) -> dict[str, object]:
            return {"custom": "state"}

    (problem,) = compat_components._check_timetable_serialize_pair_incomplete(BadTimetable)

    assert problem.code == "timetable-serialize-pair-incomplete"


def test_timetable_missing_protocol_method_requires_partition_hooks_when_partitioned() -> None:
    """Require the partition hooks only when the checked timetable is partitioned."""

    from airflow.timetables.base import DataInterval, Timetable

    class PartitionedTimetable(Timetable):
        """Set `partitioned = True` without overriding either partition hook."""

        partitioned = True

        def infer_manual_data_interval(self, *, run_after: Any) -> DataInterval:
            return DataInterval.exact(run_after)

        def next_dagrun_info(
            self, *, last_automated_data_interval: object, restriction: object
        ) -> None:
            del last_automated_data_interval, restriction
            return

    class UnpartitionedTimetable(PartitionedTimetable):
        """The same shape, but not partitioned -- the partition hooks are not required."""

        partitioned = False

    partitioned_problems = {
        problem.message
        for problem in compat_components._check_timetable_missing_protocol_method(
            PartitionedTimetable
        )
    }
    unpartitioned_problems = tuple(
        compat_components._check_timetable_missing_protocol_method(UnpartitionedTimetable)
    )

    assert any("get_partition_mapper" in message for message in partitioned_problems)
    assert any("iter_partition_dagrun_infos" in message for message in partitioned_problems)
    assert unpartitioned_problems == ()


def test_timetable_serialize_not_json_rejects_a_non_dict_payload() -> None:
    """Flag a `serialize()` returning a JSON-serializable value that is not a dict."""

    class ListSerializeTimetable:
        def serialize(self) -> list[int]:
            return [1, 2, 3]

    (problem,) = compat_components._check_timetable_serialize_not_json(ListSerializeTimetable())

    assert problem.code == "timetable-serialize-not-json"
    assert "dict" in problem.message
