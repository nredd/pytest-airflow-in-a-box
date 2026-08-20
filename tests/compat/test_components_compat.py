"""Test component-check registry invariants and version/family branching."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from importlib import metadata
from pathlib import Path
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


@pytest.fixture(autouse=True)
def _reset_provider_distribution_index() -> Iterator[None]:
    """Isolate the process-local provider distribution index cache.

    `_provider_owning_distribution` caches its `importlib.metadata.distributions()`
    scan for the whole process; a test that monkeypatches `metadata.distributions`
    without resetting this cache first would either see a stale result from an earlier
    test or (worse) populate the *real* environment's index, which then leaks into
    every later test expecting to see its own fake one. Resets both before and after:
    before, so no earlier test's fake index leaks in; after, so a fake index built here
    (from a `metadata.distributions` monkeypatch already undone by the time the next
    test's own `before` reset runs, if pytest ever changed fixture teardown ordering)
    never survives to be read by a test that never expected to see it.
    """

    compat_components._reset_provider_distribution_index_for_testing()
    yield
    compat_components._reset_provider_distribution_index_for_testing()


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
    already-empty result for an unrelated reason. `core_manager_only`/`sdk_manager_only`
    additionally depend on whether a second listener manager exists on the *running*
    release at all (`AirflowCapabilities.sdk_listener_manager_available`), not just on
    family -- on 3.1.x there is only one manager, registering every hookspec directly, so
    neither has anything to report there regardless of family, and the "fires on real
    unpatched 3.x" precondition below is skipped for whichever of the two that applies to
    on whatever release this test actually runs under.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake 2.x family.
        checker_name: str naming the module-level checker function under test.
    """

    from airflow.listeners import hookimpl

    from pytest_airflow_in_a_box._compat import resolve_capabilities

    checker = getattr(compat_components, checker_name)

    # A component carrying every offending shape at once: a stale attribute and an
    # unenforced missing override for the executor checks, an unmatched hookimpl name,
    # an unknown hookimpl argument, and a core-only hookspec for the listener checks.
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

    # No real hookspec exists only in the SDK manager today (its hookspec set is a
    # strict subset of the core one on every release that has one at all), so
    # `sdk_manager_only` never fires on real data and is always exempt here.
    # `core_manager_only` fires on any release with a genuine second manager
    # (`sdk_listener_manager_available`); on the one release line without it (3.1.x) it
    # is exempt too, for the same structural reason `sdk_manager_only` always is.
    sdk_manager_exists = resolve_capabilities().sdk_listener_manager_available
    always_exempt = checker_name == "_check_listener_sdk_manager_only"
    conditionally_exempt = (
        checker_name == "_check_listener_core_manager_only" and not sdk_manager_exists
    )
    if not (always_exempt or conditionally_exempt):
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
    mirroring every other capability probe in `_compat/`. Uses
    `airflow.listeners.spec.dagrun` as the real module: unlike `lifecycle`/`taskinstance`,
    it was never touched by the 3.2 `_shared` split and resolves identically on every
    certified 3.x release.
    """

    specs = compat_components._listener_hookspecs(
        ("airflow.listeners.spec.dagrun", "pytest_airflow_in_a_box._no_such_module")
    )

    assert "on_dag_run_success" in specs


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


# ---------------------------------------------------------------------------
# Phase 1b: xcom, weight-strategy, notifier, secrets-backend, policy, plugin,
# and provider checks and classifiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "checker_name",
    [
        "_check_xcom_orm_deserialize_removed",
        "_check_xcom_backend_signature",
        "_check_weight_strategy_abstract",
        "_check_weight_strategy_hash_of_none",
        "_check_notifier_missing_notify",
        "_check_notifier_template_fields_unresolvable",
        "_check_secrets_backend_raises_on_miss",
        "_check_policy_unknown_hookspec",
        "_check_policy_argument_name_mismatch",
        "_check_plugin_name_missing",
        "_check_provider_info_schema",
        "_check_provider_package_name_mismatch",
        "_check_provider_no_entry_point",
    ],
)
def test_phase_1b_checks_are_inapplicable_on_2x(
    monkeypatch: pytest.MonkeyPatch, checker_name: str
) -> None:
    """Report nothing on 2.x for every check phase 1b added.

    Every phase 1b checker gates on `installed_family() is not AirflowFamily.V3` as its
    first line, before touching the checked component at all -- so a bare `object()`
    exercises the gate itself without needing a kind-shaped fixture.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake 2.x family.
        checker_name: str naming the module-level checker function under test.
    """

    monkeypatch.setattr(compat_components, "installed_family", lambda: AirflowFamily.V2)
    checker = getattr(compat_components, checker_name)

    assert tuple(checker(object())) == ()


@pytest.mark.parametrize(
    "classifier_name",
    [
        "_is_xcom",
        "_is_weight_strategy",
        "_is_notifier",
        "_is_secrets_backend",
        "_is_policy",
        "_is_plugin",
        "_is_provider",
    ],
)
def test_phase_1b_classifiers_are_false_on_2x(
    monkeypatch: pytest.MonkeyPatch, classifier_name: str
) -> None:
    """Report False on 2.x for every classifier phase 1b added, importing nothing.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake 2.x family.
        classifier_name: str naming the module-level classifier function under test.
    """

    monkeypatch.setattr(compat_components, "installed_family", lambda: AirflowFamily.V2)
    classifier = getattr(compat_components, classifier_name)

    assert classifier(object()) is False


def test_xcom_backend_signature_checks_serialize_and_deserialize_independently() -> None:
    """Check each override independently: one broken does not hide the other's status."""

    from airflow.sdk.bases.xcom import BaseXCom

    class OnlySerializeOverridden(BaseXCom):
        @staticmethod
        def serialize_value(
            value: Any,
            *,
            key: Any = None,
            task_id: Any = None,
            dag_id: Any = None,
            run_id: Any = None,
            map_index: Any = None,
        ) -> str:
            del value, key, task_id, dag_id, run_id, map_index
            return "ok"

    class OnlyDeserializeOverridden(BaseXCom):
        @staticmethod
        def deserialize_value(result: Any) -> Any:
            del result
            return None

    def _broken_deserialize_value(result: Any, extra: Any) -> Any:
        del result, extra

    # Built via `type()` rather than a `class` statement: a `class BrokenDeserializeOnly
    # (BaseXCom)` with this exact shape is a genuine Liskov substitution violation (an
    # extra required parameter `deserialize_value` cannot actually accept), which `ty`
    # correctly refuses to let a real subclass declare. `type()` builds the identical
    # runtime object -- what `_check_xcom_backend_signature` inspects -- without going
    # through `ty`'s override check, exactly the way `test_executor_flag_wrong_type_*`
    # already builds deliberately-wrong-shaped fakes elsewhere in this file.
    BrokenDeserializeOnly = type(
        "BrokenDeserializeOnly",
        (BaseXCom,),
        {"deserialize_value": staticmethod(_broken_deserialize_value)},
    )

    assert tuple(compat_components._check_xcom_backend_signature(OnlySerializeOverridden)) == ()
    assert tuple(compat_components._check_xcom_backend_signature(OnlyDeserializeOverridden)) == ()
    (problem,) = compat_components._check_xcom_backend_signature(BrokenDeserializeOnly)
    assert problem.code == "xcom-backend-signature"
    assert "deserialize_value" in problem.message


def test_weight_strategy_abstract_names_the_missing_methods() -> None:
    """Name every unresolved abstract method in the reported problem."""

    from airflow.task.priority_strategy import PriorityWeightStrategy

    class StillAbstract(PriorityWeightStrategy):
        pass

    (problem,) = compat_components._check_weight_strategy_abstract(StillAbstract)

    assert problem.code == "weight-strategy-abstract"
    assert "get_weight" in problem.message


def test_weight_strategy_hash_of_none_catches_a_user_eq_without_hash() -> None:
    """Flag a user subclass whose own `__eq__` silently nulls its own `__hash__`.

    Regression test: Python's rule that defining `__eq__` without `__hash__` sets
    `__hash__` to `None` fires on *any* class body that does it, not just
    `PriorityWeightStrategy` itself. A user subclass overriding `__eq__` for value
    semantics -- a completely ordinary thing to do -- silently becomes unhashable the
    same way. Checking "who defines `__hash__`" alone missed this: `_defining_class`
    finds the user's own subclass (not `PriorityWeightStrategy`, not `airflow.*`) as the
    definer of the automatically-inserted `None`, which a naive "was it user overridden"
    reading would wrongly treat as a deliberate, working override. Comparing the
    effective `__hash__` by identity against the installed base's own catches it
    correctly instead.
    """

    from airflow.task.priority_strategy import PriorityWeightStrategy

    class EqWithoutHash(PriorityWeightStrategy):
        def get_weight(self, ti: object) -> int:
            del ti
            return 1

        def __eq__(self, other: object) -> bool:
            return isinstance(other, EqWithoutHash)

    assert EqWithoutHash.__hash__ is None
    with pytest.raises(TypeError, match="unhashable"):
        hash(EqWithoutHash())

    (problem,) = compat_components._check_weight_strategy_hash_of_none(EqWithoutHash)

    assert problem.code == "weight-strategy-hash-of-none"
    assert "unhashable" in problem.hint


def test_weight_strategy_hash_of_none_accepts_a_genuine_override() -> None:
    """Do not flag a subclass whose effective `__hash__` is a real, different callable."""

    from airflow.task.priority_strategy import PriorityWeightStrategy

    class RealOverride(PriorityWeightStrategy):
        def get_weight(self, ti: object) -> int:
            del ti
            return 1

        def __hash__(self) -> int:
            return 12345

    assert tuple(compat_components._check_weight_strategy_hash_of_none(RealOverride)) == ()


def test_notifier_missing_notify_skips_an_airflow_shipped_override() -> None:
    """Do not flag a subclass of an Airflow-authored notifier that already implements it.

    Mirrors `test_timetable_serialize_pair_incomplete_ignores_airflows_own_override`:
    an override inherited from Airflow's own shipped notifiers must not be attributed
    to the user.
    """

    from airflow.providers.smtp.notifications.smtp import SmtpNotifier

    class MySmtpNotifier(SmtpNotifier):
        """Trivial subclass adding no notify behavior of its own."""

    assert tuple(compat_components._check_notifier_missing_notify(MySmtpNotifier)) == ()


def test_notifier_template_fields_unresolvable_flags_a_bare_string() -> None:
    """Flag a bare-string `template_fields`, not iterate it character by character.

    Regression test: `str` is technically a `Sequence[str]`, so nothing structurally
    rejects `template_fields = "message"` -- a one-character typo away from the correct
    `("message",)`. Iterating it directly would check `hasattr` for each of `"m"`,
    `"e"`, `"s"`, ... instead of reporting the real mistake once, clearly.
    """

    from airflow.sdk.bases.notifier import BaseNotifier

    class StringTemplateFields(BaseNotifier):
        template_fields = "message"

        def notify(self, context: object) -> None:
            del context

    (problem,) = compat_components._check_notifier_template_fields_unresolvable(
        StringTemplateFields()
    )

    assert problem.code == "notifier-template-fields-unresolvable"
    assert "bare string" in problem.message


def test_notifier_template_fields_unresolvable_flags_a_non_iterable_value() -> None:
    """Flag a non-iterable `template_fields` instead of raising `TypeError`.

    Regression test: `BaseNotifier._update_context` iterates `template_fields` directly
    with no type check, so `template_fields = None` (or any other non-iterable) raises
    `TypeError` the moment a real notifier fires -- and, before this fix, out of
    `check_component` itself, which must never raise on the checked component.
    """

    from airflow.sdk.bases.notifier import BaseNotifier

    class NoneTemplateFields(BaseNotifier):
        template_fields = None

        def notify(self, context: object) -> None:
            del context

    (problem,) = compat_components._check_notifier_template_fields_unresolvable(
        NoneTemplateFields()
    )

    assert problem.code == "notifier-template-fields-unresolvable"
    assert "not iterable" in problem.message


def test_secrets_backend_raises_on_miss_skips_a_class_defining_no_getters_at_all() -> None:
    """Report nothing for a duck-typed, forced-kind class defining none of the getters.

    `_defining_class` returns None when no class in the MRO defines the name at all --
    unreachable for a real `BaseSecretsBackend` subclass, since the base always defines
    all four, but reachable for a forced-kind component that duck-types as this kind
    without actually subclassing it, mirroring `_NotAnExecutor` in `test_components.py`.
    """

    class NotASecretsBackend:
        pass

    assert tuple(compat_components._check_secrets_backend_raises_on_miss(NotASecretsBackend)) == ()


def test_secrets_backend_raises_on_miss_catches_a_get_conn_value_only_override() -> None:
    """Flag a backend overriding only `get_conn_value`, not `get_connection`.

    Regression test: `get_conn_value`'s own docstring names it the *default* override
    point ("If the client your secrets backend uses already returns a python dict, you
    should override `get_connection` instead"), and Airflow's own shipped
    `EnvironmentVariablesBackend` overrides it, not `get_connection`. Excluding
    `get_conn_value` from `_SECRETS_BACKEND_GETTERS` would miss the more commonly
    overridden of the two connection getters entirely.
    """

    from airflow.secrets.base_secrets import BaseSecretsBackend

    class VaultishBackend(BaseSecretsBackend):
        def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str:
            del team_name
            raise KeyError(conn_id)

    (problem,) = compat_components._check_secrets_backend_raises_on_miss(VaultishBackend)

    assert problem.code == "secrets-backend-raises-on-miss"
    assert "get_conn_value" in problem.message


def test_annotation_allows_none_recognizes_a_real_pep604_union_object() -> None:
    """Recognize a real (already-evaluated) `X | None` annotation object."""

    assert compat_components._annotation_allows_none(str | None) is True


def test_annotation_allows_none_recognizes_optional_and_union_spellings_too() -> None:
    """Recognize the pre-PEP604 `typing.Optional`/`typing.Union` spellings identically.

    Regression test: `str(typing.Optional[str])` renders as `'typing.Optional[str]'`,
    containing no literal `None` at all, unlike `str(str | None)` which does -- a plain
    substring search over `str(annotation)` would miss these two spellings even though
    `typing.get_args()` normalizes all three identically. Built via `getattr` rather
    than the `Optional[...]`/`Union[...]` subscript syntax directly, which names a
    spelling this repo's own style no longer writes.
    """

    typing_optional = __import__("typing").Optional
    typing_union = __import__("typing").Union

    assert compat_components._annotation_allows_none(typing_optional[str]) is True
    assert compat_components._annotation_allows_none(typing_union[str, None]) is True


def test_annotation_allows_none_rejects_a_non_nullable_real_object() -> None:
    """Reject a real annotation object that does not admit `None`."""

    assert compat_components._annotation_allows_none(str) is False


@pytest.mark.parametrize("annotation", [None, type(None)])
def test_annotation_allows_none_accepts_a_bare_none_annotation(annotation: object) -> None:
    """Recognize a bare `-> None` real object annotation as nullable.

    Regression test: `get_args(None)` is `()`, since `None` is not itself a
    parametrized generic -- the structural check alone would wrongly reject the one
    annotation that is trivially, entirely `None`, so it is special-cased ahead of
    `get_args`. `inspect.signature` reports a bare `-> None` return annotation as
    either the real object `None` or, equivalently, `type(None)`, depending on the
    Python version.
    """

    assert compat_components._annotation_allows_none(annotation) is True


@pytest.mark.parametrize(
    "annotation",
    ["str | None", "Optional[str]", "typing.Union[str, None]"],
)
def test_annotation_allows_none_recognizes_every_string_spelling(annotation: str) -> None:
    """Recognize the same three spellings when reported as an unevaluated string.

    `inspect.signature` reports a string annotation instead of a real object when the
    defining module uses `from __future__ import annotations` -- the common case for
    this codebase's own style, and plausible for any user module too.
    """

    assert compat_components._annotation_allows_none(annotation) is True


def test_annotation_allows_none_rejects_a_non_nullable_string() -> None:
    """Reject a string annotation that does not admit `None`."""

    assert compat_components._annotation_allows_none("str") is False


def test_secrets_backend_raises_on_miss_accepts_a_real_optional_object_annotation() -> None:
    """Do not flag an override annotated with a real (non-string) `Optional[X]` object.

    Assigns `__annotations__` directly rather than writing `Optional[str]` in this
    module's own (postponed-evaluation) source, so `inspect.signature` reports the real
    `typing.Optional[str]` object instead of a string -- exercising the
    `_annotation_allows_none` branch that inspects a live annotation object instead of
    text.
    """

    from airflow.secrets.base_secrets import BaseSecretsBackend

    class RealOptionalAnnotation(BaseSecretsBackend):
        def get_variable(self, key: str, team_name: str | None = None) -> str | None:
            del key, team_name
            return None

    typing_optional = __import__("typing").Optional
    RealOptionalAnnotation.get_variable.__annotations__["return"] = typing_optional[str]

    assert (
        tuple(compat_components._check_secrets_backend_raises_on_miss(RealOptionalAnnotation))
        == ()
    )


def test_secrets_backend_raises_on_miss_skips_unannotated_overrides() -> None:
    """Do not flag an override that carries no return annotation at all."""

    from airflow.secrets.base_secrets import BaseSecretsBackend

    class Unannotated(BaseSecretsBackend):
        def get_variable(self, key, team_name=None):
            del key, team_name
            return

    assert tuple(compat_components._check_secrets_backend_raises_on_miss(Unannotated)) == ()


def test_secrets_backend_raises_on_miss_skips_an_airflow_shipped_override() -> None:
    """Do not flag a subclass of an Airflow-authored secrets backend for its own gap."""

    from airflow.secrets.metastore import MetastoreBackend

    class MyMetastoreBackend(MetastoreBackend):
        """Trivial subclass adding no getter overrides of its own."""

    assert tuple(compat_components._check_secrets_backend_raises_on_miss(MyMetastoreBackend)) == ()


def test_secrets_backend_raises_on_miss_flags_every_offending_getter() -> None:
    """Flag every offending getter, not just the first one found."""

    from airflow.secrets.base_secrets import BaseSecretsBackend

    def _broken_get_conn_value(self: Any, conn_id: str, team_name: str | None = None) -> str:
        del self, team_name
        raise KeyError(conn_id)

    def _broken_get_connection(self: Any, conn_id: str, team_name: str | None = None) -> str:
        del self, team_name
        raise KeyError(conn_id)

    def _broken_get_variable(self: Any, key: str, team_name: str | None = None) -> str:
        del self, team_name
        raise KeyError(key)

    def _broken_get_config(self: Any, key: str) -> str:
        del self
        raise KeyError(key)

    # Built via `type()`, not a `class` statement: real annotated overrides this shape
    # trip `ty`'s own return-type reasoning in ways unrelated to what this test checks
    # (whether `_check_secrets_backend_raises_on_miss` reads the annotation), so the
    # fixture is assembled the same dynamic way `BrokenDeserializeOnly` above is.
    AllBroken = type(
        "AllBroken",
        (BaseSecretsBackend,),
        {
            "get_conn_value": _broken_get_conn_value,
            "get_connection": _broken_get_connection,
            "get_variable": _broken_get_variable,
            "get_config": _broken_get_config,
        },
    )

    codes = {p.code for p in compat_components._check_secrets_backend_raises_on_miss(AllBroken)}
    names = {
        name
        for p in compat_components._check_secrets_backend_raises_on_miss(AllBroken)
        for name in compat_components._SECRETS_BACKEND_GETTERS
        if name in p.message
    }
    assert codes == {"secrets-backend-raises-on-miss"}
    assert names == set(compat_components._SECRETS_BACKEND_GETTERS)


def test_policy_hookspecs_skips_an_unimportable_module() -> None:
    """Degrade conservatively rather than raise when the policy module cannot import."""

    specs = compat_components._policy_hookspecs(
        ("airflow.policies", "pytest_airflow_in_a_box._no_such_module")
    )

    assert "task_instance_mutation_hook" in specs


def test_policy_checks_report_nothing_without_any_hookimpl() -> None:
    """Report nothing for a component that defines no policy `@hookimpl`-marked method."""

    class NotAPolicy:
        def task_instance_mutation_hook(self, task_instance: object) -> None:
            del task_instance

    for checker_name in ("_check_policy_unknown_hookspec", "_check_policy_argument_name_mismatch"):
        checker = getattr(compat_components, checker_name)
        assert tuple(checker(NotAPolicy)) == ()


def test_is_plugin_rejects_a_same_named_class_from_the_wrong_module() -> None:
    """Reject a plugin subclassed from a look-alike `AirflowPlugin` in the wrong module.

    This is the exact "wrong re-export" bug the issue describes: a class that merely
    shares the name `AirflowPlugin` but was not defined anywhere under
    `airflow...plugins_manager` must not duck-type as a real plugin base.
    """

    class AirflowPlugin:
        """Local stand-in, not the real Airflow base -- `__module__` is this test module."""

        name = "impostor"

    class LooksLikeAPlugin(AirflowPlugin):
        pass

    assert compat_components._is_plugin(LooksLikeAPlugin) is False


def test_is_plugin_rejects_the_bare_airflow_plugin_class_itself() -> None:
    """Reject `AirflowPlugin` itself, not just its subclasses."""

    from airflow.plugins_manager import AirflowPlugin

    assert compat_components._is_plugin(AirflowPlugin) is False


class _FakeDistribution(metadata.Distribution):
    """Minimal fake `importlib.metadata.Distribution` for direct unit testing.

    `Distribution.name` and `.files` are read-only properties on the real ABC, so the
    fake values live behind a leading-underscore attribute and are exposed through
    properties of the same names, rather than plain instance attributes that would
    shadow (and, per `ty`, illegally reassign) the base's own. `read_text` is one of
    only two real abstract methods (`locate_file` is the other) and so must exist, but
    nothing under test reads distribution metadata files by name, only `.pth` file
    content reached through `files`/`locate_file`.

    Parameters:
        name: str naming the fake distribution.
        files: tuple[str, ...] | None containing fake `PackagePath`-like relative paths.
        root: pathlib.Path | None resolving each fake file when `locate_file` is called.
    """

    def __init__(
        self,
        name: str,
        *,
        files: tuple[str, ...] | None = None,
        root: Any = None,
    ) -> None:
        self._name = name
        self._files = files
        self._root = root

    @property
    def name(self) -> str:
        return self._name

    @property
    def files(self) -> Any:
        return self._files

    def read_text(self, filename: str) -> str | None:
        del filename
        return None

    def locate_file(self, path: Any) -> Any:
        assert self._root is not None
        return self._root / path


def test_distribution_editable_roots_returns_empty_without_any_pth_file() -> None:
    """Return no roots for a distribution recording no `.pth` file at all."""

    dist = _FakeDistribution("plain-dist", files=("some/file.py",), root=Path("/nonexistent"))

    assert compat_components._distribution_editable_roots(dist) == ()


def test_distribution_editable_roots_returns_empty_without_any_files() -> None:
    """Tolerate a distribution whose `files` is None rather than raising."""

    dist = _FakeDistribution("no-files-dist", files=None)

    assert compat_components._distribution_editable_roots(dist) == ()


def test_distribution_editable_roots_skips_an_unreadable_pth_file(tmp_path: Path) -> None:
    """Return no roots rather than raise when the recorded `.pth` file cannot be read."""

    dist = _FakeDistribution("broken-dist", files=("missing.pth",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == ()


def test_distribution_editable_roots_ignores_a_non_pth_file(tmp_path: Path) -> None:
    """Ignore a recorded file that is not itself a `.pth` file."""

    (tmp_path / "module.py").write_text(str(tmp_path))
    dist = _FakeDistribution("py-file-dist", files=("module.py",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == ()


def test_distribution_editable_roots_skips_comment_blank_and_import_lines(
    tmp_path: Path,
) -> None:
    """Skip `#`-comment, blank, and `import`-hook lines, matching Python's `site` module."""

    real_dir = tmp_path / "real"
    real_dir.mkdir()
    (tmp_path / "pkg.pth").write_text(f"# a comment\n\nimport something_installed\n{real_dir}\n")
    dist = _FakeDistribution("mixed-dist", files=("pkg.pth",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == (real_dir.resolve(),)


def test_distribution_editable_roots_skips_a_nonexistent_directory_line(tmp_path: Path) -> None:
    """Skip a `.pth` line naming a directory that does not actually exist."""

    (tmp_path / "pkg.pth").write_text(str(tmp_path / "does-not-exist") + "\n")
    dist = _FakeDistribution("dangling-dist", files=("pkg.pth",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == ()


def test_distribution_editable_roots_skips_an_unresolvable_line(tmp_path: Path) -> None:
    """Skip a `.pth` line that cannot even be resolved to a path, rather than raise.

    An embedded null character is a real, portable way to make `Path.resolve()` itself
    raise `ValueError` -- no monkeypatching needed to exercise this defensive branch.
    """

    (tmp_path / "pkg.pth").write_text("/tmp/\x00bad\n")
    dist = _FakeDistribution("null-byte-dist", files=("pkg.pth",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == ()


def test_distribution_editable_roots_resolves_every_real_directory_line(tmp_path: Path) -> None:
    """Resolve every real directory line, in order, from a multi-line `.pth` file."""

    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (tmp_path / "pkg.pth").write_text(f"{first}\n{second}\n")
    dist = _FakeDistribution("multi-root-dist", files=("pkg.pth",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == (
        first.resolve(),
        second.resolve(),
    )


def _provider_component(module_name: str) -> object:
    """Build a bare object exposing only a fake `__module__`, for tracing tests.

    Parameters:
        module_name: str to expose as the fake component's `__module__`.

    Returns:
        object containing a type whose `__module__` is exactly `module_name`.
    """

    return type("FakeProviderCallable", (), {"__module__": module_name})


def test_provider_owning_distribution_returns_none_without_a_module() -> None:
    """Return None for a component exposing no `__module__` at all."""

    component = type("NoModule", (), {"__module__": None})

    assert compat_components._provider_owning_distribution(component) is None


def test_provider_owning_distribution_returns_none_for_an_unimported_module() -> None:
    """Return None when the named module is not (or no longer) in `sys.modules`."""

    component = _provider_component("pytest_airflow_in_a_box._no_such_module_at_all")

    assert compat_components._provider_owning_distribution(component) is None


def test_provider_owning_distribution_returns_none_for_a_fileless_module() -> None:
    """Return None for a real, imported module that carries no `__file__` (e.g. `sys`)."""

    assert not hasattr(sys, "__file__")
    component = _provider_component("sys")

    assert compat_components._provider_owning_distribution(component) is None


def test_provider_owning_distribution_returns_none_with_no_installed_distributions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return None when no installed distribution's manifest or editable root matches."""

    monkeypatch.setattr(compat_components.metadata, "distributions", lambda: iter([]))
    component = _provider_component(__name__)

    assert compat_components._provider_owning_distribution(component) is None


def test_provider_owning_distribution_skips_a_distribution_with_no_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tolerate a distribution whose `files` is None rather than raising."""

    dist = _FakeDistribution("no-files-dist", files=None)
    monkeypatch.setattr(compat_components.metadata, "distributions", lambda: iter([dist]))
    component = _provider_component(__name__)

    assert compat_components._provider_owning_distribution(component) is None


def test_provider_owning_distribution_matches_via_file_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attribute a module to the distribution whose recorded file matches it exactly."""

    this_module_file = sys.modules[__name__].__file__
    assert this_module_file is not None
    module_file = Path(this_module_file).resolve()
    dist = _FakeDistribution("manifest-dist", files=(module_file.name,), root=module_file.parent)
    monkeypatch.setattr(compat_components.metadata, "distributions", lambda: iter([dist]))
    component = _provider_component(__name__)

    assert compat_components._provider_owning_distribution(component) == "manifest-dist"


def _fake_module_at(monkeypatch: pytest.MonkeyPatch, module_name: str, file_path: Path) -> object:
    """Register a synthetic module in `sys.modules` claiming a given file path.

    Uses `monkeypatch.setitem` rather than mutating any real module's `__file__`, so
    cleanup is automatic and nothing about a genuinely-imported module is disturbed.

    Parameters:
        monkeypatch: pytest.MonkeyPatch registering the fake module for cleanup.
        module_name: str naming the fake module.
        file_path: pathlib.Path the fake module claims as its `__file__`.

    Returns:
        object containing a fake `get_provider_info`-shaped component whose
        `__module__` resolves to the newly registered fake module.
    """

    import types

    fake_module = types.ModuleType(module_name)
    fake_module.__file__ = str(file_path)
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    return _provider_component(module_name)


def test_provider_owning_distribution_matches_via_a_precise_editable_pth_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Attribute a module under an editable `.pth` root, precisely -- not the whole project.

    Regression test for over-attribution: a naive "the whole project root is exposed"
    reading would also match a sibling directory (`tests/`, `docs/`, another package in
    a workspace) that the `.pth` file's own content does not actually add to
    `sys.path`.
    """

    src_root = tmp_path / "src"
    src_root.mkdir()
    sibling = tmp_path / "tests"
    sibling.mkdir()
    (tmp_path / "pkg.pth").write_text(str(src_root) + "\n")
    dist = _FakeDistribution("editable-dist", files=("pkg.pth",), root=tmp_path)
    monkeypatch.setattr(compat_components.metadata, "distributions", lambda: iter([dist]))

    inside = _fake_module_at(monkeypatch, "fake_inside_module", src_root / "mymodule.py")
    assert compat_components._provider_owning_distribution(inside) == "editable-dist"

    outside = _fake_module_at(monkeypatch, "fake_outside_module", sibling / "test_mymodule.py")
    assert compat_components._provider_owning_distribution(outside) is None


def test_provider_owning_distribution_prefers_the_most_specific_editable_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Prefer the deepest matching editable root when more than one contains a file.

    Regression test for workspace nondeterminism: an outer distribution's `.pth` root
    and an inner (nested) distribution's own, more specific `.pth` root can both
    legitimately contain the same file; the more specific one identifies the real owner.
    """

    outer_root = tmp_path
    inner_root = tmp_path / "providers" / "foo" / "src"
    inner_root.mkdir(parents=True)
    (tmp_path / "outer.pth").write_text(str(outer_root) + "\n")
    (tmp_path / "inner.pth").write_text(str(inner_root) + "\n")
    outer_dist = _FakeDistribution("my-monorepo", files=("outer.pth",), root=tmp_path)
    inner_dist = _FakeDistribution(
        "apache-airflow-providers-foo", files=("inner.pth",), root=tmp_path
    )
    monkeypatch.setattr(
        compat_components.metadata, "distributions", lambda: iter([outer_dist, inner_dist])
    )
    component = _fake_module_at(
        monkeypatch, "fake_nested_module", inner_root / "get_provider_info.py"
    )

    assert compat_components._provider_owning_distribution(component) == (
        "apache-airflow-providers-foo"
    )


def test_provider_owning_distribution_caches_across_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build the distribution index once and reuse it, not once per call."""

    calls = 0
    real_distributions = compat_components.metadata.distributions

    def counting_distributions() -> object:
        nonlocal calls
        calls += 1
        return real_distributions()

    monkeypatch.setattr(compat_components.metadata, "distributions", counting_distributions)
    component = _provider_component(__name__)

    compat_components._provider_owning_distribution(component)
    compat_components._provider_owning_distribution(component)

    assert calls == 1


def test_provider_owning_distribution_resolves_a_real_installed_provider() -> None:
    """Attribute the real, non-editable `apache-airflow-providers-sqlite` for real.

    Pins the resolution result directly rather than only inferring it from
    `check_component(...).ok`: `provider-package-name-mismatch`/`provider-no-entry-point`
    both silently report nothing when `_provider_owning_distribution` cannot attribute a
    callable, so a `check_component(real_get_provider_info).ok is True` assertion alone
    cannot distinguish "genuinely validated and passed" from "resolution silently failed
    and both checks no-opped" -- a regression in file-manifest matching would otherwise
    downgrade `test_check_component_reports_no_problems_for_a_clean_provider` to a
    vacuous pass instead of failing it.
    """

    from airflow.providers.sqlite.get_provider_info import get_provider_info

    resolved = compat_components._provider_owning_distribution(get_provider_info)

    assert resolved == "apache-airflow-providers-sqlite"


def test_check_provider_info_schema_returns_nothing_for_a_class() -> None:
    """Skip a bare class: a `get_provider_info`-shaped callable is never a class."""

    assert tuple(compat_components._check_provider_info_schema(object)) == ()


def test_check_provider_info_schema_reports_nothing_when_the_schema_cannot_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blame the environment, not the component, when the shipped schema is unreadable.

    Mirrors `_check_executor_flag_wrong_type`'s precedent: a broken environment fact
    unrelated to the checked component must not be attributed to it.
    """

    def broken_files(_package: str) -> Any:
        raise OSError("schema file missing")

    monkeypatch.setattr("importlib.resources.files", broken_files)

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "x", "name": "x", "description": "x"}

    assert tuple(compat_components._check_provider_info_schema(get_provider_info)) == ()


def test_check_provider_info_schema_reports_nothing_when_jsonschema_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never raise `ImportError` just because `jsonschema` is not actually installed.

    Regression test: `jsonschema` is not a direct dependency of this project -- it rides
    in only because the installed `apache-airflow-core` currently requires it, which
    `installed_family()` never verifies (it classifies from `apache-airflow-core`'s own
    dist-info metadata alone, not its dependencies' importability). A pruned or
    `--no-deps` environment can have Airflow's metadata present without `jsonschema`
    actually installed; a checker must never raise on the checked component for a
    reason that has nothing to do with it. `sys.modules[name] = None` is the standard
    way to simulate "this module cannot be imported" without needing it genuinely
    uninstalled.
    """

    monkeypatch.setitem(sys.modules, "jsonschema", None)

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "x", "name": "x", "description": "x"}

    assert tuple(compat_components._check_provider_info_schema(get_provider_info)) == ()


def test_check_provider_package_name_mismatch_returns_nothing_for_a_class() -> None:
    """Skip a bare class for the package-name-mismatch check too."""

    assert tuple(compat_components._check_provider_package_name_mismatch(object)) == ()


def test_check_provider_package_name_mismatch_fires_on_a_genuine_disagreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flag a `package-name` that genuinely disagrees with the owning distribution.

    The positive case: no prior test exercised this check actually firing against a
    fake (rather than only an accident of the real environment), which would let a
    regression that always skips go unnoticed by every negative ("skips ...") test here.
    """

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "totally-wrong-name"}

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "My-Real-Dist"
    )

    (problem,) = compat_components._check_provider_package_name_mismatch(get_provider_info)

    assert problem.code == "provider-package-name-mismatch"
    assert "totally-wrong-name" in problem.message
    assert "my-real-dist" in problem.message


def test_check_provider_package_name_mismatch_skips_a_non_dict_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing when the callable returns something other than a dict.

    `provider-info-schema` already reports a non-dict return; this check only compares
    a `package-name` field, which a non-dict value cannot have.
    """

    def get_provider_info() -> list[int]:
        return [1, 2, 3]

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "some-dist"
    )

    assert tuple(compat_components._check_provider_package_name_mismatch(get_provider_info)) == ()


def test_check_provider_package_name_mismatch_skips_a_non_string_package_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing when `package-name` itself is present but not a string."""

    def get_provider_info() -> dict[str, object]:
        return {"package-name": 123}

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "some-dist"
    )

    assert tuple(compat_components._check_provider_package_name_mismatch(get_provider_info)) == ()


def test_check_provider_package_name_mismatch_skips_when_the_callable_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing when calling the component raises; the schema check owns that."""

    def get_provider_info() -> dict[str, str]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "some-dist"
    )

    assert tuple(compat_components._check_provider_package_name_mismatch(get_provider_info)) == ()


def test_check_provider_no_entry_point_returns_nothing_for_a_class() -> None:
    """Skip a bare class for the no-entry-point check too."""

    assert tuple(compat_components._check_provider_no_entry_point(object)) == ()


def test_check_provider_no_entry_point_accepts_a_registered_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report nothing when the owning distribution registers a matching entry point."""

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "my-provider"}

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "My-Provider"
    )
    fake_entry_point = SimpleNamespace(dist=SimpleNamespace(name="my-provider"))

    def fake_entry_points(*, group: str) -> list[SimpleNamespace]:
        del group
        return [fake_entry_point]

    monkeypatch.setattr(compat_components.metadata, "entry_points", fake_entry_points)

    assert tuple(compat_components._check_provider_no_entry_point(get_provider_info)) == ()


def test_check_provider_no_entry_point_tolerates_an_entry_point_with_no_dist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip an entry point whose `dist` is None rather than raising on `.name`."""

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "my-provider"}

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "my-provider"
    )
    fake_entry_point = SimpleNamespace(dist=None)

    def fake_entry_points(*, group: str) -> list[SimpleNamespace]:
        del group
        return [fake_entry_point]

    monkeypatch.setattr(compat_components.metadata, "entry_points", fake_entry_points)

    (problem,) = compat_components._check_provider_no_entry_point(get_provider_info)
    assert problem.code == "provider-no-entry-point"


@pytest.mark.parametrize(
    "checker_name",
    ["_check_provider_package_name_mismatch", "_check_provider_no_entry_point"],
)
def test_provider_checks_skip_when_no_distribution_owns_the_callable(
    monkeypatch: pytest.MonkeyPatch, checker_name: str
) -> None:
    """Report nothing when `_provider_owning_distribution` cannot attribute the callable.

    Parameters:
        monkeypatch: pytest.MonkeyPatch forcing an unresolvable owning distribution.
        checker_name: str naming the module-level checker function under test.
    """

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: None
    )

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "whatever"}

    checker = getattr(compat_components, checker_name)

    assert tuple(checker(get_provider_info)) == ()


def test_weight_strategy_hash_of_none_reflects_an_unhashable_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Describe the certified 3.1.x failure mode when the live base is unhashable.

    Monkeypatches `PriorityWeightStrategy.__hash__` directly rather than depending on
    which Airflow release the local gate happens to run under: on the installed 3.2+
    the real base already defines `__hash__` as `return hash(None)`, so this branch is
    otherwise only exercised for real on a 3.1.x compat leg.
    """

    from airflow.task.priority_strategy import PriorityWeightStrategy

    monkeypatch.setattr(PriorityWeightStrategy, "__hash__", None, raising=False)

    class NoHashOverride(PriorityWeightStrategy):
        def get_weight(self, ti: object) -> int:
            del ti
            return 1

    (problem,) = compat_components._check_weight_strategy_hash_of_none(NoHashOverride)

    assert "unhashable" in problem.hint


def test_provider_owning_distribution_skips_a_file_manifest_entry_that_cannot_locate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tolerate a distribution whose `locate_file` raises for one of its own entries."""

    class _RaisingDistribution(_FakeDistribution):
        def locate_file(self, path: Any) -> Any:
            del path
            raise RuntimeError("cannot locate")

    dist = _RaisingDistribution("raising-dist", files=("some/file.py",))
    monkeypatch.setattr(compat_components.metadata, "distributions", lambda: iter([dist]))
    component = _provider_component(__name__)

    assert compat_components._provider_owning_distribution(component) is None


def test_check_provider_info_schema_flags_a_callable_that_raises() -> None:
    """Flag a `get_provider_info` callable that raises instead of returning a dict."""

    def get_provider_info() -> dict[str, str]:
        raise RuntimeError("boom")

    (problem,) = compat_components._check_provider_info_schema(get_provider_info)

    assert problem.code == "provider-info-schema"
    assert "RuntimeError" in problem.message


# ---------------------------------------------------------------------------
# Regression coverage for review round 2: checkers/classifiers must never raise
# when their own Airflow submodule cannot import, no matter how pathological the
# checked component's own shape is.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("checker_name", "blocked_module"),
    [
        ("_check_xcom_backend_signature", "airflow.sdk.bases.xcom"),
        ("_check_weight_strategy_hash_of_none", "airflow.task.priority_strategy"),
        ("_check_notifier_missing_notify", "airflow.sdk.bases.notifier"),
        ("_check_secrets_backend_raises_on_miss", "airflow.secrets.base_secrets"),
    ],
)
def test_v3_checks_degrade_conservatively_when_their_airflow_submodule_is_unimportable(
    monkeypatch: pytest.MonkeyPatch, checker_name: str, blocked_module: str
) -> None:
    """Report nothing, never raise, when a checker's own Airflow submodule cannot import.

    `installed_family()` classifies from `apache-airflow-core`'s own dist-info metadata
    alone, never this submodule's actual importability (see
    `_provider_owning_distribution`'s docstring for the same distinction from the other
    direction) -- an uncertified installed release (a pre-release build, or a future
    release this project has not certified yet) that has moved or removed one of these
    internal submodules must degrade the same conservative way `_listener_hookspecs`
    already does for a listener hookspec module, not raise out of `check_component` for
    an environment reason that has nothing to do with the checked component.
    `monkeypatch.setitem(sys.modules, name, None)` is the standard way to make Python's
    own import system raise on a subsequent `import`/`from ... import ...` of `name`,
    regardless of whether it was already imported before this test ran.

    Parameters:
        monkeypatch: pytest.MonkeyPatch blocking the checker's own Airflow submodule.
        checker_name: str naming the module-level checker function under test.
        blocked_module: str naming the Airflow submodule this checker imports.
    """

    monkeypatch.setitem(sys.modules, blocked_module, None)
    checker = getattr(compat_components, checker_name)

    assert tuple(checker(object())) == ()


@pytest.mark.parametrize(
    ("classifier_name", "blocked_module"),
    [
        ("_is_xcom", "airflow.sdk.bases.xcom"),
        ("_is_weight_strategy", "airflow.task.priority_strategy"),
        ("_is_notifier", "airflow.sdk.bases.notifier"),
        ("_is_secrets_backend", "airflow.secrets.base_secrets"),
    ],
)
def test_v3_classifiers_degrade_conservatively_when_their_airflow_submodule_is_unimportable(
    monkeypatch: pytest.MonkeyPatch, classifier_name: str, blocked_module: str
) -> None:
    """Report False, never raise, when a classifier's own Airflow submodule cannot import.

    Parameters:
        monkeypatch: pytest.MonkeyPatch blocking the classifier's own Airflow submodule.
        classifier_name: str naming the module-level classifier function under test.
        blocked_module: str naming the Airflow submodule this classifier imports.
    """

    monkeypatch.setitem(sys.modules, blocked_module, None)
    classifier = getattr(compat_components, classifier_name)

    assert classifier(object()) is False


def test_policy_hookspecs_degrades_conservatively_when_airflow_policies_is_unimportable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty mapping, not raise, when `airflow.policies` cannot import.

    Calling the two policy checker functions end-to-end with `airflow.policies` blocked
    cannot reach this codepath: `_policy_hookimpls` (called first by both checkers)
    degrades to an empty tuple for the same reason, and both checkers return early on an
    empty `impls` before ever calling `_policy_hookspecs`. This unit-level call is the
    only way to actually exercise this function's own guard.
    """

    monkeypatch.setitem(sys.modules, "airflow.policies", None)

    assert compat_components._policy_hookspecs(("airflow.policies",)) == {}


def test_policy_hookimpls_degrades_conservatively_when_airflow_policies_is_unimportable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty tuple, not raise, when `airflow.policies` cannot import."""

    monkeypatch.setitem(sys.modules, "airflow.policies", None)

    class _SomeClass:
        pass

    assert compat_components._policy_hookimpls(_SomeClass) == ()


def test_is_policy_degrades_conservatively_when_airflow_policies_is_unimportable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return False, not raise, when `airflow.policies` cannot import."""

    monkeypatch.setitem(sys.modules, "airflow.policies", None)

    assert compat_components._is_policy(object()) is False


def test_notifier_template_fields_unresolvable_reports_a_raising_property() -> None:
    """Report, not raise, when `template_fields` itself raises on access.

    `getattr(component, "template_fields", ())` only substitutes the default for
    `AttributeError`; a `template_fields` implemented as a `property` that raises
    something else (before some other required state is configured, a plausible
    defensive pattern) propagates straight through `getattr`. This is exactly the shape
    this check exists to catch -- a notifier that will crash the first time Airflow
    actually reads `template_fields` -- so it must become a reported `ComponentProblem`,
    not an exception escaping `check_component` itself.
    """

    from airflow.sdk.bases.notifier import BaseNotifier

    class RaisingTemplateFields(BaseNotifier):
        @property
        def template_fields(self) -> tuple[str, ...]:
            raise RuntimeError("not configured yet")

        def notify(self, context: Any) -> None:
            del context

    (problem,) = compat_components._check_notifier_template_fields_unresolvable(
        RaisingTemplateFields()
    )

    assert problem.code == "notifier-template-fields-unresolvable"
    assert "RuntimeError" in problem.message


def test_notifier_template_fields_unresolvable_reports_a_non_string_entry() -> None:
    """Report, not raise, when a `template_fields` entry is not a string.

    `hasattr(component, name)` raises `TypeError` when `name` is not a string -- an
    ordinary typo (a missing trailing comma leaves a tuple containing `None`, not just
    the deliberately adversarial shape this also happens to guard against).
    """

    from airflow.sdk.bases.notifier import BaseNotifier

    class BadEntryNotifier(BaseNotifier):
        template_fields = ("message", None)
        message = "hello"

        def notify(self, context: Any) -> None:
            del context

    (problem,) = compat_components._check_notifier_template_fields_unresolvable(BadEntryNotifier())

    assert problem.code == "notifier-template-fields-unresolvable"
    assert "None" in problem.message


def test_secrets_backend_raises_on_miss_skips_a_non_callable_getter_override() -> None:
    """Report nothing, not raise, when an override is not actually callable.

    `_defining_class` only confirms an attribute is defined somewhere in the MRO, not
    that it is callable. `get_variable = None` (a plausible work-in-progress leftover)
    makes `inspect.signature` raise `TypeError`; that is not this check's business to
    judge, the same as an override with no annotation at all.
    """

    from airflow.secrets.base_secrets import BaseSecretsBackend

    NonCallableGetter = type("NonCallableGetter", (BaseSecretsBackend,), {"get_variable": None})

    assert tuple(compat_components._check_secrets_backend_raises_on_miss(NonCallableGetter)) == ()


def test_secrets_backend_raises_on_miss_skips_an_unintrospectable_getter_override() -> None:
    """Report nothing, not raise, when an override has no introspectable signature.

    `inspect.signature` raises `ValueError`, not `TypeError`, for a callable it cannot
    introspect at all -- a C-accelerated builtin wired in directly as an override.
    """

    from airflow.secrets.base_secrets import BaseSecretsBackend

    UnintrospectableGetter = type(
        "UnintrospectableGetter", (BaseSecretsBackend,), {"get_config": staticmethod(min)}
    )

    assert (
        tuple(compat_components._check_secrets_backend_raises_on_miss(UnintrospectableGetter))
        == ()
    )


def test_xcom_backend_signature_catches_an_unintrospectable_override() -> None:
    """Flag (not raise on) an override `inspect.signature` cannot introspect at all.

    `inspect.signature` raises `ValueError`, not `TypeError`, for a callable with no
    recoverable signature -- a C-accelerated builtin wired in directly as an override,
    plausible for a performance-conscious XCom backend. Catching only `TypeError` let
    this shape raise straight out of `check_component` instead of reporting it.
    """

    from airflow.sdk.bases.xcom import BaseXCom

    UnintrospectableSerialize = type(
        "UnintrospectableSerialize", (BaseXCom,), {"serialize_value": staticmethod(min)}
    )
    UnintrospectableDeserialize = type(
        "UnintrospectableDeserialize", (BaseXCom,), {"deserialize_value": staticmethod(str)}
    )

    (problem1,) = compat_components._check_xcom_backend_signature(UnintrospectableSerialize)
    assert problem1.code == "xcom-backend-signature"

    (problem2,) = compat_components._check_xcom_backend_signature(UnintrospectableDeserialize)
    assert problem2.code == "xcom-backend-signature"


def test_check_provider_no_entry_point_tolerates_an_unrelated_malformed_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip an unrelated registered distribution whose own name is None, not raise.

    `Distribution.name` reads the `Name` metadata header and is `None` for malformed
    `.dist-info` metadata -- real, confirmed Python behavior (`email.message.Message`
    returns `None` for a missing header rather than raising), not a purely theoretical
    shape. `canonicalize_name(None)` raises `AttributeError`; since this scan walks
    every distribution registering `apache_airflow_provider`, not just the one being
    checked, one unrelated distribution with broken metadata must not crash
    `check_component` on a provider that has nothing to do with it.
    """

    def get_provider_info() -> dict[str, str]:
        return {"package-name": "my-provider"}

    monkeypatch.setattr(
        compat_components, "_provider_owning_distribution", lambda _component: "my-provider"
    )
    malformed_entry_point = SimpleNamespace(dist=SimpleNamespace(name=None))
    good_entry_point = SimpleNamespace(dist=SimpleNamespace(name="my-provider"))

    def fake_entry_points(*, group: str) -> list[SimpleNamespace]:
        del group
        return [malformed_entry_point, good_entry_point]

    monkeypatch.setattr(compat_components.metadata, "entry_points", fake_entry_points)

    assert tuple(compat_components._check_provider_no_entry_point(get_provider_info)) == ()


def test_distribution_editable_roots_skips_a_pth_file_with_undecodable_bytes(
    tmp_path: Path,
) -> None:
    """Tolerate a `.pth` file that is not valid text in the default encoding.

    `UnicodeDecodeError` is a `ValueError`, not an `OSError` -- a `.pth` file is
    ordinary text this module does not control the encoding of, so both are real
    possibilities, not just a permissions/existence failure. `site.addpackage` (the
    stdlib's own `.pth` reader) tolerates the same failure the same way.
    """

    pth_file = tmp_path / "broken.pth"
    pth_file.write_bytes(b"\xff\xfe\x00\x01garbage")
    dist = _FakeDistribution("broken-encoding-dist", files=("broken.pth",), root=tmp_path)

    assert compat_components._distribution_editable_roots(dist) == ()
