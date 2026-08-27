# Checking components

You wrote a timetable, listener, or executor; the class imported and the suite stayed green.
That proves remarkably little. Many Airflow extension points are protocols, pluggy hooks, or
base classes whose defaults fail only when the scheduler finally calls them.

`check_component` checks the static contract without a database, cache mutation, or Airflow
bootstrap:

```python
from pytest_airflow_in_a_box.components import check_component


def test_my_timetable_conforms():
    check_component(MyTimetable).raise_for_problems()
```

It covers the ten pluggable kinds Airflow discovers by configuration or registration. Your own
operator, hook, sensor, or `@task` decorator is not one of them; execute that code through the
[fidelity ladder](ladder.md).

## The report

Pass a class or an instance. The checker never constructs a class, so required or side-effectful
constructors are safe:

```python
report = check_component(MyExecutor)
report.ok
report.problems          # tuple[ComponentProblem, ...]
report.summary()
report.certification     # CertificationTier | None
report.raise_for_problems()
```

Each problem has `code`, `message`, and `hint`. Checks accumulate findings without raising;
only `raise_for_problems()` or your own assertion turns them into a failure. On an uncertified
Airflow release, live probes still run and `report.certification` is `PROBED`.

Pass `kind=ComponentKind.<NAME>` to force classification. Without it, an unknown component
returns a clean empty report. Every kind except timetable requires Airflow 3.x and reports no
problems on 2.x.

## What gets checked

| Kind | Detection | Problems |
| --- | --- | --- |
| Timetable | `Timetable` inheritance | Local qualname; missing protocol methods; incomplete serialization pair; non-JSON state |
| Listener | At least one `@hookimpl` method | Unknown hook or argument; hook available to only one listener manager |
| Executor | `BaseExecutor` subclass | Missing overrides; stale attributes; release-specific sentry flag type |
| XCom backend | `BaseXCom` subclass | Removed ORM deserializer; wrong static method or call signature |
| Weight strategy | `PriorityWeightStrategy` subclass | Abstract methods; inherited or missing hash implementation |
| Notifier | `BaseNotifier` subclass | Missing `notify`; unresolvable template field |
| Secrets backend | `BaseSecretsBackend` subclass | A lookup return annotation that does not admit `None` |
| Policy | `airflow.policies.hookimpl` method | Unknown hook or argument mismatch against the installed hookspec |
| Plugin | Airflow's name-and-module MRO check | Missing plugin name |
| Provider | Callable named `get_provider_info` | Invalid schema, package mismatch, or missing entry point |

### Timetables

- `timetable-local-qualname`: a class defined inside a function contains `<locals>` and cannot
  be found by Airflow's qualified-name lookup.
- `timetable-missing-protocol-method`: `infer_manual_data_interval` or `next_dagrun_info` still
  has the raising default.
- `timetable-serialize-pair-incomplete`: only one of `serialize` and `deserialize` is
  overridden, so state is silently lost.
- `timetable-serialize-not-json`: an instance returns state Airflow cannot JSON-encode.

The worked logic and registration flow live on [Timetables](custom-timetables.md).

### Execution components

- `executor-missing-override`: `sync`, `_process_workloads`, or `end` retains an inert or
  raising default.
- `executor-stale-attribute`: the class sets one of the 2.x-era attributes Airflow 3 ignores.
- `executor-flag-wrong-type`: the sentry capability uses the wrong release-specific name or
  type.
- `xcom-orm-deserialize-removed`: `orm_deserialize_value` is silently dead on Airflow 3.
- `xcom-backend-signature`: serialization methods cannot accept the base class's real call
  shape or lost their `@staticmethod` behavior.
- `weight-strategy-abstract` and `weight-strategy-hash-of-none`: the strategy cannot be built or
  cannot behave correctly as a set/dict key.

To prove an executor actually runs a workload, use
[`run_dag(..., executor=...)`](ladder.md#executor-driven-runs). A complete serial executor is in
the [Cookbook](cookbook.md#a-minimal-serial-executor).

### Observability components

- `listener-no-matching-hookspec` and `listener-unknown-argument`: pluggy would ignore the hook
  or reject it at registration.
- `listener-core-manager-only` and `listener-sdk-manager-only`: the hook exists on only one of
  Airflow's two listener managers.
- `notifier-missing-notify`: only `async_notify` was implemented, while callback paths call
  synchronous `notify`.
- `notifier-template-fields-unresolvable`: an instance names an attribute it does not carry.
- `policy-unknown-hookspec` and `policy-argument-name-mismatch`: the hook cannot register
  against the installed release.

Plain functions loaded through `airflow_local_settings.py` use an older policy mechanism and
are not classified as policy components.

### Distribution components

- `secrets-backend-raises-on-miss`: an override's declared return type does not allow `None`.
  The checker does not call a real backend or fabricate credentials.
- `plugin-name-missing`: discovery would reject an `AirflowPlugin` with no name.
- `provider-info-schema`: `get_provider_info()` raises or violates the installed Airflow
  package's schema.
- `provider-package-name-mismatch` and `provider-no-entry-point`: distribution metadata does
  not agree with or expose the provider callable.

Pass the `get_provider_info` callable, not its result. Distribution checks that cannot
attribute a callable to an installed package are skipped. To prove the entry point itself
resolves, use the isolated process described in
[Registration and packaging](custom-components-wiring.md#isolated-entry-point-discovery).

## Shape is not registration

A clean report proves only the component's contract. Production still loads it through a
plugin, configuration value, or distribution entry point. The next step is
[Registration and packaging](custom-components-wiring.md).
