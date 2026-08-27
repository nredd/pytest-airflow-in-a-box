# `check_component`

You wrote a `Timetable`, a listener, or a `BaseExecutor` subclass, the class definition
imported fine, and the suite is green. That proves nothing: `BaseExecutor` is not an ABC,
`Timetable` is a `typing.Protocol`, and a listener or a policy hookimpl carries no base
class at all. Nothing about any of them is enforced when the class is defined. A shape
bug ships silently and first fails in a live scheduler, worker, or Dag processor.

`check_component` runs static conformance checks against that shape -- no metadata
database, no cache, no Airflow bootstrap, so it is safe in a plain unit test or a
pre-commit hook:

```python
from pytest_airflow_in_a_box.components import check_component


def test_my_timetable_conforms():
    check_component(MyTimetable).raise_for_problems()
```

Scope, because "custom component" is an overloaded phrase: this guide covers the ten
*pluggable extension kinds* Airflow discovers by registration -- timetable, listener,
executor, `XCom` backend, weight strategy, notifier, secrets backend, policy, plugin,
provider. Your own `BaseOperator` subclass, hook, sensor, or `@task` decorator is not one
of them and has no shape to check; test those by running them, through
[`run_task`](ladder.md#one-operator-no-database) or [`dag_maker`](ladder.md#one-task-real-state).

## The report

`check_component` accepts a bare class or an already-built instance interchangeably and
never constructs one itself, so it is safe on a component whose constructor is not
side-effect-free or takes required arguments. It returns a `ComponentReport`:

```python
report = check_component(MyExecutor)
report.ok  # bool: no problems found
report.problems  # tuple[ComponentProblem, ...] -- (code, message, hint) each
report.summary()  # human-readable, one line per problem
report.certification  # CertificationTier | None -- PROBED on an uncertified release
report.raise_for_problems()  # raises ComponentContractError when not ok
```

Checks are additive: each reports what it finds and never raises on the component itself,
so a wrong or overly strict check cannot fail an otherwise-passing suite. Only
`raise_for_problems()` (or asserting `.ok` yourself) turns a report into a test failure.

## Kind detection

Pass `kind=ComponentKind.TIMETABLE` / `.LISTENER` / `.EXECUTOR` / `.XCOM` /
`.WEIGHT_STRATEGY` / `.NOTIFIER` / `.SECRETS_BACKEND` / `.POLICY` / `.PLUGIN` /
`.PROVIDER` to force a check set, or omit it and let `check_component` classify the
component itself:

- **Timetable** -- nominal `Timetable` inheritance. A purely duck-typed timetable that
  never inherits `Timetable` needs the explicit `kind=`; structural `isinstance` checks
  are not used because `Timetable` declares data attributes as well as methods, which
  makes `issubclass` against it always raise `TypeError`. Checks:
  [Timetables](custom-timetables.md#static-shape-checks)
- **Listener** -- at least one `@hookimpl`-decorated method. Checks:
  [Observability](custom-components-observability.md#listener-checks)
- **Executor** -- `BaseExecutor` subclassing. Checks:
  [Execution](custom-components-execution.md#executor-checks)
- **`XCom` backend** -- `airflow.sdk.bases.xcom.BaseXCom` subclassing. Checks:
  [Execution](custom-components-execution.md#xcom-backend-checks)
- **Weight strategy** -- `airflow.task.priority_strategy.PriorityWeightStrategy`
  subclassing. Checks: [Execution](custom-components-execution.md#weight-strategy-checks)
- **Notifier** -- `airflow.sdk.bases.notifier.BaseNotifier` subclassing. Checks:
  [Observability](custom-components-observability.md#notifier-checks)
- **Secrets backend** -- `airflow.secrets.base_secrets.BaseSecretsBackend` subclassing.
  Checks: [Distribution](custom-components-distribution.md#secrets-backend-checks)
- **Policy** -- at least one `airflow.policies.hookimpl`-decorated method. Checks:
  [Observability](custom-components-observability.md#policy-checks)
- **Plugin** -- the same MRO name-and-module duck typing Airflow's own `is_valid_plugin`
  uses (`base.__name__ == "AirflowPlugin" and "plugins_manager" in base.__module__`)
  rather than `issubclass`, since core and the Task SDK reach the plugin base through
  different symlinked paths, and Python treats the two as distinct classes. Checks:
  [Distribution](custom-components-distribution.md#plugin-checks)
- **Provider** -- a non-class callable named exactly `get_provider_info`. Checks:
  [Distribution](custom-components-distribution.md#providers-if-you-are-shipping-one)

A component matching none of these, with no `kind` given, returns a clean, empty report
rather than raising. Every kind except timetable requires Airflow 3.x and reports no
problems on 2.x.

## Next

A clean report proves the shape, never that a run can load the component. That is
[wiring components into the run](custom-components-wiring.md).
