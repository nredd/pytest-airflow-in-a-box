# One operator, no database

The usual way to test one operator is to build a `dict` context, stub `ti` with a mock, and
call `op.execute(context)`. That harness gets three things wrong, and two of them stay green.

**1. Template fields are never rendered.** Rendering happens in the task runner, not in
`execute()`. A hand-rolled call hands your operator the literal string, so an operator whose
`sql="SELECT {{ ds }}"` -- which ships `{{ ds }}` to the warehouse -- passes a test that
asserts on the same literal. `run_task`, `render_task`, and `task_context` render exactly like
a real run -- `"{{ 21 * 2 }}"` arrives at `execute()` as `"42"`.

**2. There is no active `get_current_context()`.** Passing a context as an argument does not
publish it. Any code the operator calls that reaches for
`airflow.sdk.get_current_context()` raises `RuntimeError: Current context was requested but no
context was found!`, which reads like a harness bug, so it gets patched -- and from then on the
ambient context is whatever the test typed. Here it is the real one, published by the Task SDK.

**3. A mock `ti` answers every question wrong.** Every attribute is another mock: truthy under
`if`, unequal to everything under `==`. `if ti.try_number:` takes the retry branch on the first
attempt, `ti.try_number == 1` is `False`, and `ti.try_number > 1` raises `TypeError`. In
`task_context` and `run_task`, `context["ti"]` is a real `RuntimeTaskInstance` -- the exact
class Airflow uses at runtime -- and `try_number=` selects a real integer attempt.

All three are pinned in `tests/fixtures/test_task_context.py`.

## Why not something else

- `dag.test()` is whole-Dag and DB-backed. It cannot be the fast inner loop for one operator,
  and it swallows task exceptions
- Airflow's own `tests_common` harness targets core and provider development, not Dag-author
  code in your repo. See [Deciding which failures are yours](testing-scope.md)
- Hand-rolling the runner means answering eight Task SDK supervisor message types and tracking
  `airflow.sdk.api.datamodels._generated` across minors. That is what
  [`_compat/`](../internals/compat-layer.md) is for

## Running one operator

`run_task` executes one operator through the Task SDK in process, with no metadata database.
XCom, Variable, and Connection traffic is answered from seeded dictionaries; unseeded lookups
fail exactly like a live deployment, with a hint naming the seeding keyword. Task callbacks and
listeners stay silent unless the call passes `run_callbacks=True`.

```python
def test_operator(run_task):
    result = run_task(
        my_operator,
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == "expected"
```

`TaskRunResult` carries `state`, `error`, `xcoms`, `msg`, and `sent`.

`try_number=` selects the synthetic attempt, and the operator's own `retries` decides whether a
failure classifies as `UP_FOR_RETRY` and fires its retry callback. This is the *only* rung where
that classification is reachable -- a persisted run attempts each instance exactly once.

## Where this rung stops

`run_task` is the fast inner rung. It cannot prove anything that needs a second task or real
metadata:

- XCom is keyed by XCom key **alone**. There is no `task_ids`, `run_id`, or `map_index`
  scoping, so a green `xcom_pull` does not prove data flowed from A to B
- Asset inlet/outlet validation is rubber-stamped: `ValidateInletsAndOutlets` always answers
  `inactive_assets=[]`, so an inactive asset never surfaces here
- Comms-backed `RuntimeTaskInstance` statics the fake supervisor does not answer (e.g.
  `get_dagrun_state`, `get_dr_count`) resolve to `None`, which the SDK then dereferences --
  expect an `AttributeError`, not a clean `None`

When an assertion needs any of those, climb to [`dag_maker.run_ti`](task-execution.md) --
[the fidelity ladder](ladder.md) lays out what each rung buys.

```text
Airflow 2.x: `run_task`, `render_task`, and `task_context` all fail with an actionable
error. The Task SDK in-process runner is a 3.x thing. Use `dag_maker.run_ti` or
`run_task_instance` for DB-backed execution on that family.
```

## Operators without a Dag

An operator never bound to any Dag works as-is -- no `dag=DAG(...)` boilerplate:

```python
def test_floating_operator(run_task):
    operator = BashOperator(task_id="my_task", bash_command="echo {{ dag.dag_id }}")

    result = run_task(operator)

    assert result.state == TaskInstanceState.SUCCESS
```

The same goes for a standalone `@task`: calling the decorated function outside any Dag
returns an XComArg, and its `.operator` runs directly:

```python
from airflow.sdk import task


@task
def add(x: int, y: int) -> int:
    return x + y


def test_add(run_task):
    result = run_task(add(1, 2).operator)

    assert result.xcoms["return_value"] == 3
```

The Task SDK requires every executing task to have a bound Dag, so `run_task`, `render_task`,
and `task_context` bind an unbound operator IN PLACE to a synthetic
`DAG(dag_id=..., schedule=None)`. Pass `dag_id="..."` to name it, or leave it off for a
deterministic, bounded, xdist-safe identifier derived from the test's nodeid, the xdist worker,
and a per-fixture invocation counter -- the same derivation `dag_maker` uses, salted so the two
never collide. The identifier is visible as `{{ dag.dag_id }}` in templates and as
`operator.dag.dag_id` after the call. The synthetic Dag is stamped with the test module's
location, so `template_ext` files (`.sql`, `.sh`, ...) resolve next to the test.

Two footguns follow from binding in place:

- A bound operator is never rebound (the SDK forbids it). Repeated calls reuse the first
  binding, and a later call passing a *different* explicit `dag_id` fails loudly rather than
  half-applying
- Build unbound operators inside the test body. A module-level operator shared across tests
  carries the first test's binding into the others

## Rendering template fields without running

`render_task` shares `run_task`'s machinery but stops after resolving `template_fields` -- it
never calls `execute()`. Use it when a test only needs an operator's resolved attributes. It
renders onto a fresh copy and returns that copy, so a Dag built once and reused across tests
renders independently every call. Always assert against the return value, and note the
`rendered(...)` matcher goes on the LEFT (its docstring says why):

```python
from airflow.sdk import DAG

from pytest_airflow_in_a_box.matchers import rendered


def test_operator_renders_its_query(render_task):
    with DAG(dag_id="probe_dag", schedule=None) as dag:
        MyOperator(task_id="probe", query="SELECT {{ params.value }}")

    rendered_operator = render_task(dag.get_task("probe"), params={"value": "42"})

    assert rendered(query="SELECT 42") == rendered_operator
```

`context_overrides` merges extra keys into the synthesized template context before rendering,
for fields referencing something the usual `params`/`variables`/`connections` seeding does not
cover.

## Getting the raw return value of execute()

`task_context` prepares the same machinery and then hands control back, so `execute()` and
`post_execute()` can be driven by hand. Its job is the value `execute()` *returns* -- which
`run_task` pushes to XCom and never shows you:

```python
def test_operator_execute_result(task_context):
    with task_context(my_operator, params={"value": 42}) as tc:
        result = tc.task.execute(tc.context)

    assert result.exit_code == 0
    assert tc.ti.log_url.startswith("http")
```

Reach for it when a test needs that raw return value, when an operator renders its templates
from *inside* `execute()`, or when application code reads incidental attributes off
`context["ti"]`.

Because the test drives `execute()` itself, nothing pushes the return value to XCom for you.
`tc.xcoms` reflects only what the operator pushed explicitly through
`context["ti"].xcom_push(...)`.

Always drive `tc.task`, not the operator you passed in: preparation happens on a
`prepare_for_execution()` copy, and an in-execute `render_templates()` renders `ti.task`, which
must be the object whose `execute()` is running. For a mapped operator, `tc.task` is the
concrete unmapped instance for `map_index` -- but only with the default `render=True`, since
Airflow unmaps inside `render_template_fields`. A mapped operator with `render=False` raises
`ValueError`.

Template fields are pre-rendered like a real run by default. Pass `render=False` for the
deferred-rendering pattern:

```python
def test_deferred_rendering(task_context):
    with task_context(my_operator, render=False) as tc:
        result = tc.task.execute(tc.context)  # execute() renders via context["ti"]
```

The fake supervisor is installed only inside the `with` block. `tc.xcoms` and `tc.sent` stay
readable after exit; `tc.ti` and `tc.context` do not outlive it. Full lifecycle semantics are
in [Fixtures](../reference/fixtures.md).
