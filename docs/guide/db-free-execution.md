# DB-free task execution

`run_task` drives the Task SDK in-process runner, which Airflow 2.x predates -- on the 2.x
family the fixture fails with an actionable error; use `dag_maker.run_ti` or
`run_task_instance` for DB-backed execution there.

`run_task` executes one operator through the Task SDK in process, with no metadata database. XCom,
Variable, and Connection traffic is answered from seeded dictionaries; unseeded lookups fail
exactly like a live deployment. Task callbacks and listeners stay silent unless the call passes
`run_callbacks=True`. `try_number` selects the synthetic attempt; operator retry configuration
determines whether a failure reaches `UP_FOR_RETRY` and its retry callback. Asset inlet/outlet
validation is accepted as active in this deployment-free path.

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

## Operators without a Dag

An operator never bound to any Dag works as-is -- no `dag=DAG(...)` boilerplate:

```python
def test_floating_operator(run_task):
    operator = BashOperator(task_id="my_task", bash_command="echo {{ dag.dag_id }}")

    result = run_task(operator)

    assert result.state == TaskInstanceState.SUCCESS
```

The Task SDK requires every executing task to have a bound Dag, so `run_task`,
`render_task`, and `task_context` bind an unbound operator IN PLACE to a synthetic
`DAG(dag_id=..., schedule=None)`. Pass `dag_id="..."` to name it, or leave it off for a
deterministic, bounded, xdist-safe identifier derived from the test's nodeid, the xdist
worker, and a per-fixture invocation counter -- the same derivation `dag_maker` uses for
its default Dag ids, salted so the two never collide. The identifier is visible as
`{{ dag.dag_id }}` in templates and as `operator.dag.dag_id` after the call. The
synthetic Dag is stamped with the test module's location, so `template_ext` files
(`.sql`, `.sh`, ...) resolve next to the test, same as an explicit `with DAG(...)` in the
test file. A bound operator is never rebound (the SDK forbids it) -- repeated calls with
the same operator reuse the first binding, and a later call passing a *different*
explicit `dag_id` for a synthetic binding fails loudly instead of half-applying. Because
the binding persists on the operator, build unbound operators inside the test body -- a
module-level operator shared across tests would carry the first test's binding into the
others.

## Rendering template fields without running

`render_task` shares `run_task`'s Task SDK machinery and the same 2.x gate, but stops after
resolving `template_fields` -- it never calls `execute()`. Use it when a test only needs to
assert on an operator's resolved attributes, not drive its body. It renders onto a fresh copy
and returns that copy; rendering never mutates the operator passed in (auto-binding an
unbound operator to a synthetic Dag is the one side effect), so a Dag built once and reused
across tests renders independently every call. Always assert against the return value (note the
`rendered(...)` matcher goes on the LEFT -- see its docstring for why):

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
for fields that reference something `run_task`'s usual `params`/`variables`/`connections`
seeding does not cover.

## Hand-driving execute() with a real task context

`task_context` covers the gap between the two: it prepares the same Task SDK machinery and
then hands control to the test, so `execute()` and `post_execute()` can be driven by hand.
`context["ti"]` is a real `RuntimeTaskInstance` -- the exact class Airflow uses at runtime --
so `task_id`, `log_url`, `render_templates()`, and `airflow.sdk.get_current_context()` all
behave like a supervised run, with no hand-rolled `ti` stand-in to drift. `xcom_push` and
`xcom_pull` resolve too, against the same seeded store `run_task` uses -- keyed by XCom key
alone, so `task_ids`/`run_id`/`map_index` scoping is not enforced the way a real supervisor
would. Use it when a test needs the raw
return value of `execute()`, an operator renders its templates from *inside* `execute()`, or
application code reads incidental attributes off `context["ti"]`:

```python
def test_operator_execute_result(task_context):
    with task_context(my_operator, params={"value": 42}) as tc:
        result = tc.task.execute(tc.context)

    assert result.exit_code == 0
    assert tc.ti.log_url.startswith("http")
```

Because the test drives `execute()` itself, nothing pushes the return value to XCom for
you -- that is `run_task`'s job. `tc.xcoms` reflects only what the operator pushed
explicitly through `context["ti"].xcom_push(...)`; assert on `result` for the return
value.

Always drive `tc.task`, not the operator you passed in: preparation happens on a
`prepare_for_execution()` copy (the original is never mutated), and an in-execute
`render_templates()` renders `ti.task`, which must be the object whose `execute()` is
running. For a mapped operator, `tc.task` is the concrete unmapped instance for `map_index`
-- with the default `render=True` only: Airflow unmaps inside `render_template_fields`, so
combining a mapped operator with `render=False` raises `ValueError`.

Template fields are pre-rendered like a real run by default. Pass `render=False` for the
deferred-rendering pattern, where the operator calls `context["ti"].render_templates()`
itself mid-execution:

```python
def test_deferred_rendering(task_context):
    with task_context(my_operator, render=False) as tc:
        result = tc.task.execute(tc.context)  # execute() renders via context["ti"]
```

The fake supervisor is installed only inside the `with` block; the handle's `xcoms` and
`sent` snapshots stay readable after exit, but `tc.ti` and `tc.context` do not outlive it --
their lazy accessors (`var`, `conn`, the XCom methods) resolve through the installed
supervisor, so post-exit reads surface as not-found errors or hit whatever supervisor was
restored. Comms-backed `RuntimeTaskInstance` statics the
fake supervisor does not answer (e.g. `get_dagrun_state`, `get_dr_count`) are answered with
`None`, which the SDK then dereferences -- expect an `AttributeError`, not a clean `None`.
Stick to the XCom/Variable/Connection surface and `render_templates()`.
