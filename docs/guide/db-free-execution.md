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

## Rendering template fields without running

`render_task` shares `run_task`'s Task SDK machinery and the same 2.x gate, but stops after
resolving `template_fields` -- it never calls `execute()`. Use it when a test only needs to
assert on an operator's resolved attributes, not drive its body. It renders onto a fresh copy
and returns that copy; the operator passed in is never mutated, so a Dag built once and reused
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
