# pytest-airflow-in-a-box

Your Dag files import cleanly and your callables pass. That proves neither that the *seams*
between them work.

Trigger rules, branch skips, rendered templates, `conn_id` resolution, serialization of your
own operator's constructor args -- those failures are DagRun-shaped, so they surface at 03:00
on a scheduler you cannot attach a debugger to. This plugin moves them into `pytest`, in your
repo's own CI, with no scheduler, no webserver, and no `~/airflow`.

Who this is for:

- A team owning a `dags/` repo on Airflow 3, deployed by someone else -- MWAA, Composer,
  Astro, self-hosted. You do not run the scheduler
- You write your own `BaseOperator` subclasses, hooks, sensors, `@task` decorators, and
  connection types. That is the load-bearing qualifier. A repo that is 100% stock operators
  does not need this

Already have a dagbag import test and `dag.test()`? See
[what they miss](why/dagbag-callable-gap.md) and
[why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](why/why-not.md).

## One green test

A branch skip is invisible to both halves of the usual suite: the file parses, every callable
returns the right value, and `load` still runs when it should not have.

```python
from airflow.sdk import task

from pytest_airflow_in_a_box.matchers import skipped


def test_branch_skips_the_unselected_path(dag_maker):
    with dag_maker(dag_id="branching"):

        @task.branch
        def choose() -> str:
            return "chosen"

        @task
        def chosen() -> None: ...

        @task
        def rejected() -> None: ...

        choose() >> [chosen(), rejected()]

    result = dag_maker.run()

    assert result.success
    assert result.order == ["choose", "chosen"]
    assert result["rejected"] == skipped()
```

`result.order` is what *actually* executed, not graph topology. `dag.test()` cannot phrase
either assertion: it clears task instances and swallows task exceptions, so the call itself
never fails and you are left fishing state out of the returned `DagRun` -- see [why not
`dag.test()`](why/why-not.md).

## Start here

- [Quickstart](quickstart.md) -- three rungs, one page
- [Installing the plugin](install.md), then
  [supported versions](compatibility.md)
- [The fidelity ladder](guide/ladder.md) -- how much realism a test needs, and what each rung
  costs
- [Deciding which failures are yours](guide/testing-scope.md) -- where the line falls
- [Fixtures](reference/fixtures.md) and [diagnosing a run](reference/diagnostics.md)
- Upgrading off Airflow 2? [Start here](guide/migration/index.md)

## License

Apache License 2.0. See
[`LICENSE`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE),
[`NOTICE`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/NOTICE), and
[`PROVENANCE.md`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/PROVENANCE.md).
