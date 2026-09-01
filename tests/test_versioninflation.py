"""Test the static runtime-varying Dag/task argument scanner over inline Dag sources."""

from __future__ import annotations

import ast
import textwrap

import pytest

from pytest_airflow_in_a_box import versioninflation

# Two-line prelude importing the Dag constructor and a varying callable; bodies start at line 3.
PRELUDE = "from airflow import DAG\nfrom datetime import datetime\n"

DAG_CONSTRUCTOR = "the Dag constructor"
TASK_CONSTRUCTOR = "a task constructor"
TASK_IN_DAG_BLOCK = "a task inside a with-Dag block"
TASK_DECORATOR = "a task decorator"


def _scan(source: str) -> list[versioninflation.RuntimeVaryingFinding]:
    """Run the scanner over one inline source and return its findings.

    Parameters:
        source: str containing the Dag module source, dedented before parsing.

    Returns:
        list[versioninflation.RuntimeVaryingFinding] containing every finding in line order.
    """

    text = textwrap.dedent(source)
    return versioninflation.find_runtime_varying_dag_args(ast.parse(text), text)


def _summarize(source: str) -> list[tuple[int, str]]:
    """Run the scanner and reduce each finding to its line and context.

    Parameters:
        source: str containing the Dag module source, dedented before parsing.

    Returns:
        list[tuple[int, str]] pairing each finding's 1-indexed line with its context phrase.
    """

    return [(finding.line, finding.context) for finding in _scan(source)]


@pytest.mark.parametrize(
    ("imports", "call"),
    [
        ("import datetime\n", "datetime.now()"),
        ("from datetime import datetime\n", "datetime.utcnow()"),
        ("import datetime as dt\n", "dt.today()"),
        ("import datetime\n", "datetime.datetime.now()"),
        ("from datetime import date\n", "date.today()"),
        ("import time\n", "time.time()"),
        ("import time\n", "time.localtime()"),
        ("import random\n", "random.randint(1, 10)"),
        ("from random import choice\n", "choice([1, 2])"),
        ("from random import uniform as u\n", "u(0.0, 1.0)"),
        ("from uuid import uuid4\n", "uuid4()"),
        ("from uuid import uuid1 as u1\n", "u1()"),
        ("import pendulum\n", "pendulum.now()"),
        ("import pendulum\n", "pendulum.yesterday()"),
    ],
)
def test_scanner_flags_each_runtime_varying_call_family(imports: str, call: str) -> None:
    """Flag every certified varying-call spelling passed straight to the Dag constructor."""

    source = f"from airflow import DAG\n{imports}d = DAG(dag_id='d', start_date={call})\n"

    assert _summarize(source) == [(3, DAG_CONSTRUCTOR)]


@pytest.mark.parametrize(
    ("source", "line"),
    [
        (f"{PRELUDE}d = DAG(dag_id='d', start_date=datetime.now())\n", 3),
        (f"{PRELUDE}DAG(dag_id='d', start_date=datetime.now())\n", 3),
        (f"{PRELUDE}with DAG(dag_id='d', start_date=datetime.now()):\n    pass\n", 3),
        (
            "from airflow import sdk\nfrom datetime import datetime\n"
            "d = sdk.DAG(dag_id='d', start_date=datetime.now())\n",
            3,
        ),
        (
            "from airflow.decorators import dag\nfrom datetime import datetime\n"
            "@dag(start_date=datetime.now())\ndef pipeline():\n    pass\n",
            3,
        ),
        (f"{PRELUDE}d = DAG('d', datetime.now())\n", 3),
    ],
)
def test_scanner_flags_every_dag_constructor_form(source: str, line: int) -> None:
    """Flag assign, bare-call, with-block, `sdk.DAG`, decorator, and positional forms."""

    assert _summarize(source) == [(line, DAG_CONSTRUCTOR)]


@pytest.mark.parametrize(
    ("body", "context"),
    [
        ("t = Op(task_id='t', ts=datetime.now(), dag=d)\n", TASK_CONSTRUCTOR),
        ("t = Op('t', d, ts=datetime.now())\n", TASK_CONSTRUCTOR),
        ("Op(task_id='t', ts=datetime.now(), dag=d)\n", TASK_CONSTRUCTOR),
    ],
)
def test_scanner_flags_tasks_bound_by_dag_argument(body: str, context: str) -> None:
    """Flag constructors that receive a registered Dag instance positionally or by keyword."""

    source = f"{PRELUDE}d = DAG(dag_id='d')\n{body}"

    assert _summarize(source) == [(4, context)]


@pytest.mark.parametrize(
    "call",
    ["PythonOperator", "make_task", "FileSensor"],
)
def test_scanner_flags_task_named_calls_inside_a_with_dag_block(call: str) -> None:
    """Flag `*Operator`/`*task`/`*sensor` calls with varying arguments inside a Dag block."""

    source = f"{PRELUDE}with DAG(dag_id='d') as dag:\n    t = {call}(ts=datetime.now())\n"

    assert _summarize(source) == [(4, TASK_IN_DAG_BLOCK)]


def test_scanner_flags_unassigned_task_calls_inside_a_with_dag_block() -> None:
    """Flag a bare task-call statement inside a Dag block, not just assignments."""

    source = f"{PRELUDE}with DAG(dag_id='d') as dag:\n    PythonOperator(ts=datetime.now())\n"

    assert _summarize(source) == [(4, TASK_IN_DAG_BLOCK)]


def test_scanner_flags_tasks_referencing_the_with_dag_variable_after_the_block() -> None:
    """Keep the `as dag` variable registered once the with-block has closed."""

    source = (
        f"{PRELUDE}with DAG(dag_id='d') as dag:\n    pass\n"
        "t = PythonOperator(task_id='t', ts=datetime.now(), dag=dag)\n"
    )

    assert _summarize(source) == [(5, TASK_CONSTRUCTOR)]


@pytest.mark.parametrize(
    ("decorator", "extra_import"),
    [
        ("task", "from airflow.decorators import task\n"),
        ("helpers.setup_task", "import helpers\n"),
    ],
)
def test_scanner_flags_task_decorators_with_varying_arguments(
    decorator: str, extra_import: str
) -> None:
    """Flag `@task(...)`-style decorators, plain or attribute-qualified."""

    source = (
        f"from datetime import datetime\n{extra_import}"
        f"@{decorator}(ts=datetime.now())\ndef f():\n    pass\n"
    )

    assert _summarize(source) == [(3, TASK_DECORATOR)]


@pytest.mark.parametrize(
    ("assignment", "snippet"),
    [
        ("start = datetime.now()", "datetime.now()"),
        ("start = datetime.now().replace(second=0)", "datetime.now()"),
        ("start = f'run_{datetime.now()}'", "datetime.now()"),
        ("start = datetime.now() - delta", "datetime.now()"),
        ("start = delta + datetime.now()", "datetime.now()"),
        ("start = [datetime.now()]", "datetime.now()"),
        ("start = (delta, datetime.now())", "datetime.now()"),
        ("start = {datetime.now()}", "datetime.now()"),
        ("start = [datetime.now() for _ in range(2)]", "datetime.now()"),
        ("start = {'sd': datetime.now()}", "datetime.now()"),
        ("start = {datetime.now(): 'sd'}", "datetime.now()"),
        ("start = wrap(datetime.now())", "wrap(datetime.now())"),
        ("start = wrap(when=datetime.now())", "wrap(when=datetime.now())"),
    ],
)
def test_scanner_tracks_tainted_variables_through_expression_shapes(
    assignment: str, snippet: str
) -> None:
    """Taint variables through chains, f-strings, arithmetic, collections, and wrappers."""

    source = f"{PRELUDE}{assignment}\nd = DAG(dag_id='d', start_date=start)\n"
    findings = _scan(source)

    assert [(finding.line, finding.snippet) for finding in findings] == [(4, snippet)]


def test_scanner_flags_a_dag_bound_to_an_attribute_target() -> None:
    """Flag a varying Dag constructor even when its result binds to an attribute."""

    source = f"{PRELUDE}ns.dag = DAG(dag_id='d', start_date=datetime.now())\n"

    assert _summarize(source) == [(3, DAG_CONSTRUCTOR)]


def test_scanner_taints_every_target_of_a_multiple_assignment() -> None:
    """Taint both names bound by `x = y = datetime.now()`."""

    source = f"{PRELUDE}x = y = datetime.now()\nd = DAG(dag_id='d', start_date=y)\n"

    assert _summarize(source) == [(4, DAG_CONSTRUCTOR)]


def test_scanner_reports_the_varying_expression_not_the_whole_constructor() -> None:
    """Report the varying sub-expression's own source text as the finding snippet."""

    findings = _scan(f"{PRELUDE}d = DAG(dag_id='d', start_date=datetime.now())\n")

    assert [finding.snippet for finding in findings] == ["datetime.now()"]


def test_scanner_reports_the_constructor_call_span() -> None:
    """Report both the first and last line of a multi-line constructor call."""

    source = f"{PRELUDE}d = DAG(\n    dag_id='d',\n    start_date=datetime.now(),\n)\n"

    assert [(finding.line, finding.end_line) for finding in _scan(source)] == [(3, 6)]


def test_scanner_flags_varying_values_behind_a_dict_expansion() -> None:
    """Skip a `**` expansion's ``None`` key and still flag the varying literal entry."""

    source = f"{PRELUDE}d = DAG(dag_id='d', default_args={{**base, 'sd': datetime.now()}})\n"

    assert _summarize(source) == [(3, DAG_CONSTRUCTOR)]


def test_scanner_defers_findings_inside_factory_functions_until_called() -> None:
    """Surface a factory's varying constructor only when the factory is actually called."""

    factory = f"{PRELUDE}def make():\n    return DAG(dag_id='d', start_date=datetime.now())\n"

    assert _summarize(factory) == []
    assert _summarize(f"{factory}make()\n") == [(4, DAG_CONSTRUCTOR)]


def test_scanner_reports_a_twice_called_factory_once() -> None:
    """Deduplicate the deferred finding when the factory is called more than once."""

    source = (
        f"{PRELUDE}def make():\n    return DAG(dag_id='d', start_date=datetime.now())\n"
        "make()\nmake()\n"
    )

    assert _summarize(source) == [(4, DAG_CONSTRUCTOR)]


def test_scanner_defers_async_factories_like_sync_ones() -> None:
    """Treat an async factory's body as deferred, exactly like a sync factory's."""

    factory = (
        f"{PRELUDE}async def make():\n    return DAG(dag_id='d', start_date=datetime.now())\n"
    )

    assert _summarize(factory) == []
    assert _summarize(f"{factory}make()\n") == [(4, DAG_CONSTRUCTOR)]


def test_scanner_taints_a_for_loop_target_iterating_varying_values() -> None:
    """Taint the loop variable while iterating a collection holding a varying value."""

    source = f"{PRELUDE}for i in [datetime.now()]:\n    d = DAG(dag_id='d', start_date=i)\n"

    assert _summarize(source) == [(4, DAG_CONSTRUCTOR)]


def test_scanner_untaints_the_loop_target_after_the_loop() -> None:
    """Stop tainting the loop variable once the loop body ends."""

    source = f"{PRELUDE}for i in [datetime.now()]:\n    pass\nd = DAG(dag_id='d', start_date=i)\n"

    assert _summarize(source) == []


def test_scanner_survives_nested_loops_sharing_a_target() -> None:
    """Untaint a shared loop target without crashing when nested loops reuse the name."""

    source = f"{PRELUDE}for i in [datetime.now()]:\n    for i in [datetime.now()]:\n        pass\n"

    assert _summarize(source) == []


@pytest.mark.parametrize(
    "source",
    [
        # Tuple loop targets are not tracked; the reference through `a` escapes.
        f"{PRELUDE}for a, b in [(datetime.now(), 1)]:\n    d = DAG(dag_id='d', start_date=a)\n",
        # A loop over static values taints nothing.
        f"{PRELUDE}for i in [1, 2]:\n    d = DAG(dag_id='d', start_date=i)\n",
    ],
)
def test_scanner_leaves_untracked_loop_shapes_alone(source: str) -> None:
    """Ignore tuple-target loops and loops over static iterables."""

    assert _summarize(source) == []


@pytest.mark.parametrize(
    "source",
    [
        # `import airflow` binds a plain module name; only from-imports are resolved.
        "import airflow\nfrom datetime import datetime\n"
        "d = airflow.DAG(dag_id='d', start_date=datetime.now())\n",
        # A DAG name imported from outside the `airflow` namespace is not Airflow's.
        "from mylib import DAG\nfrom datetime import datetime\n"
        "d = DAG(dag_id='d', start_date=datetime.now())\n",
        # An attribute constructor on a non-`airflow` from-import is not Airflow's.
        "from mylib import sdk\nfrom datetime import datetime\n"
        "d = sdk.DAG(dag_id='d', start_date=datetime.now())\n",
        # An `airflow` from-import that is neither `DAG` nor `dag` is not a constructor.
        "from airflow import Dataset\nfrom datetime import datetime\n"
        "x = Dataset(uri=f'a{datetime.now()}')\n",
    ],
)
def test_scanner_ignores_non_airflow_constructor_lookalikes(source: str) -> None:
    """Ignore DAG-shaped calls that do not resolve to Airflow's Dag constructor."""

    assert _summarize(source) == []


@pytest.mark.parametrize(
    "source",
    [
        # A tasky name outside any with-Dag block and without a dag argument is unbound.
        f"{PRELUDE}t = PythonOperator(task_id='t', ts=datetime.now())\n",
        # A non-tasky name inside a with-Dag block is not a task constructor.
        f"{PRELUDE}with DAG(dag_id='d') as dag:\n    helper(ts=datetime.now())\n",
        # Constructor arguments that never vary produce no finding.
        f"{PRELUDE}d = DAG(dag_id='d', start_date=datetime(2024, 1, 1), tags=['a'])\n",
        # `pendulum.datetime(...)` is a fixed instant, unlike `pendulum.now()`.
        "from airflow import DAG\nimport pendulum\n"
        "d = DAG(dag_id='d', start_date=pendulum.datetime(2024, 1, 1))\n",
        # A user-defined `now` is not the certified varying callable.
        "from airflow import DAG\ndef now():\n    return 1\n"
        "d = DAG(dag_id='d', start_date=now())\n",
        # A from-import whose module path has no varying entry stays clean.
        "from airflow import DAG\nfrom mylib.clock import stamp\n"
        "d = DAG(dag_id='d', start_date=stamp())\n",
        # A deep attribute chain never bottoms out at a certified module name.
        f"{PRELUDE}d = DAG(dag_id='d', start_date=a.b.c.now())\n",
        # A varying call outside any constructor is not a finding.
        "from datetime import datetime\ndatetime.now()\n",
        # An f-string over a static name does not taint the constructor.
        f"{PRELUDE}name = 'static'\nd = DAG(dag_id=f'prefix_{{name}}')\n",
        # A non-Name assignment target is not tracked.
        f"{PRELUDE}cfg.start = datetime.now()\nd = DAG(dag_id='d')\n",
        # A non-Call, non-name decorator shape passes through undisturbed.
        "from datetime import datetime\n@decorators[0]\ndef f():\n    return datetime.now()\n",
        # A non-task decorator with varying arguments is not a task decorator.
        "from datetime import datetime\n@cached(key=datetime.now())\ndef f():\n    pass\n",
        # A bare `@task` decorator defers the body without inspecting it.
        "from airflow.decorators import task\nfrom datetime import datetime\n"
        "@task\ndef f():\n    return datetime.now()\n",
        # A factory that is never called keeps its finding deferred forever.
        f"{PRELUDE}def make():\n    return DAG(dag_id='d', start_date=datetime.now())\nother()\n",
        # A with-statement over a non-Dag context manager creates no Dag block.
        "from datetime import datetime\nimport tempfile\n"
        "with tempfile.TemporaryDirectory() as td:\n    PythonOperator(ts=datetime.now())\n",
        # A with-statement over a bare name is not a constructor call at all.
        f"{PRELUDE}with ctx:\n    PythonOperator(ts=datetime.now())\n",
        # A tuple `as` binding registers no Dag instance variable.
        f"{PRELUDE}with DAG(dag_id='d') as (a, b):\n    pass\n"
        "t = PythonOperator(task_id='t', ts=datetime.now(), dag=a)\n",
        # A dict argument holding only static entries stays clean.
        f"{PRELUDE}d = DAG(dag_id='d', default_args={{'owner': 'me'}})\n",
        # A callee that is itself a call resolves to no certified callable.
        f"{PRELUDE}d = DAG(dag_id='d', start_date=make()())\n",
        # An attribute decorator with no task-shaped segment is not a task decorator.
        "from datetime import datetime\n@ns.helper(ts=datetime.now())\ndef f():\n    pass\n",
        # A call-shaped callee inside a Dag block carries no task name at all.
        f"{PRELUDE}with DAG(dag_id='d') as dag:\n    make()(ts=datetime.now())\n",
        # A relative import binds nothing this scanner can resolve.
        "from . import helpers\nfrom datetime import datetime\nx = helpers.f(datetime.now())\n",
    ],
)
def test_scanner_ignores_non_varying_and_unrelated_shapes(source: str) -> None:
    """Stay quiet on static values, lookalike names, and untracked binding shapes."""

    assert _summarize(source) == []


def test_scanner_matches_any_dotted_module_part_for_from_imports() -> None:
    """Flag a from-import whose dotted module path contains a certified module name."""

    source = (
        "from airflow import DAG\nfrom vendor.random import random\n"
        "d = DAG(dag_id='d', start_date=random())\n"
    )

    assert _summarize(source) == [(3, DAG_CONSTRUCTOR)]


def test_scanner_orders_findings_by_line() -> None:
    """Return findings sorted by source line across detection paths."""

    source = (
        f"{PRELUDE}def make():\n    return DAG(dag_id='a', start_date=datetime.now())\n"
        "d = DAG(dag_id='b', start_date=datetime.now())\nmake()\n"
    )

    assert _summarize(source) == [(4, DAG_CONSTRUCTOR), (5, DAG_CONSTRUCTOR)]


def test_snippet_falls_back_to_unparse_without_positions() -> None:
    """Render a node lacking end-position attributes through `ast.unparse`."""

    call = ast.Call(func=ast.Name(id="now", ctx=ast.Load()), args=[], keywords=[])
    call.lineno = 1
    call.col_offset = 0

    assert versioninflation._snippet(call, "irrelevant") == "now()"
