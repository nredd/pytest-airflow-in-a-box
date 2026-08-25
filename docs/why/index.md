# The wall you hit at 500 tasks

In 2024 my team moved its nightly regressions off Jenkins and onto Airflow. Rewriting a pile
of unversioned shell scripts into documented, statically-checked, unit-tested Python was the
fun part. Then the Dag templates grew. A few hundred lines of generator, 500+ tasks on the
other side, and not one of the failures I actually cared about was reachable without a live
Airflow instance. Every check was a deploy and a wait.

This plugin is the thing I wanted then. It is a `pytest` run 🫡

## The failures that need a DagRun to exist

Your Dag files import cleanly and your callables pass. Everything between those two facts is
where the outages live:

- A `trigger_rule` that never fires because the branch upstream skipped
- A `template_fields` entry added to a subclass but never wired, so `{{ ds }}` ships literally
- A constructor arg that is not JSON-serializable, so the scheduler drops the Dag entirely
- A producer whose outlet does not actually trigger your consumer
- `depends_on_past` on a task whose first run has no predecessor
- A top-level `Variable.get()`: fine on your laptop, a parse failure in the scheduler loop

All of these are DagRun-shaped, so they surface at 03:00 on a scheduler you cannot attach a
debugger to. See [what a dagbag test and a callable test miss](dagbag-callable-gap.md) for
the two tests most repos already have, and exactly where they stop.

## What 500+ tasks changes

Scale does not just add more of the same failures. It adds failures that are properties of
the *set*, which no single-Dag test can phrase at any fidelity:

- Two templates rendering the same `dag_id`. `test_no_duplicate_dag_ids`
- One template doing import-time I/O, multiplied by every file it generates and paid on every
  scheduler parse loop. `test_dag_parse_budget` fails a file parsing slower than
  `airflow_dag_parse_budget_ratio` (default `10`) times the corpus median
- An `.expand()` over runtime data with no `max_active_tis_per_dag`, where one oversized
  upstream result fans out unbounded. `test_no_unbounded_expand`

Those ship as `--airflow-smoke`, a catalog you opt into rather than write. See
[smoke checks over every Dag](../guide/smoke-tests.md).

Scale also costs wall clock, because every one of those checks needs the whole folder parsed.
`--airflow-dag-bag-fanout` shards that parse across subprocess workers, and the parsed corpus
is built once per process and shared with local xdist workers through a file lock rather than
re-parsed per worker. Details in [corpus parsing and
parallelism](../internals/dag-corpus.md).

## Who this is for

- A team owning a single `dags/` repo on Airflow 3.1+, deployed by someone else (MWAA,
  Composer, Astro, self-hosted). You do not run the scheduler
- 50-500+ Dag files, many generated from templates. The templates are the scary part
- You write your own `BaseOperator` subclasses, hooks, sensors, `@task` decorators, and
  connection types. That is the load-bearing qualifier
- CI is GitHub Actions with `-n auto`, and a full-corpus check has to finish in minutes

If your repo is 100% stock operators, `dag.test()` plus a dagbag test is genuinely enough.
This is for the repos where the interesting code is yours.

## Where to go next

- [Deciding which failures are yours](../guide/testing-scope.md) -- the scope boundary
- [The fidelity ladder](../guide/ladder.md) -- how much machinery each assertion costs
- [Why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](why-not.md)
