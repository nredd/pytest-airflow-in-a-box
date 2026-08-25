# Vision

## 1. Thesis

> Your Dag files import cleanly and your callables pass -- this proves the seams between them work, in the repo's own CI, with no scheduler, no webserver, and no `~/airflow`.

Everything between "the file parses" and "the function returns the right value" is currently untested in most Dag repos: trigger rules, branch skips, cross-Dag asset triggering, `depends_on_past`, rendered templates, `conn_id` resolution, serialization of your own operator's constructor args. Those failures are DagRun-shaped, so they surface at 03:00 on a scheduler you cannot attach a debugger to. This plugin moves them left into `pytest`.

The moat is *timing*. Bootstrap runs from `pytest_load_initial_conftests`; pytest's own conftest collector is `trylast`. A consumer `conftest.py` structurally cannot win that race, so it cannot set `AIRFLOW_HOME`, the Airflow cfg, or ini-driven Airflow config before the first Airflow import. Every downstream capability -- `_compat/` absorbing ~45 private Airflow modules across 3.1-3.3, `run_task` driving the real Task SDK runner instead of a MagicMock `ti`, `cap_structlog` catching the Airflow 3 case where `caplog` returns empty -- depends on owning that slot.

## 2. Who it is for

For, one ICP, named:

- A platform or data-eng team of 2-8 owning a single `dags/` repo on Airflow 3.1+, deployed by someone else (MWAA, Composer, Astro, self-hosted -- they do not run the scheduler)
- 50-500+ Dag files, many generated from templates. The templates are the scary part
- They write their own `BaseOperator` subclasses, hooks, sensors, `@task` decorators, and connection types. That is the load-bearing qualifier
- Their suite today is exactly two things: a `DagBag` import test asserting `import_errors == {}`, and tests that call `task.function(...)` with hand-built mock contexts
- CI is GitHub Actions with `-n auto`, and a full-corpus check has to finish in minutes

Not for, by name:

- Airflow core contributors and provider authors. Shipping a package to other people as part of Airflow wants Breeze and upstream `tests_common`. `guide/testing-scope.md` draws that line correctly; the README's "Why not `tests_common`" bullet contradicts it, and the README is the thing that is wrong
- Anyone testing Airflow's machinery. `test_xcom_transports_a_value`, `test_scheduler_honors_timetable`, `test_stock_operator_serializes` are Airflow's tests, and Airflow runs them
- Airflow 2.x-only shops with no upgrade planned. The 2.x tier exists to get you *off* 2.x, not to be a permanent harness
- Repos that are 100% stock operators. `dag.test()` plus a dagbag test is genuinely enough. Say so out loud instead of pretending everyone is the target

The competitive frame, three claims and only three:

- vs `dag.test()`: not a test harness. It clears task instances and swallows task exceptions, so `assert result.success` is not an assertion. No fixtures, no isolated metadata DB, no xdist, no per-task drill-down. `dag.test(use_executor=True)` queues workloads nothing serves, so it cannot do what `executor=` does here (apache/airflow#59074)
- vs `DebugExecutor`: it does not exist on Airflow 3. It is the incumbent in every 2.x-era blog post and appears zero times in this docset today. A reader searching "DebugExecutor Airflow 3" must land somewhere
- vs a hand-rolled `conftest.py`: the real competitor, never named in the repo today. The claim is timing, not features. Cost of the hand-roll is not "impossible", it is "you now maintain a compat layer across three Airflow minors and you find out it broke in prod"

## 3. Why test -- the doctrine

The bug class this catches is the one that is *invisible until a DagRun exists*:

- A `trigger_rule` that never fires because the branch upstream skipped
- A `template_fields` entry added to a subclass but never wired, so `{{ ds }}` ships literally
- An operator whose constructor arg is not JSON-serializable, so the scheduler drops the Dag entirely
- A producer whose outlet does not actually trigger your consumer
- `depends_on_past` on a task whose first run has no predecessor
- Top-level `Variable.get()` in a Dag file: fine on your laptop, a parse failure in the scheduler loop

All fail late. The unit of feedback in a live deployment is a deploy plus a wait. Here it is a `pytest` run.

## 4. How to test -- the fidelity ladder

Fidelity costs money. Each rung buys a class of assertion the rung below structurally cannot make, and charges for it.

**Rung 0 -- `render_task`.** Proves your `template_fields` resolve to the string you expect. Costs nothing: no DB, no `execute()`, no Airflow ORM import. Cannot prove anything about the operator body.

**Rung 1 -- `run_task` / `task_context` (DB-free).** Proves your `execute()` runs against a real `RuntimeTaskInstance`, with real template rendering and a real `get_current_context()`. Retry classification is reachable *only* here, via `try_number=`. Costs a Task SDK in-process runner; 3.x only. Cannot prove anything involving a second task -- XCom is keyed by key alone, no `task_ids`/`run_id`/`map_index` scoping, so it cannot prove data flowed from A to B. Asset inlet/outlet validation is rubber-stamped.

**Rung 2 -- `dag_maker` + `run_ti` (persisted).** Proves one task instance against real metadata: real DagRun, real XCom table, real mapped expansion at a given `map_index`, real deferral through a persisted `Trigger` row. Costs a lazy DB migration on first request and an authored Dag in the test body. Cannot prove ordering or settling.

**Rung 3 -- `dag_maker.run()` / `run_dag`.** Proves `result.order` (what actually executed, not graph topology), `result.states` including `upstream_failed` propagation, mid-run mapped expansion. `run_dag` additionally proves your *real file* in `dags/` does this, under its real `dag_id`. Costs: the real `dag_id` becomes a shared metadata key, so two xdist workers running the same Dag can share a `DagModel` row and tear each other's metadata down. Only mitigation is `pytest.mark.xdist_group`, which is inert outside `--dist loadgroup`. Cannot prove retries: every instance is attempted exactly once, so a retry-configured task strands at `up_for_retry`.

**Rung 4 -- `executor=`.** Proves your task body survives re-import in a subprocess and your executor round-trips through a live Task Execution API. Upstream cannot reach this rung at all. Costs: the Dag must be a file in your Dag folder, `result.errors` degrades to best-effort, per-instance timeouts.

Two surfaces sit off the ladder, not on it:

- `run_trigger` -- defer, fire, resume. Spans rung 1 and rung 2. Single-shot: the first event only, one resume. A poll-loop trigger is not modeled
- The live REST API -- a different surface, for code you wrote that resolves `conf.get("api", "base_url")` or calls `/api/v2`. Not a fidelity increment

The climbing rule, one sentence: **stand on the lowest rung that can still fail for the reason you care about.** If the test would fail identically one rung down, you are paying for fidelity you are not asserting on.

Corpus checking is a *different axis*, not a rung. The ladder varies fidelity on one unit of code. `--airflow-smoke`, `dag_corpus`, and Dag coverage vary breadth over all units at fixed parse-only fidelity, and assert things no single-Dag test can phrase at any rung: duplicate `dag_id`s across files, a per-file parse budget, `catchup=True` anywhere, an unbounded `.expand()`. Properties of the *set*. The nav models this as a plane: `Running a task, rung by rung` on one axis, `Checking the whole Dag corpus` on the other.

## 5. What you can test -- the boundary

The test is already right and stays: **if it fails, is the bug yours?**

Two refinements the doctrine needs stated:

- The *subject* test, not the fixture test. The same `dag_maker` + `run()` shape is in scope when the assertion is "`branch` pushed `quarantine`, so `load` skipped", and out of scope when it is "a task pushed `{"a": 1}` and a downstream pulled `{"a": 1}`". Fixtures do not determine scope. Assertions do
- The "stock" qualifier is load-bearing. `test_dag_serialization_roundtrip` over *your* operator is in scope precisely because your constructor args are the part that fails. Over a stock operator it is upstream's test

Upper bound: shipping a provider package to other people wants Breeze and `tests_common`. This is for components living in your repo next to your Dags.

Unreconciled today, and closed by this plan: `test_dag_serialization_roundtrip` and `test_schedule_sanity` run over every Dag by default while `guide/testing-scope.md` rules stock-mechanism assertions out of scope. The carve-out *is* the reconciliation and the smoke page never cites it. Owner: `guide/smoke-tests.md` cites the carve-out, `guide/testing-scope.md` takes a one-line edit naming the default smoke items as the sanctioned instance. `testing-scope` is otherwise `keep`.

## 6. What's in the box

### Core -- five things

Everything else is optional. These are why anyone types `uv add --dev`.

1. **Pre-conftest bootstrap** -- isolated `AIRFLOW_HOME` plus disposable metadata DB, applied from `pytest_load_initial_conftests`. The moat
2. **Persisted execution** -- `dag_maker` / `run_dag` / `run_ti`. Real DagRun, real states, real XComs, real task relations. The product
3. **DB-free execution** -- `run_task` / `render_task` / `task_context`. Real Task SDK runner over four private Task SDK modules including `_generated`. The fast inner rung
4. **Corpus smoke checks** -- `--airflow-smoke`. Facts no per-Dag test and no text linter can phrase
5. **`_compat/` version shielding** -- 12.5k lines absorbing ~45 private Airflow modules across 3.1-3.3, gated by capability probes. Invisible, and the only reason 1-4 survive a minor bump

Core does not mean unblemished. Four core units have weak spots the rewrites must state, not hide:

- `airflow_config` as a *context manager* is a thin wrapper over `monkeypatch.setenv` -- env is first in `_lookup_sequence`, no cache, no invalidation hook. Only the *ini option* is irreducible, because only the ini option lands before the first `DagBag` parse. The page leads with the wrapper today. Lead with the ini option or the core claim is dishonest
- Rung 3 hides two costs: retries never re-run, and the same-`dag_id` xdist race whose only mitigation is inert outside `--dist loadgroup`. Both visible on the practitioner page, once, not twice
- `clear_db` is serial-only. Under `-n auto`, which is what CI runs, it is a footgun and the page never warns
- The Postgres tier over-serves the ICP. Keep it, stop selling it first

### Supporting

Earns its keep. Nobody installs for it.

- `run_trigger` -- real, single-shot, and the page never says so. The best concrete instance of "why not `dag.test()`" in the repo
- Seeding -- the fixtures lose to `monkeypatch.setenv("AIRFLOW_CONN_DB", ...)` for the ordinary case. The parse-time shim is the part with no substitute
- `cap_structlog` -- on Airflow 3 `caplog` returns *empty*, so a log assertion passes forever, including after you invert the branch. That fact is one clause today
- `--collect-dag-folder`, report artifacts, the live REST API, markers, `--airflow-doctor`, custom-component conformance, custom timetables, the isolated `AIRFLOW_HOME` page

### Peripheral

Real, kept, and off the primary reading path:

- The whole Airflow 2 to 3 migration toolkit
- Entry-point / packaging tests
- Cluster-policy composition
- Dag coverage
- `conf_vars` (deprecated alias; one sentence, not a paragraph)

## 7. How deep the box goes

One basement section, `Under the hood`, last in nav. Rule: **if a page cites a private module path, an upstream apache/airflow PR, or a version-divergence table, it lives in `internals/`.** Its reader arrives from a traceback or a port, never from onboarding.

`docs/adr/` and `docs/agents/` stay out of nav entirely. `internals/` pages link into ADRs; nav does not.

One exception, deliberate: `internals/bootstrap-env-ownership.md` and `internals/compat-layer.md` hold the *mechanism* behind core items 1 and 5, and `why/why-not.md` links to both from the on-ramp. The moat cannot be reachable only from a traceback.

## 8. The proposed docs tree

```yaml
nav:
  - Home: index.md

  - Getting started:
      - Quickstart: quickstart.md
      - Installing the plugin: install.md
      - Supported Airflow and Python versions: compatibility.md

  - Why test your Dag code:
      - The wall you hit at 500 tasks: why/index.md
      - What a dagbag test and a callable test miss: why/dagbag-callable-gap.md
      - Why not dag.test(), DebugExecutor, or your own conftest: why/why-not.md

  - What to test:
      - Deciding which failures are yours: guide/testing-scope.md
      - Recipes for the seams between tasks: guide/cookbook.md

  - Running a task, rung by rung:
      - The fidelity ladder: guide/ladder.md
      - One operator, no database: guide/db-free-execution.md
      - Real DagRuns and real state: guide/task-execution.md
      - Deferred tasks and your own triggers: guide/deferrable-operators.md
      - Talking to a live Airflow API: guide/rest-api.md

  - Giving tests their environment:
      - Where the run lives: guide/airflow-home.md
      - The disposable metadata database: guide/database.md
      - Overriding Airflow configuration: guide/configuration.md
      - Seeding Variables and Connections: guide/seeding.md
      - Asserting on what a task logged: guide/structlog.md
      - Keeping your own airflow_local_settings.py: guide/cluster-policies.md

  - Checking the whole Dag corpus:
      - Smoke checks over every Dag: guide/smoke-tests.md
      - One pytest item per Dag file: guide/dag-collection.md
      - Proving your Dag files are actually executed: guide/dag-coverage.md

  - Running it in CI:
      - The GitHub Action: guide/ci/github-action.md
      - Logs and JUnit XML you can trust: guide/reports.md

  - Testing your own Airflow components:
      - Checking a custom component: guide/custom-components.md
      - Wiring components into the run: guide/custom-components-wiring.md
      - Custom timetables: guide/custom-timetables.md
      - Entry points and packaging: guide/isolated-tests.md

  - What ships in the box:
      - Fixtures: reference/fixtures.md
      - Markers: reference/markers.md
      - Diagnosing a run: reference/diagnostics.md

  - Migrating from Airflow 2 to 3:
      - Start here: guide/migration/index.md
      - Migration-strict mode: guide/migration/strict.md
      - Pairing migration-strict with ruff's AIR rules: guide/migration/ruff-air-rules.md
      - Diffing outcomes across the upgrade: guide/migration/outcome-diff.md
      - Baseline artifact contract: guide/migration/baseline-artifact.md
      - Driving both families in one run: guide/migration/orchestrator.md
      - Running both families in CI: guide/migration/orchestrator-in-ci.md

  - Under the hood:
      - What _compat/ absorbs: internals/compat-layer.md
      - Who owns AIRFLOW__* (bootstrap and env drift): internals/bootstrap-env-ownership.md
      - Upstream tests_common parity: internals/tests-common-parity.md
      - Parse-time secret resolution: internals/parse-time-secrets.md
      - Corpus parsing, parallelism, and dag_corpus: internals/dag-corpus.md
      - Working on the plugin itself: development.md
```

Shape: 22 flat Guide siblings -> 11 top-level nodes. Largest section is 7 leaves. Every path is depth 2, and that is fine, because prose that needs an owner gets a real page, not a nav label.

Decisions this tree makes, stated because they overrule an input:

- **Section labels hold no prose.** Any instruction of the form "state it once on the parent" is unimplementable in mkdocs. Where a section needs prose, it gets a first leaf that *is* the landing page: `guide/ladder.md`, `why/index.md`, `guide/migration/index.md`. The migration funnel order (strict -> diff -> orchestrator -> orchestrator in CI) is stated once, in `guide/migration/index.md`
- **Rungs ascend.** `guide/db-free-execution.md` precedes `guide/task-execution.md`. The old nav put the most expensive rung first, and reproducing that one level up inside a section literally named "rung by rung" would have been the same defect with better signage
- **No index/detail table pairs.** `reference/fixtures.md` and `reference/markers.md` are the single source for each. The README carries a job-grouped teaser of 6-8 fixture rows and *no* marker table, both ending in a link. Manufacturing `fixture-index.md` and `marker-index.md` would create two more hand-maintained mirrors to cure a disease that is hand-maintained mirrors. A sync test against `fixtures/__init__.py::__all__`, `DATABASE_FIXTURE_NAMES`, and `MARKER_DESCRIPTIONS` guards the remaining copies
- **Quickstart is canonical in `quickstart.md`.** The README shows the same fenced example, included from one source file via `pymdownx.snippets`, which is already enabled. Two copies of an example is how the fixture table drifted
- **Three splits land in two pages.** `index`, `readme-installation`, and `readme-requirements` all split into the same two destinations: `install.md` (command, extras, resolution, `pytest11` auto-registration, `-p no:`) and `compatibility.md` (the version matrix, the interpreter caps, and the Airflow 2.x tier). Deliberate, and it is the point: `compatibility.md` becomes the *one* owner of the tier question
- **`install.md` precedes `compatibility.md`**, in nav and in the README. You install, then the plugin's own `--airflow-doctor` tells you whether your pin works. Overrules the earlier "check the pin first" ordering, which asked the reader to hand-verify a matrix the tool generates
- **`reference/defaults.md` is deleted**, folded into `guide/configuration.md`. Two beats must land intact or the merge is a regression: `airflow_default_filterwarnings` (the bootstrap's `catch_warnings()` + `simplefilter("default")` wipes the ini filter list, so a strict repo provably cannot silence alembic's deprecation without it) and the disclosure that the plugin silently rewrites `tbstyle`, `reportchars`, and `durations`. The cluster-policy ini option lands there too, not in a page that no longer exists
- **`doc-map.md` does not exist.** Inside the docs site the nav *is* the map. The README's `## Documentation` is a signpost of one deep link per stage, plus `airflow-migration-diff` named once (only console script, `pytest --help` will never show it) and one contributor bullet
- **The migration subtree is linked from Home.** It sits second-to-last in nav because a reader either enters it deliberately or skips it whole, but "you arrive migrating, you leave with a suite" is the best on-ramp this project has, so Home and `compatibility.md` both link into `guide/migration/index.md`. Sequenced late in nav, not hidden

### This is two products in one box

Say it out loud:

- **Product A** -- a Dag-author test harness for Airflow 3. Quickstart, the fidelity ladder, smoke, corpus, fixtures. That is the pitch
- **Product B** -- an Airflow 2 to 3 migration toolkit: `--airflow-migration-strict`, migration-diff, the `airflow-migration-diff` orchestrator, `requires_airflow2`/`requires_airflow3`, the 2.x compat tier. Different persona, different lifespan, different install path
- Today B occupies 3 of 22 flat Guide slots, a `## ` README heading that A's own core features do not get, and half the version-matrix prose. It drowns the pitch
- Fix is not to cut B. Fix is one subtree, one owning page for the 2.x tier, zero first-screen real estate, and one link from Home

### Newcomer reading order

Roughly a first day. Everything after item 15 is arrival-by-need.

1. `index.md`
2. `quickstart.md`
3. `install.md`
4. `compatibility.md`
5. `why/index.md`
6. `why/why-not.md`
7. `why/dagbag-callable-gap.md`
8. `guide/testing-scope.md`
9. `guide/ladder.md`
10. `guide/db-free-execution.md`
11. `guide/task-execution.md`
12. `guide/airflow-home.md`
13. `guide/database.md`
14. `guide/configuration.md`
15. `guide/smoke-tests.md`
16. `guide/cookbook.md`
17. `reference/fixtures.md`
18. `guide/ci/github-action.md`

### README after the trim

Order: Quickstart -> Install -> Requirements -> What ships -> Documentation -> License. ~400 lines -> ~180.

First screen, in order: thesis, two lines naming the reader, one line linking "already have `dag.test()` and a dagbag test? here is what they miss", the `run_dag` snippet, then `uv add --dev pytest-airflow-in-a-box` alone in its own fence.

Gone: the 9-row Markers table, the 21-row fixtures table, the 66-line GitHub Action reference, the full compat matrix, the `## Migration diff orchestrator` heading, `## Development`, `## Manifesto`.

Sequencing constraint: **`guide/ci/github-action.md` lands before the README trims.** That section is currently the only complete documentation of a published, `v0`-tagged, consumer-facing interface anywhere in the repo.

## 9. Cut, merge, split

Every non-`keep` verdict and its destination. `guide/testing-scope.md`, `guide/custom-timetables.md`, and `reference/fixtures.md` are `keep` and are not listed.

| Unit | Verdict | Destination |
|---|---|---|
| `index` | split | `index.md` (pitch, absorbs the `why-not` link) + `install.md` / `compatibility.md` |
| `readme-quickstart` | rewrite | `quickstart.md` canonical; README shows the same snippet via `pymdownx.snippets` |
| `readme-installation` | split | `install.md` (command, extras) + `compatibility.md` (the why) |
| `readme-requirements` | split | `install.md` + `compatibility.md`. Replace "exercised in CI against every combination below" -- it is false against `compat.yml`'s `include:` list -- and name `--airflow-doctor` as the live check |
| `readme-manifesto` | move-to-docs | `why/index.md`. Keep the 500+ tasks, trim the employer-identifying detail. It currently ships to PyPI as the package landing page |
| `readme-why-not` | move-to-docs | `why/why-not.md`. Keep `dag.test()` and dagbag+callable; add DebugExecutor and hand-rolled conftest; drop the Flowminder and `airflow-pytest-plugin` bullets; fold the `tests_common` claim into `guide/testing-scope.md`'s provider boundary |
| `cookbook` | split | `why/dagbag-callable-gap.md` (the argument, promoted to the on-ramp) + `guide/cookbook.md` (the recipes). Promote `evaluate_asset_schedules` out of recipe 7 of 7 |
| `task-execution` | split | `guide/task-execution.md` + `internals/tests-common-parity.md` |
| `db-free-execution` | rewrite | Lead with the three silent breakages of a hand-rolled `op.execute(mock_context)`; retitle to "One operator, no database" |
| `deferrable-operators` | rewrite | Lead with the renamed `TriggerEvent` key; state the single-shot limit; add the `run_triggerer=` / `executor=` exclusion |
| `rest-api` | rewrite | Lead with base-url publication, not a stock-endpoint assertion; compress the executor paragraph to a cross-link |
| `airflow-home` | rewrite | Lead with the isolation promise; fix the duplicated table header and the dead `--airflow-home-keep` link |
| `database` | rewrite | Lead with isolation, demote backends to last, give `clear_db` real prose plus a serial-only warning |
| `configuration` | split | `guide/configuration.md` (lead with the ini option) + `internals/bootstrap-env-ownership.md`. Absorbs `reference/defaults.md` and the cluster-policy ini option |
| `reference-defaults` | merge | `guide/configuration.md` |
| `seeding` | split | `guide/seeding.md` + `internals/parse-time-secrets.md` |
| `structlog` | rewrite | Lead with the silently-empty `caplog`; name `structlog.testing.capture_logs`; add `StructlogCapture` to `types.py` |
| `cluster-policies` | demote | `Giving tests their environment`. Lead with the `UsageError`, cut the ini-option apologia, fix `custom-components.md`'s dangling "above" |
| `smoke-tests` | split | `guide/smoke-tests.md` (the catalog) + `internals/dag-corpus.md`. Cite the "stock" carve-out |
| `dag-collection` | rewrite | Lead with per-file items; state the honest overlap with `--airflow-smoke` (identical messages, both parse, enabling both parses twice) |
| `dag-coverage` | demote | `Checking the whole Dag corpus`, not "Running the suite". Fix the false claim that there is no subprocess: `executor=` runs supervised workers |
| `readme-github-action` | move-to-docs | `guide/ci/github-action.md`. Fix `reports.md`'s `action@main` against the documented `@v0` |
| `reports` | rewrite | Invert: lead with the xdist log race and the isolated-child XML clobber, then the flag |
| `custom-components` | split | `guide/custom-components.md` + `guide/custom-components-wiring.md`. Retitle away from the bare phrase; stop `testing-scope.md` routing "custom components" here |
| `isolated-tests` | demote | `Testing your own Airflow components`, last leaf. Name `uv pip install -e .` as the more faithful test |
| `readme-fixtures` | rewrite | Job-grouped 6-8 rows plus a link. Drop the upstream-parity rows |
| `readme-markers` | move-to-docs | `reference/markers.md`. README keeps one pointer line |
| `reference-markers` | rewrite | Lead with the gating job, order by gate precedence, document the `environment` ini grammar |
| `reference-diagnostics` | rewrite | Lead with the false-green `--cov` containment check, paste a real report, add the missing inbound edges from `index.md` and README |
| `migration-strict` | split | `guide/migration/strict.md` + `guide/migration/ruff-air-rules.md` |
| `migration-diff` | split | `guide/migration/outcome-diff.md` + `guide/migration/baseline-artifact.md` |
| `migration-orchestrator` | demote | `guide/migration/orchestrator.md`. Lead with the provisioning problem, cut `## What it does not do` to one sentence |
| `readme-migration-orchestrator` | demote | `guide/migration/orchestrator-in-ci.md`. Delete the README heading; name `airflow-migration-diff` in `## Documentation` |
| `readme-documentation` | rewrite | Signpost of one deep link per stage. No topic-word list |
| `readme-development` | merge | One bullet in `## Documentation`. Fix `CONTRIBUTING.md`'s pointer at a Development section for the Postgres extra -- the real target is `make install-postgres` |

Files that move, so `mkdocs build --strict` needs the redirect in the same commit: `guide/migration-strict.md` -> `guide/migration/strict.md`, `guide/migration-diff.md` -> `guide/migration/outcome-diff.md`, `guide/migration-orchestrator.md` -> `guide/migration/orchestrator.md`. Every other surviving page keeps its path.

### Loose defects, each with an owner

Named in the audit, previously unowned:

- `StructlogCapture` is absent from `types.py` while `reference/fixtures.md` promises every return type lives there -> `structlog` rewrite
- `--airflow-doctor` has zero mentions in `README.md` and `docs/index.md` -> `reference-diagnostics` rewrite adds the inbound edges
- No test guards `README.md`'s fixture mirror against `__all__` / `DATABASE_FIXTURE_NAMES`, and none guards the marker mirror against `MARKER_DESCRIPTIONS` -> one sync test, landed with the `readme-fixtures` rewrite
- `guide/task-execution.md` cites `--airflow-home-keep`, which does not exist; the real flag is `--airflow-home-retention`. `guide/airflow-home.md` emits a duplicate table header row -> `airflow-home` rewrite
- `guide/reports.md` points at `action@main` while the README documents `@v0` -> `readme-github-action` move
- `conf_vars` is a third spelling buying a public `__all__` entry -> one sentence in `guide/configuration.md`
- The Airflow 2.x tier claim contradicts itself across four files -> `compatibility.md` owns it; nothing else restates it
