# Airflow in a Box: Research Baseline for the Testing Plugin Release

_Canonical research record. Imported from an internal source revision and revised on 2026-08-07 to
remove environment-specific details. Later validation corrected four implementation assumptions:
early worker configuration requires an inherited environment handoff before `workerinput` exists;
`tmp_path_factory` is unavailable during pre-import bootstrap; the live API uses Airflow's built-in
`SimpleAuthManager` and a retained loopback socket; and the released support matrix is Airflow
3.1.8, 3.2.2, and 3.3.0 with Python 3.10-3.14 where constraints exist. The implementation plan is
authoritative where this historical record describes an earlier design._

## Context

This is the research record backing the Airflow Summit 2026 talk ["Airflow in a Box:
Methodology or Madness"](https://airflowsummit.org/sessions/2026/airflow-in-a-box-methodology-or-madness/)
and the public PyPI pytest plugin it will launch alongside. The talk has two goals: (1) give a
formal breakdown of the state of testing/verification in Apache Airflow, and (2) release a
generalized, independently supported version of a forked `devel-common`/`tests_common` pytest plugin
that lets developers verify real Dags and Airflow infrastructure entirely inside a pytest harness,
without a live Airflow instance.

The question this document answers: **what does upstream `apache/airflow`'s own `devel-common`
provide today, is it evolving toward public reuse, what does a production fork already do beyond
it, and what's the resulting gap that a
public release should fill?**

Research was conducted by: reading `devel-common/src/tests_common/pytest_plugin.py` (~3,125 lines)
and `test_utils/*` in full in the local `apache/airflow` checkout; walking its git history;
reading the fork's conftest bootstrap and core plugin module,
and `tests/common/test_utils/*` in full; querying `apache/airflow` on GitHub (PRs, issues,
discussions) via `gh`; and querying the `dev@airflow.apache.org` mailing list archive directly via
its Pony Mail JSON API (`lists.apache.org/api/thread.lua`, `.../email.lua`) with individual quotes
fetched and verified against the raw message bodies, not paraphrased from search snippets.

A separate research document (a prior internal write-up, a "Unit Testing Challenges and Proposals in
Airflow" brief prepared earlier this year) was reviewed but is **not cited as a source** anywhere
below — it reads as an AI-generated synthesis, and its one specific, checkable claim (a "2021
GitHub discussion" in which maintainers "welcomed" a public `pytest-airflow` plugin and said it
"could even live in the Airflow repository") could not be verified and appears to be a fabrication
or misattribution (see [§4](#4-the-2021-pytest-airflow-claim-debunked)). Every claim in this
document is traceable to a primary source: a specific commit, PR, mailing-list message, or file in
one of the two repos.

**Sections 7–9 were added 2026-08-06** and cover decisions and measurements rather than literature:
the settled package/repo name (§7), the database architecture — including empirically measured SQLite
tuning results on both target machines and the NFS correctness trap (§8) — and the deferred
Postgres/MySQL provisioning survey (§9). Benchmarks and SQLite/SQLAlchemy behavioral claims in §8
were measured directly on two representative target hosts rather than taken from documentation.

**Sections 10–15 were added later the same day** and cover the `test_utils` disposition audit (§10,
driven by real usage counts from the fork rather than re-deriving from first principles), the
xdist bootstrapping redesign replacing env vars and FileLock convergence with
`pytest_configure_node`/`workerinput`/`pytest.StashKey` (§11), the custom Dag-file collection design
including a reproduced double-collection trap and its fix (§12), the dependency and
config-boilerplate audit (§13), and the build-tooling decision (`uv_build`, no `hatch`/`tox`/`nox`,
§14). Every mechanism in §11–§14 was verified with a runnable local reproduction before being
recorded as a conclusion, not asserted from documentation alone.

**Sections 16–17 were added the same day, after further discussion**, covering the official
`pytester`-based self-testing strategy and why the fork's `tox.ini`/Makefile/`.github/workflows`
cannot be inherited as-is (§16), including the finding that Docker-based per-OS/ISA testing
(`tox-docker`, Dockerfiles per platform) is not merely suboptimal but structurally incoherent for
two of the three target platforms (macOS, Windows) since Docker containers share the host kernel.

**Section 18 was added after §16 prompted a follow-up question**: whether GitHub Actions workflows
(now the sole CI mechanism, since §16 dropped `tox` entirely) can be reproduced and iterated on
locally without push-to-test. Verified yes via `act`, with a real local run reproduced end-to-end
rather than asserted from its README.

**Section 20 was added after a follow-up question about the multi-Airflow-version compat matrix
itself**: whether to adopt the fork's constraints-to-pinned-extras generator script. Verified `uv run --with
apache-airflow-core==X` resolves without it, but also verified a real transitive-pin drift risk
that motivates using Airflow's own published constraints files (via `uv pip install
--constraint`, not that generator script) rather than either unconstrained resolution or the fork's
provider-deployment-oriented generator.

---

## 1. Upstream `devel-common`/`tests_common` — current architecture

**Location:** `devel-common/src/tests_common/pytest_plugin.py` (3,125 lines) plus ~50 modules
under `devel-common/src/tests_common/test_utils/`.

**Not a `pytest11` entry point.** There is no `[project.entry-points.pytest11]` anywhere in the
monorepo. Every consuming test suite opts in explicitly via a `conftest.py` one-liner:

```python
pytest_plugins = "tests_common.pytest_plugin"
```

This string is copy-pasted into ~115 `conftest.py` files across `airflow-core/tests`, every
`providers/*/tests`, `task-sdk/tests`, `airflow-ctl/tests`, and `devel-common/tests` itself. The
plugin is a **shared implementation detail wired in by convention**, not a package that announces
itself to pytest the way a real public plugin would.

**Bootstrapping.** The module asserts `"airflow" not in sys.modules` at import time — it must
configure environment variables before Airflow itself is imported, or config gets baked in wrong.
It scrubs all `AIRFLOW__SECTION__OPTION` env vars except an allow-list (full env isolation unless
`--keep-env-variables` is passed), and force-sets `AIRFLOW__CORE__DAGS_FOLDER`,
`AIRFLOW__CORE__UNIT_TEST_MODE=True`, and several other core config keys before any test runs.

**Hooks implemented:** `pytest_addoption` (`--with-db-init`/`--without-db-init`, `--skip-db-tests`
/`--run-db-tests-only`, `--backend`, `--integration`, `--system`, etc.), `pytest_configure`
(registers ~14 markers, wires in two internal plugins for warning capture), `pytest_unconfigure`,
`pytest_collection_modifyitems`, and `pytest_runtest_setup`.

**Markers registered:** `integration(name)`, `backend(name)`, `system`, `platform(name)`,
`long_running`, `quarantined`, `credential_file(name)`, `need_serialized_dag`,
`want_activate_assets`, `db_test`, `non_db_test_override`, `virtualenv_operator`,
`external_python_operator`, `enable_redact`, `mock_plugin_manager`.

**DB test detection and collection-time deselection.** A test counts as a DB test if marked
`db_test` (and not overridden by `non_db_test_override`), or if marked with any `backend` marker.
As of PR [#63791](https://github.com/apache/airflow/pull/63791) ("Deselect DB tests at collection
time instead of skipping at runtime", merged 2026-03-19), `--skip-db-tests`/`--run-db-tests-only`
filtering happens in `pytest_collection_modifyitems`, not `pytest_runtest_setup` — so `pytest-xdist`
workers never even import the ~6,350 modules they'd otherwise skip. This was a deliberate fix for
OOMs on Python 3.14 CI runners caused by importing thousands of soon-to-be-skipped DB tests.

**xdist coordination** is handled almost entirely through environment-variable propagation
(`_AIRFLOW_SKIP_DB_TESTS`, `_AIRFLOW_RUN_DB_TESTS_ONLY` set at import time, before xdist forks
workers) plus a module-scoped `_clear_db` autouse fixture that explicitly backs off under xdist
(`if dist_option != "no" or hasattr(request.config, "workerinput"): return`) — DB cleanup between
modules is a controller-only concern.

**Key fixtures:**

- **`dag_maker`** — the workhorse: builds a `DAG`, syncs `DagModel`/`SerializedDagModel`
  /`DagBundleModel`, exposes `.create_dagrun()`, `.create_ti()`, `.run_ti()`, `.cleanup()`. Heavily
  branches on `AIRFLOW_V_3_0_PLUS`/`_3_1_PLUS`/`_3_2_PLUS`/`_3_3_PLUS` compat flags throughout
  (`test_utils/version_compat.py`) — this file is a living map of Airflow 2→3 API churn.
- **`mocked_parse` / `create_runtime_ti` / `run_task`** — the AIP-72/Task SDK–oriented fixtures.
  They build a `RuntimeTaskInstance` and drive it through
  `airflow.sdk.execution_time.task_runner.run()` **without a live scheduler or API server**,
  entirely in-process, with `mock_supervisor_comms` faking the Execution API socket. `run_task`
  exposes `.state`, `.msg`, `.error`, `.xcom.get()`/`.assert_pushed()`. This is the closest thing in
  the whole repo to "run one task in isolation and assert on its outcome," but it's deeply wired
  into internal SDK types (`StartupDetails`, `TIRunContext`, `RuntimeTaskInstance`) with no
  documented stability guarantee.
- `create_dummy_dag`, `create_task_instance`, `create_task_instance_of_operator`, `get_test_dag`,
  `session`, `testing_dag_bundle`, `mock_xcom_backend`, `clean_executor_loader`,
  `hook_lineage_collector`, `frozen_sleep`, `trace_sql`, and a `caplog` override backed by
  structlog capture (`cap_structlog`).

**`test_utils/*` inventory (by category):**

| Category | Modules |
|---|---|
| DB & data setup | `db.py` (all `clear_db_*`/`initial_db_init`), `dag.py`, `taskinstance.py` |
| Mocking | `mock_context.py`, `mock_executor.py`, `mock_operators.py`, `mock_plugins.py`, `common_sql.py`, `aiohttp.py`, `hdfs_utils.py`, `fake_datetime.py` |
| Task SDK / Execution API | `in_process_taskrun.py` — DB-free, xdist-safe execution of a task through a *real* supervisor socket (built for `PythonVirtualenvOperator`/`ExternalPythonOperator`, which spawn subprocesses that reconnect to the supervisor) |
| Compat shims | `version_compat.py` (explicitly commented "copied manually in other providers deliberately" — i.e. not even imported cross-package, duplicated by convention), `compat.py` |
| Config/env | `config.py` (`conf_vars`), `fernet.py`, `paths.py`, `permissions.py` |
| Timetables | `timetables.py` |
| API/HTTP testing | `api_fastapi.py`, `api_client_helpers.py` |
| Logging | `logs.py` (`StructlogCapture`, `check_last_log`), `log_handlers.py`, `otel_utils.py` |
| System/integration (external-service-dependent) | `azure_system_helpers.py`, `gcp_system_helpers.py`, `salesforce_system_helpers.py`, `sftp_system_helpers.py`, `system_tests.py`, `system_tests_class.py`, `terraform.py` |
| Misc | `mapping.py`, `providers.py`, `watcher.py` (the `@task` "watcher" pattern used at the end of *system test* Dags to fail the whole run if any upstream task fails) |

**Package metadata confirms internal-only intent.** `devel-common/pyproject.toml` declares
`name = "apache-airflow-devel-common"`, `classifiers = ["Private :: Do Not Upload"]` — the standard
Trove classifier meaning "never upload to PyPI," which `flit` respects as a build safety net. It
depends *on* `apache-airflow-core` (the reverse of what a standalone reusable testing package would
look like), and there is no `[project.entry-points]` table anywhere. This same classifier is used
identically across ~18 other internal-only packages in the repo (`dev/`, `scripts/`,
`docker-tests/`, every `shared/*` library) — devel-common follows an established repo-wide
"never publish" convention, not a devel-common-specific decision.

### Historical trajectory

| Date | Move |
|---|---|
| 2024-10-09/11 (#42505/#42624) | Providers split into a uv workspace; shared test code first extracted into `dev.tests_common` because provider tests could no longer implicitly inherit fixtures from root `tests/conftest.py`. |
| 2024-10-15 (#42985) | Moved `tests_common` out of `dev/` — for a **security** reason: the whole `dev/` folder gets replaced with target-branch content for non-committer PRs (to stop a malicious PR from smuggling code into `pull_request_target` CI), so test code couldn't safely live there. |
| 2025-02-16 → 2025-03-05 | `[DISCUSS] Turn "tests_common" into separate distribution for development` on `dev@airflow.apache.org` → PR [#47281](https://github.com/apache/airflow/pull/47281), merged by Jarek Potiuk, creating today's `devel-common` workspace member. See [§3](#3-the-devdevel-common-mailing-list-thread--the-single-most-relevant-primary-source) — this is also where a public-package proposal was explicitly rejected. |

Recent activity (last ~50 commits touching `pytest_plugin.py`, spanning Aug 2025–Jun 2026) is
dominated by tracking Airflow 3's internal architecture churn (Task SDK/Execution API split,
serialization refactors, provider-manager rework) and CI/performance hardening — e.g. PR
[#63791](https://github.com/apache/airflow/pull/63791) (collection-time DB-test deselection), PR
[#59849](https://github.com/apache/airflow/pull/59849) (SQLAlchemy 2.0 migration across
`pytest_plugin.py`/`test_utils/db.py`), PR [#69093](https://github.com/apache/airflow/pull/69093)
(fixture cleanup for bundles/teams it creates), PR
[#62823](https://github.com/apache/airflow/pull/62823) (retry decorator for flaky DB setup). None
of this is "generalize for external reuse" work — it's "keep our own CI fast and correct" work.

One PR worth watching for the talk: [#70541](https://github.com/apache/airflow/pull/70541) (open
as of 2026-07-27), "Allow providers to ship testing Dags separately from example Dags." A reviewer
(`jason810496`) argues explicitly for keeping test-only Dag support out of "prod code" entirely and
using a breeze-only test bundle instead — a live example of maintainers actively drawing the line
between testing infrastructure and shipped product, which is exactly the boundary this talk's
plugin needs to sit on the far side of.

### The separate, thinner "public" story for Dag authors

Two docs exist for *external* Dag authors, neither of which references `devel-common` by name for
tooling (one does reference a fixture, see below):

- **`contributing-docs/testing/dag_testing.rst`** — covers exactly two things: `dag.test()` run
  from an `if __name__ == "__main__":` block, and `airflow dags test <dag_id> <execution_date>` on
  the CLI. No pytest plugin, no fixture library, no mocking helpers.
- **`airflow-core/docs/best-practices.rst`, "Testing a Dag"** — shows a bare, hand-rolled
  `DagBag()` fixture the user must define themselves, a structural-equality helper for Dag
  topology, and a custom-operator test using bare `dag.test()` → `dagrun.get_task_instance(...)`.
  No `dag_maker`, no db-lifecycle handling, no XCom assertion helpers.
- **`airflow-core/docs/core-concepts/dags.rst`, "Testing a Dag"** contains one direct pointer from
  public API docs into the unpublished package: *"Using the call within a pytest test suite, you
  may benefit of the Airflow pytest plugin's `conf_vars` fixture..."* — `conf_vars` lives in
  `devel-common/src/tests_common/test_utils/config.py`, which ships only in the unpublished
  `apache-airflow-devel-common` distribution. An external Dag author following this doc literally
  cannot `pip install` this fixture. This is a concrete, citable documentation/packaging
  inconsistency worth naming in the talk.

Task SDK (`task-sdk/`) itself has **no separate public testing surface** — no `testing.py`, no
`pytest11` entry point. Its own test suite depends on `tests_common.pytest_plugin` exactly like
everything else.

---

## 2. The production fork — what it already does beyond upstream

**Scale.** The fork's conftest bootstrap sits in front of the predecessor's core plugin module
(a near-verbatim fork of upstream's ~3,125-line plugin), plus `tests/common/test_utils/*`
(mostly file-for-file mirrors of upstream, plus one fork-only addition).

**Novel engineering, not present upstream:**

1. **`dictConfig` monkeypatch reattachment** (in the fork's conftest bootstrap). Airflow calls
   `logging.config.dictConfig` at unpredictable points (Dag collection, provider init, API-server
   startup, `caplog` teardown) and each call wipes root's handlers. Rather than reattach at every
   scattered call site, the fork monkeypatches `dictConfig` itself so the fix applies at the one
   choke point. This is a genuinely reusable pattern for *any* Airflow test harness.

2. **Two-tier FileLock xdist coordination with independent completion markers**
   (in the fork's conftest bootstrap). One `FileLock` guards two *independently gated* concerns — DB
   init and API-server/user init — via separate marker files, so a failure in one doesn't
   permanently wedge retries of the other. This exact bug (shared marker across unrelated
   concerns) was called out by name in the fix commit.

3. **Shared-single-SQLite-file-across-xdist-workers model**, with a dedicated busy-timeout fix
   (`tests/common/test_utils/sqlite.py`, `SQLITE_TEST_CONNECT_ARGS = {"timeout": 30}`). Upstream
   Airflow's CI instead runs one DB per worker/container; this fork's lighter-weight alternative
   needed its own contention fix for SQLite lock contention and `dag_maker` ID collisions under
   xdist.

4. **xdist-safe `dag_maker` defaults.** The same commit made `dag_maker`'s default `dag_id`/`run_id`
   derive from `request.node.nodeid` instead of upstream's fixed `"test_dag"`/`"test"` literals —
   fixing a real class of collision bugs that surfaces specifically once all workers share one DB
   file. This is a strictly better default than upstream's and worth proposing back independently
   of any public-package release.

5. **Production-host environment-sentinel marker** — a declarative marker plus
   an autouse-style skip that defaults to skipping any so-marked test unless running on a host
   where a designated sentinel path exists. The *pattern* (skip a class of tests unless a
   designated "real environment" sentinel check passes) generalizes to any org with a
   prod-like-host-vs-dev-machine split; the specific sentinel path does not.

6. **`run_task_instance()` compat shim** (in the fork's task-instance compat shim) for
   Airflow 3.2's removal of `TaskInstance.run()`
   ([apache/airflow#59835](https://github.com/apache/airflow/pull/59835)). On Airflow ≥3.2 it
   delegates to `airflow.sdk.definitions.dag._run_task` (the same helper `Dag.test()` uses),
   deliberately skipping dependency-check machinery to match `_run_task`'s own semantics, and calls
   `session.expire_all()` afterward since `_run_task` executes in a subprocess whose commits the
   caller's session won't otherwise see. This is load-bearing glue every other Airflow-3.2 test
   author will independently need to reinvent — packaging it centrally has real value.

7. **Self-testing discipline.** The fork's own plugin self-test suite unit-tests the
   plugin's *own* conftest machinery — FileLock coordination semantics (simulated with
   `threading.Thread` against a temp lock file), the `dictConfig` reattachment behavior, and
   `pytest_sessionfinish` master/worker branching. Testing test-infrastructure this rigorously is
   uncommon and a strong credibility signal for a public release aiming for adoption trust.

8. **A real `api_test` marker delivering a genuinely live API server.** Tests marked `api_test`
   auto-get `need_serialized_dag(True)`, and the fixture stack starts an actual
   `airflow api-server` process (`multiprocessing.Process` running the real CLI entrypoint, not a
   mock or `TestClient`) exactly once per session, gated by the same FileLock, torn down only on
   the xdist master at `pytest_sessionfinish`. This closes a gap that background research
   correctly identified as hard: testing code that calls Airflow's own REST API.

**Explicit upstreaming intent already on record.** The fork's `pyproject.toml` carries a
TODO noting intent to upstream `tests/common` into `apache-airflow-devel-common` — i.e., you had already
flagged this exact direction before this research pass, which the mailing-list findings below
confirm is a dead end *as an upstream contribution*, but a green field *as an independent package*.

**What must be stripped before any public release:** hardcoded company hostnames, internal
filesystem paths, production-host sentinel defaults, private package indexes, proprietary-tool
naming, and private repository history. The `tests/common/` vs. `tests/conftest.py` split is already a
clean boundary for this: `tests/common/*` is >90% a faithful upstream mirror with the genuinely
novel, vendor-neutral engineering concentrated in `tests/conftest.py`'s environment-isolation block
and the environment sentinel — which is good news, since it means the hard design work is
already largely reusable as-is.

---

## 3. The `dev@`/`devel-common` mailing-list thread — the single most relevant primary source

**Thread: "[DISCUSS] Turn 'tests_common' into separate distribution for development,"** Feb 16 –
Mar 6, 2025. Root: <https://lists.apache.org/thread/jonodvjnycv01qcsnlqczpvrjhfsx04g>. This is the
thread that created today's `devel-common`, merged as PR
[#47281](https://github.com/apache/airflow/pull/47281). Jarek Potiuk's original proposal was purely
about uv-workspace packaging mechanics (per-provider standalone `uv sync`, PYTHONPATH cleanliness).

Directly on point, Alexander Shorin (`kxepal`) asked for exactly what this talk's plugin proposes:

> "I would love to see some `airflow_testing` package which will be useful for testing
> airflow-related projects and involve independently. Certainly, it's not a good thing to have
> tests import something from tests... Also this project could have a future for testing,
> compatibility, quality and rest of measuring."

Jarek Potiuk's reply (quoted here verbatim, fetched and verified directly against the raw message
body, not a paraphrase) is the clearest on-record maintainer position on this exact question:

> "IMHO It's extremely unlikely we are going to release and publish the common test code /
> fixtures in any way. They will continue to be in development-only-distribution and they will be
> treated as 'internal detail'. If we decide to release and publish them, we will have to maintain
> backwards compatibility and account for our users (like you) using them for their own purpose.
> That would block us or make it very difficult to make breaking changes in them. So while you
> will be free to continue copying the whole distribution and use it in your tests as you want
> (our licence allows that) — I seriously doubt we will ever release and publish it in 'reusable
> form' with 'compatibility guarantees'. It's far more efficient if people like you just copy them
> and are aware that they can change any time."

A related follow-up thread, **"[DISCUSS] Improve test package hierarchy?"** (Jul 28 – Aug 3, 2025,
<https://lists.apache.org/thread/tz9w8t6k68mnjqgbyj2z5vf73hbp4zwy>), has Potiuk drawing the same
line again — `devel-common` classes are fine to import as regular `src` classes internally, but
that's categorically different from making them a supported external API.

**Reading for the talk:** this is not maintainer ambivalence — it is an explicit, reasoned
rejection of ever publishing `devel-common` with compatibility guarantees, for exactly the reason
you'd expect (it would freeze their ability to make breaking changes to their own internal test
harness), paired with an explicit statement that copying the code is licensed and expected. That
is the strongest possible mandate for exactly the approach this talk's plugin takes: not
"upstream `devel-common` publicly," but "build an independent, generalized package descended
from it, with its own compatibility contract, decoupled from Airflow's internal refactor cadence."

### Earlier, less conclusive precedent (2017)

**Thread: "Airflow Testing Library,"** May 5–18, 2017. Root:
<https://lists.apache.org/thread/9zbndfvsk5fmoj2jvgk3mgkpt8lkdjcj>. Sam Elamin opened a genuine,
well-attended discussion (Industry Dive, Airbnb, and others weighed in with real in-house
Dag-testing approaches — mocked `BackfillJob` invocations, dummy-task verification, Docker-based
golden-file end-to-end tests) about building a shared Dag-testing library. A call was held; Elamin
then said discussion would continue off-list. **No AIP number was ever assigned, no package
shipped, and no dev@ follow-up describing an outcome exists.** This is likely the seed of what
later became `dag.test()`/`DagBag`-based testing utilities inside Airflow itself, but it never
became — and was never conclusively *rejected* as — a standalone package; it simply went quiet.
Useful as "this idea has organic roots going back nine years," not as evidence of a specific
maintainer verdict.

### General testing-philosophy threads (no devel-common mention, high signal on how the core team invests in test infra)

- **"[PROPOSAL] ...splitting to db/non-db tests"** (Oct 2023,
  <https://lists.apache.org/thread/bbojb6kqbdvqcshnh80hqw0vhtd6ccwn>) — Potiuk's data-driven case
  for the `db_test` marker split, citing measured ~45%/~30% CI time reductions. Origin of the
  `--skip-db-tests`/`--run-db-tests-only` flags this repo's own `CLAUDE.md` documents.
- **"[DISCUSS] Tests structure for providers"** (Feb 2025,
  <https://lists.apache.org/thread/xm6th19xw2v1zo8k8kjomsbzz51njxgv>) — established the
  `tests/unit`/`tests/integration`/`tests/system` convention; Potiuk's stated long-term goal is
  making **all** provider tests non-DB / Task-SDK-based, since "with Task SDK, provider code won't
  have a DB to talk to at all" — a forward-compatibility argument, not just a speed one.
- **"[PROPOSAL] Remove creation of real Airflow connections in provider unit tests"**
  (Feb–Jun 2025, <https://lists.apache.org/thread/5ygm6sl5fox0qoywo5w6ojj3c7mtw6mo>) — landed;
  reframed by Potiuk as a Task SDK forward-compat requirement, same theme as above.
- **"[LAZY CONSENSUS] Remove caplog usage from Unit Tests"** (Feb 2025,
  <https://lists.apache.org/thread/f7t5zl6t3t0s89rt37orfcv4966crojt>) — origin of this repo's own
  rule against asserting on raw `caplog.text`.

---

## 4. The 2021 `pytest-airflow` claim (debunked)

The PDF background material claimed: *"In a 2021 discussion on GitHub, a user proposed creating a
pytest-airflow plugin for end users... Airflow maintainers welcomed the idea in principle... They
suggested that as long as contributors are willing to keep it updated, it could even live in the
Airflow repository."*

This does **not** check out as described:

- The real, closest-matching thread is **GitHub Discussion
  [#18195](https://github.com/apache/airflow/discussions/18195)**, "Create pytest plugin for
  prepare test environment," opened 2021-09-09 by `andrey-anshin-godel`. The actual ask was
  narrower — a plugin to set up env vars and initialize the DB, not a full end-user testing SDK —
  and user `Taragolis` (not a maintainer) floated the `pip install pytest-apache-airflow` framing.
- Maintainer `potiuk`'s actual, on-record response was **more skeptical than "welcomed," and
  pointed the opposite direction from "could live in the Airflow repo":** *"...if you feel like
  creating such plugin as your own open-source project - feel free... The thing that it does not
  matter if it's in your repo or airflow - it can become stale... If you take a look there are NO
  end-users tools 'around' testing and 'deploying' airflow in Airflow repo... we have no intention
  to bring those or implement similar stuff in Airflow because it's not 'core' of airflow."* That
  is a steer toward *"build it externally, we won't take it in-repo,"* not the PDF's "could even
  live in the Airflow repository."
- The specific project name `pytest-airflow` is real, but unrelated in concept:
  [`Flowminder/pytest-airflow`](https://github.com/Flowminder/pytest-airflow) (created 2018,
  archived, last pushed 2021-04) runs pytest *test functions* **as** Airflow DAG **tasks** — the
  inverse of testing Dags with pytest. It's still resolvable on PyPI (`pip install pytest-airflow`)
  but is not what the PDF describes, and is very likely what got conflated with an "official
  discussion" in the source material.

**Bottom line:** don't cite the PDF's framing. If the talk wants a 2021-era citation, cite
Discussion #18195 directly and use potiuk's actual quote above — it's a more interesting, more
citation-safe story anyway: *maintainers have twice, four years apart (2021 and 2025), independently
and explicitly told the community "build this outside the repo, we won't own it publicly."* That
consistency is a stronger talk point than a fabricated "maintainers welcomed it."

---

## 5. GitHub — no competing effort, one adjacent-but-different naming collision to flag

No open or closed PR/issue/discussion on `apache/airflow` in the last 12 months proposes splitting
`devel-common`/`tests_common` into an independently published PyPI package, and no one has already
shipped `apache-airflow-devel-common` or a similarly-named package — **that namespace is open and
uncontested as of 2026-07-28.**

Two existing PyPI projects use adjacent-but-different naming and should be explicitly
differentiated in the talk, since attendees will likely have seen the names:

- **`airflow-pytest-plugin`** ([IKrysanov/airflow-pytest-plugin](https://github.com/IKrysanov/airflow-pytest-plugin),
  created 2026-06-20, actively pushed as recently as 2026-07-28) — a JUnit-result-archiving
  dashboard for flaky-test detection *inside* the Airflow 3 UI, paired with
  **`airflow-pytest-operator`** ([IKrysanov/airflow-pytest-operator](https://github.com/IKrysanov/airflow-pytest-operator))
  which runs pytest *as* an Airflow task. Both are about running/reporting on pytest suites
  orchestrated *by* Airflow — the inverse of testing Dags *with* pytest outside a live instance.
- **Astronomer's own testing docs** stop at recommending plain `pytest` + a hand-rolled `DagBag`
  fixture, `dag.test()` for single-process runs, and third-party data-quality tools (Great
  Expectations, Soda Core) for data checks — no packaged harness of their own. This is a legitimate
  gap-in-the-ecosystem citation: even the most Airflow-adjacent commercial vendor doesn't ship what
  this talk's plugin will ship.

Recent substantive PR activity on `devel-common` itself (SQLAlchemy 2.0 migration, collection-time
DB-test deselection, fixture cleanup correctness, flaky-DB retry logic — see §1) is entirely
internal-CI-hardening in character. There is no visible internal appetite to generalize it for
external consumption.

---

## 6. Gap analysis

Putting upstream's current scope, your fork's additions, and the community's stated position
together, the gaps a public release should close are:

1. **No installable "verify a Dag without a live Airflow instance" story for outsiders exists at
   all.** `dag.test()` and `airflow dags test` need a locally configured Airflow environment (DB,
   `AIRFLOW_HOME`); they are not zero-infra unit-test tools. The genuinely infra-free path
   (`create_runtime_ti`/`run_task`/`in_process_taskrun.run_task_no_db`) exists only inside the
   unpublished `devel-common`.
2. **The one place public docs point at `devel-common` (`conf_vars` in `core-concepts/dags.rst`) is
   a dead end for anyone outside the monorepo.** A public package should ship this fixture (or
   equivalent) under a name Dag authors can actually `pip install`.
3. **No packaged compat shim for the Airflow 3.2 removal of `TaskInstance.run()`.** Your fork
   already solved this (`run_task_instance()`); every other Airflow-3.2 adopter is currently
   reinventing it independently.
4. **No xdist-safe, DB-light harness for orgs that don't want a DB-per-worker CI topology.** Your
   fork's shared-SQLite-plus-busy-timeout model and two-tier FileLock coordination are a genuinely
   novel, documented alternative to what upstream assumes.
5. **No real, testable path to exercising the Airflow REST API in a pytest run.** Your `api_test`
   marker + live subprocess API server closes this; nothing upstream or in the wider ecosystem
   does.
6. **No independent public artifact offers a maintained compatibility contract for testing Dags
   with pytest.** Maintainers have twice explicitly declined to own that artifact themselves (2021,
   2025) while encouraging external projects. That's the opening.

### What's already provably ready to generalize vs. what's environment-specific

Ready to generalize as-is or with light parameterization: the `dictConfig` reattachment pattern,
the two-tier FileLock/marker-file xdist coordination, the shared-SQLite busy-timeout fix, the
`dag_maker` nodeid-derived default IDs (also worth upstreaming independently — it's a strict
bugfix), the `run_task_instance()` Airflow-3.2 compat shim, the `api_test` live-server pattern, and
the self-testing-the-harness discipline. Needs genuine design work to generalize: the
production-host sentinel mechanism (keep the pattern — a configurable "real-environment" marker —
drop the hardcoded path) and deciding how much of upstream's `tests_common` to re-vendor
byte-for-byte versus depend on `apache-airflow-devel-common` directly as an internal dependency
(your fork's own `pyproject.toml` TODO already flags this exact fork point).

---

## 7. Naming and publishing decisions (settled 2026-08-06)

- **Package/PyPI name: `pytest-airflow-in-a-box`.** Verified available on PyPI (HTTP 404 on
  `https://pypi.org/pypi/pytest-airflow-in-a-box/json`) and on GitHub. Keeps the conventional
  `pytest-*` prefix for ecosystem discoverability while matching the talk title.
- **Repo: `nredd/pytest-airflow-in-a-box`** (verified: `nredd` exists, 14 public repos; that repo
  name is free). Published to PyPI from the same account.
- **`pytest-airflow` is NOT obtainable and should not be pursued.** It is live on PyPI (v0.0.3,
  [Flowminder/pytest-airflow](https://github.com/Flowminder/pytest-airflow)). PyPI has no
  abandonment policy of its own — it defers entirely to
  [PEP 541](https://peps.python.org/pep-0541/), which requires **all three** of: owner unreachable
  after ≥3 contact attempts over a 6-week window, no releases in 12 months, and no home-page
  activity. The repo is archived (a *voluntary* owner flag, which does **not** free the name) and
  last pushed 2021-04-20, but Flowminder is an extant org, so reachability is unestablished. PEP 541
  further holds *reuse for an unrelated project* to a stricter bar than continuation, and explicitly
  states that owning the name elsewhere does not override an existing claim. Semantically it's also
  wrong for us: that plugin runs pytest tests **as** Airflow Dag tasks — the inverse of this project.
- **`pytest-dag` is also off-limits** — live, actively maintained (v3.14.15, 2026-06-02), and
  unrelated (enforces test execution order via a dependency DAG). Direct confusion risk.

---

## 8. Database architecture: how multi-backend support actually works

### SQLAlchemy abstracts the query layer, not database *behavior*

Airflow's models, queries, and its 129 Alembic migrations are written once against SQLAlchemy and
run on SQLite/Postgres/MySQL. **The plugin will not write a single dialect-conditional query** —
that work is already done upstream and is not ours.

What SQLAlchemy does *not* abstract is everything around provisioning and runtime semantics.
Airflow's own `airflow-core/src/airflow/settings.py` is the proof — even with SQLAlchemy doing the
heavy lifting, it still hand-branches on dialect throughout:

| Concern | Dialect-specific handling in `settings.py` |
|---|---|
| Pool class | In-memory SQLite gets `SingletonThreadPool`, which **cannot accept** `pool_size`/`max_overflow`, so Airflow explicitly skips pool settings for it (`:555-557`). File-based SQLite gets `QueuePool`. |
| `connect_args` | SQLite needs `check_same_thread=False` (`:469-473`); meaningless elsewhere. |
| Isolation level | MySQL is forced to `READ COMMITTED` because its default `REPEATABLE READ` causes stale-snapshot bugs with multiple schedulers (`:603-609`). |
| Perf tuning | Postgres gets `insertmanyvalues_page_size=10000` + psycopg2-only `executemany_mode` (`:527-538`). |
| Type adapters | SQLite registers a pendulum adapter; MySQL patches converters and hard-requires `mysqlclient` (`configure_adapters`, `:653+`). |
| Migrations | 39 of 129 migration files contain dialect branches; offline migration is only supported from a limited revision on SQLite (`utils/db.py:1174-1186`); MySQL needs special migration locking (`:1198-1203`). |

**The reframe that matters:** Airflow accepts any SQLAlchemy URL via `[database] sql_alchemy_conn`.
So "support backend X" reduces, from Airflow's perspective, to *provisioning a server and handing
over a URL*. The plugin's abstraction is therefore a **provisioner**, not a data-access layer —
roughly `start() -> url`, `stop()`, `reset()`, plus backend-specific engine tuning. Airflow's own
`settings.py` then does all the dialect-correct engine work for us.

**Scope decision (2026-08-06): Postgres is dropped from v1.** Focus is a single, genuinely
well-tuned SQLite backend that performs on both target machines (below). The provisioner seam should
still be designed so a Postgres/Docker tier can be added later without restructuring, but no
Postgres code, dependency, or extra ships in v1. Prior research on the Postgres options is retained
in §9 for whenever that tier is revisited.

**Update (2026-08-07): the Postgres tier shipped**
([#5](https://github.com/nredd/pytest-airflow-in-a-box/issues/5)). The provisioner seam held: the
`Provisioner` protocol (`start() -> url`, `stop()`) added a testcontainers-backed Postgres
implementation with zero dialect-conditional queries, exactly as designed. testcontainers was
chosen over `pytest-postgresql` (no local Postgres binary required); Level-A single-shared-DB
topology was chosen over per-worker isolation (mirrors production, keeps the env handoff identical
to SQLite); `reset()` stayed deferred (`clear_db` + `dag_maker` still own per-test cleanup). See §9
for the provisioning survey and the NullPool/connection-exhaustion analysis that drove the design.

### The official, non-monkeypatch seam for engine customization

`settings.py:361-378` defines `create_metadata_engine(...)` with the docstring: *"Override in
`airflow_local_settings.py` to customize engine creation, e.g. to register `do_connect` event
handlers."* There is a matching `create_async_metadata_engine` (`:381-400`). **This is a documented,
supported override point** — the plugin can inject SQLite PRAGMA tuning without monkeypatching
Airflow internals. Note also that `sql_alchemy_connect_args` applies to **sync engines only** as of
Airflow 3.1; async needs `sql_alchemy_connect_args_async` (`config.yml:700-726`).

Critically, **`connect_args` cannot set PRAGMAs** — `sqlite3.connect()` has no pragma parameter. The
correct mechanism is a SQLAlchemy `connect` **event listener**, verified working locally:

```python
@event.listens_for(engine, "connect")
def _set_pragmas(dbapi_conn, _rec):
    cur = dbapi_conn.cursor()
    for k, v in PRAGMAS.items():
        cur.execute(f"PRAGMA {k}={v}")
    cur.close()
```
Confirmed: all PRAGMAs apply, and they persist correctly for brand-new pooled connections after
`engine.dispose()`. The fork's current `SQLITE_TEST_CONNECT_ARGS = {"timeout": 30}`
(`tests/common/test_utils/sqlite.py`) only sets the busy-wait — it is a strict subset of this.

### True in-memory SQLite is architecturally impossible here (measured)

Three independently fatal constraints, each verified empirically:

1. **WAL cannot coexist with in-memory.** `PRAGMA journal_mode=WAL` on `:memory:` silently returns
   `memory`, not `wal`. The two goals are mutually exclusive.
2. **Each connection to `:memory:` is a separate database.** Two connections; the second cannot see
   the first's tables. `?cache=shared` fixes this *within a single process only*.
3. **Cross-process sharing is impossible.** Directly tested: process B cannot see process A's
   shared-cache memory DB (`no such table`). The pages live in one process's heap; there is no
   mechanism to share them.

Constraint 3 is disqualifying, because this harness inherently spans processes: the live API server
(`api_test`), `PythonVirtualenvOperator`/`ExternalPythonOperator`, `run_as_user` tasks, and every
`pytest-xdist` worker all need the same metadata DB. There is also a lifecycle trap — a shared-cache
memory DB is **destroyed when the last connection closes**, and Airflow's `dispose_orm()` closes
pools routinely, which would silently vaporize the database mid-session.

**On `SingletonThreadPool` specifically** (the mechanism behind the constraint) — SQLAlchemy 2.0.51
picks the pool from the URL:

| URL | Pool |
|---|---|
| `sqlite://`, `sqlite:///:memory:` | `SingletonThreadPool` |
| `sqlite:////abs/path.db` | `QueuePool` |

`SingletonThreadPool` keeps one connection per thread — which is *why* in-memory works at all (that
connection **is** the database) and why it cannot be shared. It also hard-rejects pool tuning:
`create_engine("sqlite://", pool_size=5, max_overflow=10)` raises
`TypeError: Invalid argument(s) 'max_overflow' sent to create_engine()`. That is exactly why
`settings.py:555-557` must skip pool settings for in-memory SQLite. `StaticPool` is the usual
workaround (verified: it does make `:memory:` persist across `connect()` calls by reusing one
underlying connection) but it serializes everything through that single connection and still cannot
cross a process boundary — a dead end for this plugin.

### What to do instead: durability-off PRAGMAs, with tmpfs as a secondary win

The actual goal isn't "in memory" — it's **"never touch a spinning disk, never call fsync."** That
is achievable with a *file-based* DB, preserving WAL, `QueuePool`, and multi-process access.

Benchmark (300 inserts / 30 commits / 30 selects), one representative dev host, 12 CPU / 15 GiB RAM:

| Configuration | Time |
|---|---|
| pure `:memory:` (WAL impossible) | 0.5 ms |
| disk (ext4), **default** PRAGMAs | **103.3 ms** |
| disk (ext4), WAL + `synchronous=OFF` + mmap | **0.8 ms** |
| tmpfs `/dev/shm`, WAL + `synchronous=OFF` + mmap | 0.7 ms |
| tmpfs `/dev/shm`, default PRAGMAs | 1.8 ms |

**The PRAGMAs matter far more than the storage medium**: a tuned DB on real disk (0.8 ms) beats an
untuned one on tmpfs (1.8 ms), and lands within 0.3 ms of true in-memory — a ~130× improvement over
untuned disk. tmpfs is a marginal extra win, not the main event.

`synchronous=OFF` is the single biggest lever and is *safe here specifically* because a test
database is definitionally disposable: the only thing sacrificed is crash durability, and a crashed
test run discards the DB anyway. This is the rare case where the normally-reckless setting is
exactly correct.

**Recommended defaults:** `journal_mode=WAL`, `synchronous=OFF`, `temp_store=MEMORY`,
`mmap_size=256MB`, `cache_size=-131072` (128 MiB), `busy_timeout=30000`, `page_size=8192`.
`locking_mode=EXCLUSIVE` applies cleanly but must **NOT** be used — it would lock out the very
subprocesses the harness depends on. Caveat: `mmap_size` and `cache_size` are **per connection**, so
N xdist workers × 128 MiB is real memory pressure; these must scale down as worker count rises.

### Two-machine reality check — and the NFS trap

Measured on both target machines:

| | dev host | CI host |
|---|---|---|
| CPU / RAM | 12 / 15 GiB | **4** / 30 GiB |
| `/tmp` | ext4 | **xfs**, 11 GiB (45% used) |
| `/dev/shm` | tmpfs 7.7 GiB | tmpfs **16 GiB** |
| `$HOME` | ext4 (local) | **NFS** |
| Python | 3.12.3 | **3.6.15** (system) |
| SQLite lib | — | 3.50.2 |

VM benchmark, same workload:

| Configuration | Time |
|---|---|
| xfs `/tmp`, default PRAGMAs | 287.5 ms |
| xfs `/tmp`, **TUNED** | **1.4 ms** |
| tmpfs `/dev/shm`, **TUNED** | **1.3 ms** |
| NFS `$HOME`, default PRAGMAs | 238.1 ms |
| NFS `$HOME`, **TUNED** | **148.5 ms** |

**The critical finding: tuned-on-NFS is ~110× slower than tuned-on-local (148.5 ms vs 1.3 ms)** —
the PRAGMAs that buy a 200× speedup on local filesystems buy only ~1.6× on NFS, because the
bottleneck is network round-trips, not fsync.

Worse, this is a **correctness** problem, not just performance. WAL requires a shared-memory
sidecar (`-shm`) plus real POSIX byte-range locking. Verified that SQLite *does* create
`t.db-wal` + `t.db-shm` on NFS and *does* report `journal_mode=wal` — i.e. **it silently appears to
work.** But NFS locking is famously unreliable, and this is the classic SQLite corruption vector for
multi-process access. Since this harness deliberately shares one DB file across xdist workers and
subprocesses, **the database file must never live on NFS.**

**Design consequences:**
1. **Never place the DB under `$HOME`.** The plugin must resolve its own storage location rather
   than inheriting `AIRFLOW_HOME` or a home-relative default (the fork currently puts it under
   pytest's `cache._cachedir`, which is repo- and therefore potentially NFS-relative — a latent bug
   on the VM).
2. **Storage-location precedence:** explicit user override → `/dev/shm` (if tmpfs, writable, and
   sufficiently sized) → a local-disk temp dir (`/tmp`, verified non-network) → last-resort
   fallback with a **loud warning** if only a network filesystem is available.
3. **Detect network filesystems and refuse/warn.** `stat -f`/`os.statvfs` fstype detection ("nfs",
   "cifs", "smb", "fuse.sshfs", …) is the check; on match, either relocate automatically or emit a
   prominent warning, because the failure mode is silent corruption rather than a clean error.
4. **Scale memory PRAGMAs to the host, not to a constant.** 4 CPUs vs 12 changes xdist worker count
   and therefore total `mmap_size`/`cache_size` footprint. Derive from detected RAM/CPU
   (`os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')` works without `psutil`, which is
   absent on both boxes) rather than hardcoding 128 MiB per connection.
5. **tmpfs consumes RAM.** `/dev/shm` defaults to ~50% of RAM and is not free memory — sizing checks
   are required before placing a DB there, especially alongside a 16 GiB `/dev/shm` on a box also
   running real workloads.
6. **Don't assume the interpreter.** The VM's system Python is 3.6.15; the plugin targets a modern
   Python via `uv`/venv, but any *diagnostic* or bootstrap snippet that might run under system
   Python must not use modern-only syntax (f-string backslashes broke a probe script during this
   research).

### Implications had Postgres been kept (realized in the shipped tier)

For client-server backends the in-memory question largely dissolves — pages live in the server's
shared buffers, which all client processes already share. The parallel knobs would be: `initdb` with
the data directory on tmpfs, plus `fsync=off`, `synchronous_commit=off`, `full_page_writes=off`, and
raised `shared_buffers` (same "disposable data → disable durability" philosophy, same
order-of-magnitude payoff). Pooling inverts and becomes genuinely dangerous: Airflow's
`pool_size=5`/`max_overflow=10` are **per process**, so 12 workers → up to 180 connections against
Postgres's default `max_connections=100`. **The shipped tier's chosen lever is `NullPool`**, set
deterministically via `AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED=False` (Airflow's
`settings.py:prepare_engine_args` selects `NullPool` from that flag) — short-lived test workers gain
nothing from connection reuse. Concurrent Alembic migrations from multiple workers against one real
server is also a genuine hazard (inconsistent schema state, not just lock contention); the shipped
Level-A design sidesteps it by having the controller alone run `initdb` before workers connect.

Finally: SQLite-with-WAL and Postgres are **not behaviorally equivalent test targets**. Tests can
pass on SQLite and fail on Postgres (stricter type coercion, real transaction isolation, actual
concurrent writers). That's a feature of having the tier — but it means backends are a *fidelity
ladder*, not interchangeable options, and docs must say so.

---

## 9. Postgres/MySQL provisioning options (testcontainers chosen for the shipped tier)

Researched before Postgres was cut from v1; recorded so the evaluation doesn't have to be redone.
**The shipped tier ([#5](https://github.com/nredd/pytest-airflow-in-a-box/issues/5)) chose
`testcontainers`** (bottom of this list): the target machines have Docker but no local Postgres
binary, so it runs both locally and on `ubuntu-latest` CI where `pytest-postgresql`/`pgserver`
would not.

- **`py-pglite` is a trap — do not adopt.** Markets itself as "zero config, no Docker," but its
  `manager.py` runs `npm install` and spawns `node pglite_manager.js`, making **Node.js a hard
  runtime dependency**. Worse, its docs state PGlite Socket "supports only one active connection at
  a time" — strictly *worse* than SQLite for the concurrency scenarios that would justify the tier.
- **`pgserver`** ([PyPI](https://pypi.org/project/pgserver/)) is the architecturally correct model —
  real Postgres 16.2 binaries baked into platform wheels (manylinux, macOS x86_64/arm64, Windows),
  no local install needed. But it is **effectively unmaintained** (v0.1.4, June 2024). Several forks
  show more recent activity. Worth a spike, not a dependency to take today.
- **`pytest-postgresql`** ([PyPI](https://pypi.org/project/pytest-postgresql/), v8.1.0 May 2026) is
  the pragmatic middle tier: actively maintained, mature session-scoped `postgresql_proc` fixture,
  template-database cloning for fast per-test isolation — but requires a **local Postgres binary**.
- **`testing.postgresql`** — dead (v1.3.0, Feb 2016). Do not use.
- **MySQL: no viable local-binary-free path exists.** No bundled-binary wheel analogous to
  `pgserver` exists for MySQL/MariaDB. `pytest-mysql` (v4.0.0, Apr 2026) requires a local
  `mysqld`/`mysqladmin`. **Do not force parity** — route MySQL to Docker if it's ever needed.
- **`testcontainers`** ([PyPI](https://pypi.org/project/testcontainers/), v4.15.0 Jul 2026) is the
  power-user/CI path for both Postgres and MySQL via one actively maintained package. Modules now
  live under `testcontainers.community.postgres` / `.mysql` (the old standalone
  `testcontainers-postgres`/`-mysql` PyPI packages are dead `0.0.1rc1` stubs — don't reference
  those). Requires a Docker daemon; recommended xdist pattern is a **filelock-guarded single shared
  container with one migration**, not one container per worker.

Notably, **Airflow's own CI never attempted an embedded-Postgres tier** — Breeze goes straight to
Docker Compose (`ALLOWED_BACKENDS` in `dev/breeze/.../global_constants.py:70`) for anything beyond
SQLite. That's a real signal about the ecosystem's ceiling.

Also relevant: `devel-common`'s `--backend` option and `SUPPORTED_DB_BACKENDS = ("sqlite",
"postgres", "mysql")` (the fork's plugin module) already exist upstream but are **inert in the fork** —
they validate the flag and do nothing, because Breeze/Docker provisioned externally. Making that
flag mean something *without* Breeze is precisely this plugin's novel contribution.

---

## 10. `test_utils` disposition — what ships in v1, what doesn't

Settled 2026-08-06 by auditing **real usage** across ~2 years of the fork's production tests
(`grep`-counted fixture/import usage in the fork's `tests/unit`), not by re-deriving from first principles.

Fixture/marker usage counts that drove every decision below:

| Fixture/util | Real usages | v1 verdict |
|---|---|---|
| `session` | 1064 | **ship** |
| `dag_maker` | 371 | **ship** |
| `caplog` (structlog override) | 38 | **ship** |
| `full_dag_bag` (fork-local) | 14 | **ship**, generalized |
| `run_task_instance` | 14 | **ship** |
| `run_task` (Task SDK path) | 12 | **ship** |
| `clear_db_*` | 2 files | **ship**, reduced |
| `create_dummy_dag`, `create_task_instance`, `create_runtime_ti`, `mock_supervisor_comms`, `cap_structlog`, `testing_dag_bundle`, `create_connection_without_db`, `create_dag_without_db`, `mock_xcom_backend`, `conf_vars` | **0 each** | **defer** |

Markers actually used: `db_test` (112), `compat` (122), `api_test` (22),
`real_environment` (17),
`smoke` (9), `xdist_group` (4), `need_serialized_dag` (3).

**The load-bearing surface is ~6 fixtures and ~5 markers.** Upstream's 5,767-line `test_utils/` is
mostly provider/system-test infrastructure that should not be inherited.

### Drop outright (~2,000 lines) — provider/system-test infrastructure

`gcp_system_helpers.py`, `azure_system_helpers.py`, `salesforce_system_helpers.py`,
`sftp_system_helpers.py`, `system_tests.py`, `system_tests_class.py`, `terraform.py`,
`integration_setup.py`, `logging_command_executor.py`, `otel_utils.py`, `get_all_tests.py`,
`providers.py`, `hdfs_utils.py`, `aiohttp.py`, `common_sql.py`, `common_msg_queue.py`,
`perf/perf_kit/`, `stream_capture_manager.py`, `reset_warning_registry.py`,
`file_task_handler.py`, `log_handlers.py`, `permissions.py`, `fernet.py`, `watcher.py`,
`format_datetime.py`, `file_loading.py`, `fake_datetime.py` (superseded by `time-machine`, itself
not a dependency — see §12), `mapping.py`, `asserts.py`, `executor_loader.py`, `dag.py`, `paths.py`.
These exist to test *Airflow itself* and *providers against live cloud services*; a Dag author
testing their own Dags needs none of it.

### Drop — mocking modules (answering "is this all really needed?")

`mock_operators.py`, `mock_executor.py`, `mock_plugins.py`, `mock_context.py` — all fakes of
**Airflow's own internals**, built so Airflow can test Airflow (a fake `BaseExecutor` subclass, fake
operators with fake extra-links). Zero usages in the fork. A Dag author mocks *their own* hooks with
stdlib `unittest.mock` (169 real usages in the fork vs. 2 for `pytest-mock`'s `mocker` — see §12).
Where v1 *does* need a fake (Task SDK `SUPERVISOR_COMMS`), it's a hand-written `Protocol`-conformant
class, not `mock.create_autospec` — the `ty`-clean choice (§ typing).

### Drop — `timetables.py`

Fixtures for testing custom `Timetable` implementations. Writing a custom timetable is rare/advanced
— those authors are testing an Airflow extension point, not a Dag. Zero usages; revisit on request.

### Drop — API helpers, replaced by core (answering "can we rely on core now?")

`api_fastapi.py` is entirely private helpers for asserting on Airflow's *own* REST responses.
`api_client_helpers.py` is thin `requests` wrappers re-deriving host/token per call. With the REST
API v2 stabilized, v1 ships **one small typed client** bound to the `api_test` server fixture
(base URL + auth token already known) instead of free functions — net reduction, better API.

### Keep, heavily reduced — `db.py`: 1047 → ~150 lines

**481 of the 1047 lines are a hand-copied default-connections table.** Core Airflow now exposes
`get_default_connections()`/`create_default_connections()` directly
(`airflow-core/src/airflow/utils/db.py:201,209` — confirmed present and public). Delete the copy;
call core. Of the 26 `clear_db_*` functions, keep only what a Dag author touches (dags, runs, task
instances, serialized dags, xcom, variables, connections, assets, logs, bundles); drop
core-internals ones (import errors, jobs, pools, FAB tables). Implement as one
table-registry-driven `clear_db(*, tables=None)` instead of 26 near-identical functions.

### Keep as-is (vendored, high value) — `taskinstance.py`

`run_task_instance()` (14 usages) is the compat shim for `TaskInstance.run()`'s removal in Airflow
3.2 ([apache/airflow#59835](https://github.com/apache/airflow/pull/59835)) — load-bearing glue
every 3.2 adopter must otherwise reinvent. Its docstrings already explain *why* dependency-checking
is bypassed and *why* `session.expire_all()` is required; vendor with attribution, keep the
reasoning intact. `ordered_task_instances()` travels with it (a real, subtle correctness fix —
`DagRun.get_task_instances()` sorts alphabetically by `task_id`, not by dependency order, which
silently breaks the 3.2 execution path).

### Keep, reduced — `in_process_taskrun.py` + Task SDK fixtures

The DB-free, xdist-safe task execution path (`run_task`, 12 usages) is the single most compelling
"test without infrastructure" feature. Keep it; retype it cleanly (Protocol-based, no
`create_autospec`).

### Unify — config/env (answering "this deserves unification, right?")

Yes. Upstream's `config.py` has three overlapping things (`conf_vars`, `env_vars`,
`create_fresh_airflow_config`). Ship **one** context manager, usable as decorator or CM, handling
Airflow config keys *and* env vars through one code path. Keep `conf_vars` as a deprecated alias —
it's the one fixture *public Airflow docs* name by name (`core-concepts/dags.rst`), so the
migration path must exist even though it's a dead link today (see §2).

### Condense — logging: 9 layers → 3

Per the fork's own git history (§ below), the same bug ("root logger's handlers get wiped, pytest's
log file goes silent") was fixed reactively three separate times
before the root cause — Airflow's `logging.config.dictConfig` unconditionally stripping root's
handlers at unpredictable points — was addressed directly instead of patched at each newly
discovered call site. Minimal correct footprint for v1: (1) a `dictConfig` interception that
reattaches handlers after every call, regardless of which Airflow code path triggered it; (2) one
idempotent `ensure_handlers()` helper; (3) one `logging.Filter` for worker/test attribution
(orthogonal — attribution, not loss-prevention). **Dropped**: the per-test autouse fallback fixture
(superseded — the interception fires on every `dictConfig` call, not just checkpoints), dead
vendored code (nothing to diff against once we're not re-syncing from upstream), and — critically —
coverage/XML-path bookkeeping, which is not a logging concern and moves to its own function. That
split also makes both functions trivially typeable.

### Keep — `logs.py` reduced to `StructlogCapture`

Airflow 3 logs via structlog, so plain `caplog` misses records; 38 real usages make this
load-bearing. Keep the capture shim; drop `check_last_log` (asserts on Airflow's `Log` table —
core-internals testing, not ours).

### Redesign — `compat.py`/`version_compat.py`: capability-based, not version-based

The most important architectural decision for long-term survival. Upstream's pattern — bare
`try/except ImportError` scattered across ~10 blocks, plus `AIRFLOW_V_3_X_PLUS` booleans checked
inline at call sites, in a file explicitly commented *"THIS FILE IS COPIED MANUALLY IN OTHER
PROVIDERS DELIBERATELY"* — is exactly what makes upstream unpublishable: version branches leak into
every fixture.

**v1 design:** one `_compat/` subpackage is the *only* place that imports non-public Airflow paths
or branches on version; everything else imports from `_compat`, never from Airflow internals
directly. Prefer **capability probes** (`HAS_RUNTIME_TASK_RUNNER`) over version checks
(`AIRFLOW_V_3_2_PLUS`) — versions are a lossy proxy for capabilities (vendors backport). Fail loudly
once at session start by resolving every required internal symbol and raising one actionable error,
rather than an `AttributeError` 400 tests deep. Back this with a documented support matrix and a CI
job per supported Airflow minor. This is the plugin's core value proposition: absorb Airflow's
internal churn behind one seam so Dag authors never see it.

### Drop the fork-specific production-host marker, keep the pattern

Generalize to a configurable "this test needs a real environment" gate (sentinel path from a
`[tool.pytest-airflow-in-a-box.environments]` table), same mechanism, zero hardcoded paths.

---

## 11. Bootstrapping architecture — replacing lockfiles with controlled early state

The fork's `pytest_configure` does, in one function: locate a venv, raise if missing; resolve
AIRFLOW_HOME under `.pytest_cache`; sanitize `sys.path` and `$PATH` against hardcoded allow-lists
(including environment-specific tool paths); pop ambient `PYTHONPATH`; force `--keep-env-variables`
onto `sys.argv`; and — inside a `FileLock` — elect an API port by having every xdist worker
independently call `get_open_port()` and race to write `airflow.cfg` first, with late arrivals
reading the winner's choice back out of the config file. A second `FileLock`, at
`pytest_collection_finish`, separately gates DB init and API-server/user init behind independent
completion-marker files. State is threaded through a `Protocol`-typed `_ExtConfig` class and accessed
via `cast(_ExtConfig, config)` at every call site — a `cast` at every read, because `pytest.Config`
has no typed extension point for plugin-owned state.

The controller can remove lockfile convergence, but later validation found that `workerinput` alone
is insufficient: it is attached after worker plugins and conftests may already import Airflow. The
correct sequence is:

```
pytest_load_initial_conftests       ← CONTROLLER computes pre-import environment
pytest_sessionstart(tryfirst=True)  ← CONTROLLER initializes DB and starts API
pytest_configure_node → workerinput ← CONTROLLER sends validation state
worker process inherits environment before plugins/conftests import Airflow
pytest_configure(is_worker=True)    ← WORKER validates state into StashKey
```

The environment handoff is deliberately narrow and contains only the run root and Airflow settings
that must exist before import. Typed runtime state moves to `pytest.StashKey` as soon as `Config`
exists. Serial runs use the same owner path without `workerinput`.

```python
state = load_early_environment()
validate_worker_state(config, state)
```

This dissolves three problems the fork currently solves with files-on-disk:

1. **Port election** — the controller binds and retains a loopback socket, then passes it to a
   module-level spawn-compatible API child. No close-and-rebind race or config-file convergence.
2. **Cross-worker state** — inherited environment supplies pre-import state; `workerinput` validates
   JSON-serializable state after worker configuration.
3. **The `cast(_ExtConfig, config)` pattern** — replaced by `pytest.StashKey`, the sanctioned typed
   extension point for `pytest.Config`. Every read becomes type-safe with zero casts.

**What still genuinely needs synchronization:** DB initialization and API startup run in
controller-side `pytest_sessionstart(tryfirst=True)` before xdist starts local workers. API readiness
means obtaining a token from Airflow's built-in `SimpleAuthManager` and calling the authenticated
health endpoint, not merely opening a TCP connection. `Config.add_cleanup` covers partial startup;
normal teardown is controller-owned and process-object based. Remote xdist workers remain out of
scope because they cannot share the controller's SQLite file, filesystem, or loopback server.

`sys.path`/`$PATH` sanitization and `PYTHONPATH` popping are a separate concern (process isolation
against ambient site tooling) and should not live inside the same function as AIRFLOW_HOME/DB
bootstrapping — they get their own narrowly-scoped, independently testable step.

---

## 12. Custom collection of user-supplied Dag files

Goal: a user drops a real Dag `.py` file into a directory (e.g. `tests/dags/`), and the plugin
parses/validates it, registers it into the test DB, and collects it as one or more pytest test
items — auto-marked `db_test`/`api_test` as appropriate — including a path to define multiple
pinned-`Param` test cases per Dag file.

**Verified working end-to-end** with a real prototype (`pytest_collect_file` → custom
`pytest.File`/`pytest.Item` subclasses):

- `pytest.File.from_parent(parent, path=file_path)` and `pytest.Item.from_parent(self, name=..., **extra)`
  are the required constructors (direct `__init__` construction is deprecated).
- **Multiple parametrized items per file** is done by `collect()` yielding one `Item` per test case,
  *not* by `pytest_generate_tests`/`metafunc.parametrize` — confirmed those hooks don't compose with
  custom collectors. A `DagFile.collect()` that yields `DagCase.from_parent(self, name=case_name,
  params=pinned_params)` per declared case is the mechanism for "let users define test cases which
  pin `DagRun` `Param`s."
- **Markers are injected at collection time** via `self.add_marker(pytest.mark.db_test)` inside the
  `Item`'s `__init__` — verified `-m db_test` correctly selects/deselects on dynamically-collected
  items exactly like statically-declared ones.
- The Dag `.py` file is never imported by pytest's default Python collector (verified via a
  deliberate `raise SystemExit` guard in the probe file, which never fired).

**The real trap, confirmed by reproduction:** if a Dag file happens to match pytest's default
`python_files` pattern (e.g. named `test_something.py`, or passed directly on the command line —
`isinitpath` bypasses the pattern check entirely), pytest's *default* Python collector ALSO
collects it, producing duplicate/spurious items (measured: 5 items collected where 4 were correct,
including a stray `test_would_be_collected_by_default` that should never have existed as a test).
`collect_ignore_glob = ["*.py"]` is **not** the fix — it's too blunt and suppresses our own custom
collector too (verified: drops to zero collected items). The working fix is
`@pytest.hookimpl(tryfirst=True)` on `pytest_collect_file` plus an explicit dedupe pass in
`pytest_collection_modifyitems` that drops any default-collector item whose path falls under the
Dag directory and isn't one of our own `Item` subclass instances.

This is a real, non-obvious design surface (naming collisions between "files that happen to look
like tests" and "files that are Dags") and needs its own dedicated test coverage in the plugin's
self-tests, not just a happy-path example.

---

## 13. Dependency and configuration-boilerplate audit

Driven by grepping actual usage in the fork rather than copying upstream's dependency list.

### Runtime dependencies: 11 pytest plugins (upstream) → ~5-6 (v1)

Upstream `devel-common`'s `"pytest"` extra pulls `pytest-asyncio`, `pytest-cov`,
`pytest-custom-exit-code`, `pytest-icdiff`, `pytest-instafail`, `pytest-mock`,
`pytest-rerunfailures`, `pytest-timeouts`, `pytest-unordered`, `pytest-xdist`, `pytest` — 11
packages. Measured real usage in the fork's own ~2 years of tests:

| Dependency | Real usage | Verdict |
|---|---|---|
| `pytest` | — | required (core) |
| `pytest-xdist` | 4 `xdist_group` uses + all parallel runs | required |
| `pytest-timeout` | **234** `@pytest.mark.timeout` uses | required — by far the most-used plugin |
| `filelock` | 2 files | keep, demoted to defense-in-depth (§11) |
| `pytest-mock` | **2** `mocker` uses vs. **169** stdlib `unittest.mock` uses | **drop** |
| `requests_mock` | 1 file | drop from core — user's own concern |
| `sqlalchemy[asyncio]` | **0** async usages anywhere | **drop** — and `sqlalchemy` itself is already a transitive dependency of `apache-airflow-core` (confirmed via `uv.lock`), so declaring it again is redundant |
| `coverage`/`pytest-cov` | **0** refs | drop from core → optional `[cov]` extra |
| `pytest-asyncio` | **0** files | never add |
| `time-machine` | **0** files | drop → optional extra if ever requested |
| `psutil` | used only for port probing | drop the *declaration* — already transitive via `apache-airflow-core`/`apache-airflow-task-sdk` (confirmed via `uv.lock`); or replace the one use site with stdlib `socket` and remove the need entirely |

**Net v1 runtime dependencies: `pytest`, `pytest-xdist`, `pytest-timeout`, `apache-airflow-core`,
`apache-airflow-providers-sqlite`** — `filelock` may drop out entirely once §11's controller-side
bootstrap replaces its remaining use. Five to six packages, versus eleven pytest plugins alone
upstream.

### Zero required `[tool.pytest.ini_options]` — verified, not asserted

Built and ran a plugin-only config with **no `pytest.ini` and no `[tool.pytest.ini_options]`
anywhere** that reproduced every piece of the fork's current ini boilerplate — `--tb=short`,
`-rasl`, verbosity, `--durations`/`--durations-min`, all three `filterwarnings` entries,
`tmp_path_retention_count`/`_policy`, and marker registration — entirely from `pytest_configure`,
confirmed passing under `--strict-markers`.

**Precedence was verified correct**, not just "it runs": invoking with explicit `--tb=long
--durations=3` on the command line left both untouched (`tb=long`, `durations=3` survived), while
the no-flags case applied the plugin's defaults (`tb=short`, `durations=15`). The mechanism is an
"only set if unset" guard on `config.option`, applied in `pytest_configure` — the plugin provides
defaults, never overrides. `testpaths` is deliberately excluded from this list — it's project
layout, not plugin policy, and stays entirely the user's call.

### `filterwarnings` — narrowed to third-party noise, not a blanket `DeprecationWarning` kill

The fork currently does `simplefilter("ignore", DeprecationWarning)` at conftest import time — an
unconditional, global kill of every `DeprecationWarning` in the process, which also hides genuine
Airflow deprecations that warn a Dag author their code will break in the next minor. Traced the
fork's two explicitly-named suppressions to their actual source, rather than guessing:

- `HTTP_422_UNPROCESSABLE_ENTITY` deprecation → **Starlette** renamed the constant; Airflow has its
  own compat shim at `airflow-core/src/airflow/api_fastapi/compat.py:34`. Fires only when the API
  server is involved.
- `"appbuilder.app is deprecated"` → **flask_appbuilder** (FAB provider,
  `providers/fab/src/airflow/providers/fab/www/extensions/init_appbuilder.py:242`). Fires only if
  the FAB auth manager loads.

Both are third-party/provider noise the user cannot act on — not Airflow-core deprecations. Model
taken directly from upstream Airflow's own `pyproject.toml`, which does both narrow-ignore *and*
promote-to-error:
```toml
"error::pytest.PytestCollectionWarning",
"ignore::DeprecationWarning:flask_appbuilder.filemanager",
"ignore::sqlalchemy.exc.MovedIn20Warning:flask_appbuilder",
```
**v1 default filters** (all added via `config.addinivalue_line`, so user-supplied filters still take
precedence): module-scoped ignores for `flask_appbuilder`/`flask_sqlalchemy` and the two traced
warnings above, plus `error::pytest.PytestCollectionWarning` /
`error::pytest.PytestUnraisableExceptionWarning` to promote signals of a real problem. **Airflow's
own `DeprecationWarning`s stay visible by default** — the deliberate behavior change, turning the
plugin into an early-warning system for version breakage rather than a mute button.

### Tool pins (current as of 2026-08-06, re-verify before release)

| Tool | Pin | Note |
|---|---|---|
| `ty` | `==0.0.69` | released 2026-08-06; pre-1.0, exact-pin and bump deliberately |
| `ruff` | `==0.16.1` | 2026-07-30 |
| `pytest` | `>=9.1` | 9.1.1 current |
| `pytest-xdist` | `>=3.8` | 3.8.0 |
| `pytest-timeout` | `>=2.4` | 2.4.0 |

---

## 14. Build/packaging tooling — `uv_build`, no `hatch`

Considered `hatch` for scalable matrix testing across Python/Airflow versions/extras, given the
repo's "latest Astral tooling" direction. Concluded it's the wrong tool here, for a specific reason
rather than by default preference:

**`hatch` (the CLI/env orchestrator) depends on `uv>=0.5.23` internally** (confirmed via PyPI
metadata) — layering it on top of a repo that already drives everything through `uv` means running
`uv` twice, once directly and once wrapped inside `hatch`'s own env management. That's redundant
plumbing for a project whose stated goal is *lean*.

**Precedent check across real, well-maintained plugins:** `pytest-asyncio`, `pytest-mock`, and
`pytest-xdist` (all `pytest-dev`) use `setuptools` + a root `tox.ini` for their matrix. `syrupy`
uses `hatchling` as build backend with **no local orchestrator at all** — its CI job matrix calls
its test command directly. `syrupy`'s pattern is the one to follow, just with Astral's own backend
instead of `hatchling`: **`uv_build`** (`requires = ["uv_build>=0.9"]`, `build-backend = "uv_build"`)
— verified working end-to-end: built a real wheel from a `src/` layout with a `pytest11` entry
point, confirmed the entry point lands correctly in the wheel's `entry_points.txt`. One fewer
dependency than `hatchling` (`uv_build` needs nothing but `uv`), one fewer release cadence to track.

**The matrix itself needs no orchestrator.** Verified `uv run --isolated --python 3.12 --with
apache-airflow-core==3.1.8 pytest` and the equivalent for `3.2.2` resolve cleanly side by side with
no shared state — `uv run`'s own `--python`/`--with`/`--isolated` flags *are* the matrix mechanism.
No `tox.ini`, no `noxfile.py`, no `hatch.toml` matrix table: the CI workflow's own
`{python-version} × {airflow-version}` job matrix calls `uv run` directly per cell.

This also means v1 does **not** need the fork's `[tool.uv] conflicts` table or its ~100-package,
per-Airflow-version pinned extras (one exactly-pinned group per supported Airflow version) — those
exist because the predecessor repo *deploys* pinned providers, which a plugin never does. Testing
against several Airflow versions is fully served by `--with apache-airflow-core==X` per CI cell,
with no extras or conflict-resolution machinery in `pyproject.toml` at all.

**Decision: `build-backend = "uv_build"`, no `hatch`, no `tox`, no `nox`.**

---

## 16. Self-testing with `pytester`, and CI/tox scaffolding

### `pytester` — official mechanism for testing the plugin itself

Enable via a single top-level declaration: `pytest_plugins = ["pytester"]` in the repo's top-level
`tests/conftest.py` (confirmed: this must be the topmost conftest, not per-file — both pytest's own
suite and pytest-xdist's `testing/conftest.py` declare it exactly once). `testdir` is the same
fixture's legacy predecessor (`py.path.local`-based); official docs say new code should avoid it —
use `pytester` only.

**Two run modes, and the distinction is load-bearing for this plugin specifically:**

- `runpytest_inprocess` — same interpreter, same process. `Pytester._finalize` only guarantees
  `sys.modules`/`sys.path` restoration (via `SysModulesSnapshot`/`SysPathsSnapshot`) plus whatever
  `monkeypatch` undoes; its own docstring says it "tries to clean up global state," not that it
  does exhaustively.
- `runpytest_subprocess` — spawns a real `sys.executable -m pytest` subprocess with its own
  `--basetemp`; fully isolates interpreter state.

**Confirmed via pytest's own test suite**, not inferred: `testing/test_capture.py`'s
`TestLoggingInteraction` class uses `runpytest_subprocess` for every logging-state test
(`test_logging_stream_ownership`, `test_conftestlogging_and_test_logging`, etc.) — i.e. pytest's own
maintainers reach for the subprocess variant specifically for the class of problem our §10 `dictConfig`
monkeypatch is. **Our own reattach-regression test must use `runpytest_subprocess`, not
`runpytest_inprocess`** — an in-process run would not reliably re-exercise (or would leak) the
`logging.config.dictConfig` monkeypatch across repeated `runpytest()` calls in the same test session.

**Directly adaptable template for the xdist bootstrap tests.** `pytest-xdist`'s own suite
(`testing/acceptance_test.py::test_data_exchange`) is close to a literal match for our
`pytest_configure_node`/`workerinput` design (§11): a `pytester.makeconftest(...)` block sets
`node.workerinput['a'] = 42` in `pytest_configure_node` (controller-only), reads it back in
`pytest_configure` via `hasattr(config, 'workerinput')` (worker-only branch), and asserts via
`pytester.runpytest("-v", p1, "-d", "--tx=popen")` + `result.stdout.fnmatch_lines([...])`. Use
`result.assert_outcomes(passed=N, ...)` for outcome counts. For xdist coverage generally, plain
`runpytest("-nN", ...)` suffices for collection/outcome-level assertions; drop to `--tx=popen` (real
subprocess workers) when asserting genuine cross-process behavior — confirmed this is exactly the
threshold xdist's own suite uses (its worker-hang/crash tests and `workerinput` exchange test both
use `--tx=popen` rather than bare `-nN`).

### Inheriting `tool.tox`/Makefile/`.github/workflows` from the fork — confirmed non-portable as-is

Read the fork's actual CI directly rather than assuming "inherit" meant "copy":

- `.github/workflows/main.yml` and `compat.yml` both run on a private self-hosted runner with a
  hardcoded enterprise certificate path, private cache path, and GitHub Enterprise PAT auth.
  None of it runs against public
  `github.com` runners as written — nothing here is copy-paste-portable to a public repo.
- The Makefile (~200 lines) is overwhelmingly private Dag-repo deployment tooling
  (internal Dag-repo deployment targets, `dag_processor`, `standalone`) — irrelevant to a testing plugin.
- **The `[tool.tox]` block itself is clean and genuinely portable**: `format`/`lint`/`type` envs plus
  `tool.tox.labels` grouping (`smoke = [format, lint, type, uv_lock, pytest-smoke]`,
  `compat = [..., pytest-compat]`), all using the `uv-venv-lock-runner` tox-uv runner. This *shape*
  transfers directly.
- **Found sitting directly above `[tool.tox.env.pytest-compat]`**: `# TODO(redd): Add back
  multi-version compat testing`. The exact "run `@pytest.mark.compat` across multiple Airflow
  versions" story requested for this plugin is a **self-admitted, currently-unimplemented gap in
  the fork itself**, not a working feature to port over.

**Decision:** adopt the tox env *shape* (labels, `format`/`lint`/`type`/`smoke`/`compat`,
`uv-venv-lock-runner`); rewrite the GitHub Actions workflows from scratch against public runners;
implement the multi-Airflow-version compat matrix using `uv run --python X --with
apache-airflow-core==Y --isolated` per cell (§14) rather than the fork's pinned-extras-and-conflicts
machinery, since that machinery exists to support *deploying* pinned providers — a concern this
plugin never has.

### `tox-docker` and Dockerfile-per-OS/ISA — rejected, not merely deprioritized

Two independent, decisive findings, not a maintenance nitpick:

1. **Maintenance**: `tox-docker` last released 2024-05-25 (~26 months stale as of this writing),
   last commit 2024-06-04, essentially single-maintainer (209 vs. 12 commits from the next
   contributor), 21 open issues.
2. **Wrong category of tool, independent of maintenance status.** `tox-docker`'s own documented
   model is a *sidecar-service launcher* (`docker = db` in `[testenv]`, spins up e.g.
   `postgres:9-alpine` alongside a tox environment that **still runs on the host**) — functionally
   `testcontainers` for tox. It has no mechanism to run the test suite's own interpreter inside a
   container targeting a different OS/architecture; it was never designed to.

**The deeper problem is structural, not a tooling gap: Docker containers share the host kernel, so
"a macOS Dockerfile" is not a coherent concept, and "a Windows container on a Linux runner" is
impossible with or without QEMU** — QEMU emulates CPU instructions, not kernel syscalls/ABI. Docker
Desktop on macOS itself works by running Linux containers inside a hidden Linux VM; there is no path
to a Darwin-kernel container from any host. Confirmed by pulling the live CI workflows of three real
packages with genuine per-OS/arch risk (`psutil`, `cryptography`, `ruff`): **all three cover macOS
and Windows exclusively via native GH Actions runners, zero Docker involved for those legs.** Docker
appears in their workflows for exactly one purpose — getting old-glibc/musl Linux userland variance
(manylinux/Alpine) on a *native Linux runner*, i.e. same kernel, sometimes same arch, different
libc — never as a stand-in for a different OS.

This maps directly onto our plugin's actual risk surface, which the user named precisely: SQLite
locking / tmpfs-NFS filesystem semantics, `fork` vs. `spawn` (Windows has no `fork()` at all), and
glibc vs. musl. **None of these are CPU-instruction-set concerns** — they're kernel- and
libc-level, which is exactly what QEMU-based emulation does not touch.

**Decision: no `tox-docker`, no Dockerfile-per-platform matrix.** Run the full Airflow compatibility
matrix on native Linux, with targeted Linux ARM and macOS jobs. Windows runs only
platform-independent modules because Airflow itself imports POSIX-only facilities; it is not a
supported full-stack target. Add
exactly **one** `container: python:3.x-alpine` job on a Linux runner (running pytest directly, no
`tox-docker`) for the musl leg — the one place Docker is genuinely the right tool, matching
`cryptography`'s own Alpine-job pattern.

### `pytest-cov` — CI-internal, not a runtime dependency

Reconciled against the §13 dependency audit (0 real `pytest-cov` usages in the fork → drop from
runtime deps). These aren't in tension: coverage measurement for **this plugin's own test suite**,
run in **our own CI**, is unrelated to what ships in `[project.dependencies]`. Decision:
`pytest-cov` lives in the repo's own `dev`/`test` dependency group and is wired into the tox
`smoke`/`compat` envs (coverage uploaded as a CI artifact/badge for this repo's own tests) but never
appears in the installable package's runtime dependencies — installing `pytest-airflow-in-a-box`
must never pull coverage tooling into an end user's own Dag-testing environment.

---

## 18. Local CI reproduction — `act`, confirmed working end-to-end

Question: since `tox`/`tox-docker` are both dropped (§16) in favor of GitHub Actions running
natively, can the workflows be run and iterated on locally without push-to-test? Verified yes, by
actually installing and running the tool rather than citing its docs.

**Tool: [`nektos/act`](https://github.com/nektos/act)**, v0.2.89 (current as of 2026-08-06). Reads
the repo's real `.github/workflows/*.yml` and executes each job inside a Docker container matching
its `runs-on`, using the same YAML that gets committed — no separate local-CI config to keep in
sync.

**Reproduced a real run, end to end**, with a workflow shaped like our actual CI (`uv`-based steps,
a Python-version matrix, checkout, a real command):

```
[CI/test] 🧪  Matrix: map[python-version:3.13]
[CI/test] ⭐ Run Main actions/checkout@v4  ...  ✅  Success
[CI/test] ⭐ Run Main Install curl + uv
[CI/test]   | downloading uv 0.12.2 aarch64-unknown-linux-gnu
[CI/test]   | everything's installed!  ...  ✅  Success
[CI/test] ⭐ Run Main Show env + run real command
[CI/test]   | uv 0.12.2 (aarch64-unknown-linux-gnu)
[CI/test]   | matrix.python-version=3.13
[CI/test]   | pytest 9.1.1 on py 3.13.14 (main, Aug  5 2026, 15:42:48) [Clang 22.1.3 ]  ...  ✅  Success
[CI/test] 🏁  Job succeeded
```

Matrix substitution, step sequencing, `GITHUB_PATH` mutation (`echo "$HOME/.local/bin" >>
"$GITHUB_PATH"`), and `uv` genuinely downloading a Python 3.13 interpreter and running `pytest`
inside the container all worked correctly, with no special local-CI configuration beyond the
workflow file itself.

**One real, documented limitation surfaced directly, not anticipated in advance**: `act`'s default
runner images bundle Node.js because most third-party marketplace actions (`astral-sh/setup-uv@v5`
included) are Node-based under the hood; a bare `ubuntu:22.04` image failed on
`astral-sh/setup-uv@v5` with `exec: "node": executable file not found` — a known, tracked `act`
issue ([nektos/act#107](https://github.com/nektos/act/issues/107)). The practical, verified fix:
**prefer shell-based installation steps over Node-based marketplace actions** where there's a
choice — `curl -LsSf https://astral.sh/uv/install.sh | sh` instead of `astral-sh/setup-uv@v5`
reproduced correctly on the minimal image. This is a net win independent of `act`: fewer
third-party actions to pin/audit/trust, consistent with the "lean" goal. `actions/checkout@v4`
itself works out of the box under `act` (it special-cases this action).

**What `act` cannot reproduce, and isn't expected to**: `runs-on: macos-*`/`windows-*` legs — `act`
only ever runs Docker containers, so it's Linux-only by the same kernel-sharing constraint from
§16. This is an acceptable split: `act` covers fast local iteration on the Linux legs (where most
day-to-day development happens), while the macOS/Windows legs of the native matrix are verified by
an actual push. Local network egress to Docker Hub is also required (irrelevant to a real dev
machine; this research session's own sandbox had it blocked, which is why the verification run used
a locally-cached `ubuntu:22.04` rather than one of `act`'s bundled runner images).

**Decision:** document `act` as the supported local-CI-reproduction tool in the README/CONTRIBUTING,
and write the workflow's install steps shell-first specifically so they work under `act` without
extra flags or a parallel local-only config.

---

## 20. Multi-Airflow-version compat testing — the fork's generator script is not needed for v1

Question: does the fork's generator script (a 660-line helper that generates pinned
`airflow_x_y_z` extras in `[project.optional-dependencies]` from Airflow's published constraints
files) belong in this plugin's CI matrix strategy?

**Verified empirically that the underlying mechanism `uv run --with apache-airflow-core==X`
(established in §14) resolves cleanly with no constraints file at all** — `apache-airflow-core==3.1.8`
installed and imported successfully in ~10s via `uv run --isolated --with`, no extra generated by
that script required.

**But also verified a real gap that matters**: with no constraints file, `uv`'s resolver picked
newer transitive dependencies than Airflow's own CI validated against for that release —
measured directly against the constraints file at
`https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt`:

| Package | Unconstrained (`uv run --with`) | Airflow's own constraints-3.1.8 |
|---|---|---|
| `alembic` | 1.19.0 | **1.18.4** |
| `structlog` | 26.1.0 | **25.5.0** |
| `pydantic` | 2.13.4 | **2.12.5** |
| `sqlalchemy` | 2.0.51 | 2.0.48 |
| `fastapi` | 0.117.1 | 0.117.1 (matched) |

So the unconstrained resolution "works" today but is not what Airflow's own maintainers tested
against for that release — a real risk for a compat suite whose entire purpose is catching version
drift before users do.

**The fix is Airflow's own published constraints files directly, not the fork's generator script's
`pyproject.toml`-extras-generation machinery.** Verified: `uv run --with` does **not** honor
`UV_CONSTRAINT`/`--constraints` for ad-hoc ephemeral environments (confirmed with a fresh
`UV_CACHE_DIR` to rule out caching — the env var is silently ignored for this invocation shape).
The correct, verified mechanism is `uv pip install --constraint <constraints-file>` into an
explicit `uv venv`:
```bash
uv venv --python 3.12 .venv-3.1.8
uv pip install --python .venv-3.1.8 \
  --constraint constraints-3.1.8-py312.txt \
  "apache-airflow-core==3.1.8"
```
Verified this reproduces Airflow's own pins exactly (`alembic==1.18.4`, `structlog==25.5.0`,
`pydantic==2.12.5` — all matched the constraints file after switching to `--constraint`, none
matched under unconstrained `--with`).

**Why the fork's generator script's actual purpose doesn't transfer to this repo.** Reading it
(a several-hundred-line script in the fork): its job is generating a **committed, versioned dependency
group** (one per supported Airflow version) containing ~100 exactly-pinned *provider* packages
(`apache-airflow-providers-amazon==9.22.0`, etc.) for a repo that **deploys** those providers to a
real Airflow instance and needs `uv.lock` to reproduce that exact deployed state. This plugin
never deploys providers — its CI only needs *ephemeral, per-matrix-cell* resolution against a
specific Airflow core version for the duration of one test run, which a constraints file handed
straight to `uv pip install --constraint` (or `uv venv` + `uv pip sync` from a
constraints-derived requirements file) already does, with no generation step and nothing to keep
in sync in a committed `pyproject.toml`.

**Decision: don't adopt the fork's generator script.** CI matrix cells fetch the matching
`constraints-{airflow_version}/constraints-{python_version}.txt` from
`apache/airflow` at job runtime (one `curl`/`uv pip install --constraint` step, no committed
per-version extras), matching how `uv run --with apache-airflow-core==X` was already planned to
drive the matrix (§14) — just adding the constraints file as the missing piece for transitive-pin
fidelity. If a *committed, offline-reproducible* per-version lock ever becomes necessary (e.g. for
release-time compat verification rather than routine CI), revisit that script's core
constraints-parsing logic (`PIN_RE`, `dependency_lines_from_pins`) as a reusable piece — but that's
a "maybe later," not a v1 requirement, and even then it would be repurposed for
`[dependency-groups]` test-only pins, not the provider-deployment `[project.optional-dependencies]`
shape it currently produces.

---

## 21. Proposed next step

Sections 10–20 turned the original §9 "proposed next step" into settled decisions: the `test_utils`
disposition, the xdist/`pytest_configure_node` bootstrapping architecture, the Dag-file collection
design, the trimmed dependency/config-boilerplate footprint, the build-tooling choice, the
`pytester`-based self-testing strategy, the CI/tox scaffolding decision (native OS matrix, no
`tox-docker`/`tox`, no cross-platform Dockerfiles), local CI reproduction via `act`, and the
multi-Airflow-version compat testing mechanism (constraints files, not the fork's generator
script).

The full, current architecture is now maintained in the live plan file
(`~/.claude/plans/purring-crafting-rain.md`), which supersedes the remaining open-thread list
previously kept here.

---

## 22. Representative `compat` regression tests — what the end-user-facing suite should contain

**Added 2026-08-06.** This section answers a specific gap: v1's `compat` marker (which the CI matrix
runs per `{OS} × {python} × {airflow-version}` cell, doc §16/§20) needs a *small, curated* set of
tests that exercise the shipped plugin **as an end user would** — build a Dag, run a task, import a
Dag bag, assert on the result — so that a green compat matrix genuinely proves the plugin's public
surface works in each runtime environment. It is NOT the fork's current `compat` bucket.

Findings below come from scanning the fork's `tests/unit/` (two internal workspaces, same repo). Three
takeaways up front:

1. **The fork's `compat` marker is a 48-file catch-all, not a curated end-user set.** It means "runs
   on the compat matrix," applied mostly as module-level `pytestmark = pytest.mark.compat` to
   script/utility/business-logic tests that mock everything and never touch the plugin fixtures.
   Only ~6 files combine `compat` with `dag_maker`/`run_task_instance`. Do **not** port the fork's
   `compat` selection wholesale — re-curate from scratch.
2. **The `smoke` set (~9 tests) is closer to "minimal representative," but is almost entirely
   DagBag/config-validation bound to the private `dags/` tree** — not fixture demonstrations. Model
   the *shape* of `test_integrity`, not its contents.
3. **No existing test is a clean, dependency-free "end user drives the plugin" test.** The best
   structural templates all import private infrastructure and environment-specific tools. The
   representative compat tests must be **newly
   authored** against benign, in-test Dags/operators — the fork gives us the exact *idioms* to copy,
   not shippable bodies.

### The four canonical idioms (verified in the fork, to be distilled)

Every representative test reduces to one of these. Paths are the cleanest fork exemplar of each.

**A. DagBag import + structure assertion** — `tests/unit/dags/<domain>/test_<dag>.py`
(39 lines, `@pytest.mark.compat` only, no DB, only a path fixture). The exact vocabulary to reuse:
```python
dag_bag = DagBag(dag_folder=<dir>, include_examples=False)
assert dag_bag.import_errors == {}
dag = dag_bag.dags["<dag_id>"]
leaf_ids = {t.task_id for t in dag.tasks if not t.downstream_task_ids}
assert dag.get_task("final_status").trigger_rule == TriggerRule.ALL_SUCCESS
```
The `full_dag_bag` fixture (in the fork's fixtures module) is the whole-tree version
(`DagBag(dag_folder=dags_path, include_examples=False)`, session-scoped). For the public plugin the
generalized `full_dag_bag` should point at the user's Dag dir; the *minimal regression test* instead
builds a fresh `DagBag` over a small bundled example dir.

**B. Custom operator subclass → run → assert output** — `create_task_instance_of_operator` +
`session`, from the fork's operator test suite:
```python
ti = create_task_instance_of_operator(MyOperator, <kwargs>, dag_id=..., task_id=..., session=session)
context = ti.get_template_context(session=session)
ti.render_templates(context=context)
result = ti.task.execute(context).output
```
Purest no-DB variant: the fork's operator-init test — instantiate the subclass,
assert its default attrs, zero fixtures.

**C. Custom TaskFlow `@task` decorator → run → assert XCom** —
the fork's TaskFlow decorator test suite (init, and XCom via `run_task_instance`):
```python
with dag_maker(dag_id=...) as dag:

    @task
    def my_task(x):
        return x + 1

    my_task(41)
dr = dag_maker.create_dagrun()
for ti in dr.get_task_instances(session=session):
    run_task_instance(ti, session=session)
    assert ti.state == TaskInstanceState.SUCCESS
assert dr.get_task_instance("my_task").xcom_pull() == 42
```

**D. Inline Dag + run-all-TIs + assert DagRun success** — the run-loop idiom repeated across the
whole suite; cleanest compat-marked exemplar is the fork's task-archival test
(`@pytest.mark.compat` + `db_test` + `timeout(200)`), whose `archive` task is nearly generic
(`shutil`-based). The canonical loop:
```python
dr = dag_maker.create_dagrun()
for ti in dr.get_task_instances(session=session):
    run_task_instance(ti, session=session)
    assert ti.state == TaskInstanceState.SUCCESS
dr.update_state(session=session)
assert dr.state == DagRunState.SUCCESS
```
Use `ordered_task_instances(dr, dag, session=session)` (the fork's task-instance helper module) instead of
`get_task_instances()` when the Dag has real inter-task dependencies (Airflow 3.2's `_run_task`
path doesn't dependency-sort for you).

### Recommended v1 compat suite (all newly authored, zero private deps)

Each is ~20-40 lines, lives under the plugin's own `tests/`, and is what the CI matrix runs per
Airflow version. This is the "end user simulating" set:

| # | Test | Idiom | Fixtures | Proves |
|---|---|---|---|---|
| 1 | `test_dagbag_imports_example_dag` | A | `full_dag_bag` (or fresh `DagBag`) over a bundled `tests/dags/` | Dag files parse with no import errors in this runtime |
| 2 | `test_dagbag_structure` | A | same | Topology assertions (`dag_id`, task count, leaf tasks, `trigger_rule`, up/downstream sets) survive serialization round-trip |
| 3 | `test_custom_operator_executes` | B | `create_task_instance_of_operator`, `session` | An in-test `class MyOp(BashOperator)` (or `PythonOperator`/`BaseOperator`) subclass renders templates and `execute()`s, `.output` correct |
| 4 | `test_custom_operator_init_defaults` | B | none | Subclass construction + default-attr assertions (no-DB, fastest, catches API drift) |
| 5 | `test_taskflow_task_runs_and_xcoms` | C | `dag_maker`, `session` | A benign `@task def f(x): return x+1` runs to SUCCESS and its XCom is pullable |
| 6 | `test_inline_dag_runs_to_success` | D | `dag_maker`, `session` | Multi-task inline Dag (e.g. `@task` → `EmptyOperator`) runs all TIs, DagRun state = SUCCESS |
| 7 | `test_skipped_task_state` | D | `dag_maker`, `session` | `AirflowSkipException` → `TaskInstanceState.SKIPPED` (models `test_archive_skips_when_patterns_none`) |
| 8 | `test_structlog_caplog_capture` | — | `caplog` (structlog override) | Airflow-3 structlog records are visible to the overridden `caplog` (model: `the fork's disk-logging test`, but a benign logger) |

Notes:
- **Cover both operator base classes and the TaskFlow API** (tests 3+5) — they're distinct plugin
  code paths for building a serialized Dag, and Airflow churns them independently.
- **Test 4 is the cheapest early-warning tripwire** for Airflow API changes across the version matrix
  (no DB, pure construction) — keep it even though it looks trivial.
- **The example Dag dir (tests 1-2) is bundled fixture data**, modeled on the fork's tiniest DAG
  (one `@dag` + one `@task`). Ship 2-3 tiny
  Dags: a single-task happy path, a multi-task-with-dependency, and a deliberately-broken one to
  assert `import_errors` is populated (negative test). This dir doubles as the §12 custom-collection
  fixture corpus.
- These 8 also exercise the plugin's own §12 double-collection guard implicitly (Dags named to look
  like `test_*.py` would collide) — but that collision needs a *dedicated* `pytester` self-test,
  not a compat test (it's plugin-internal, not end-user-facing).

### Provenance boundary before any implementation is reused

No private-fork-only code, hostnames, paths, credentials, test data, or repository history are copied.
Public Airflow imports replace private facades, and examples are newly authored. The
`run_task_instance()`/`ordered_task_instances()` shim is vendored from the public Apache Airflow
source with its license header, modification notice, source path, and exact commit recorded.

## 23. Implementation findings (2026-08-07, build-order steps 6–9)

Empirical results from implementing reporting, collection, `clear_db`, `cap_structlog`,
`run_task`, and the zero-ini defaults. Everything below was verified against installed
Airflow 3.3.0 plus `uv run --isolated --with apache-airflow-core=={3.1.8,3.2.2}` probes.

### Task SDK in-process execution (`run_task`)

- The runner surface is **uniform across 3.1.8/3.2.2/3.3.0**: `task_runner.run(ti, context, log)`
  returns `(TaskInstanceState, ToSupervisor | None, BaseException | None)`; `CommsDecoder.send`
  exists on all three (no `send_request`/`get_message` legacy shape anywhere in the certified set);
  `TIRunContext` requires exactly `dag_run` + `max_tries`. No new capability entries were needed.
- `RuntimeTaskInstance.bundle_instance` is **required on all three releases** (not new churn), but
  is only consumed by the `parse()`/bundle path — `run()` and `get_template_context()` never touch
  it, so `model_construct` simply omits it for DB-free execution.
- `SUPERVISOR_COMMS` is an *unset module annotation* until the supervisor assigns it (all three
  releases) — install/restore must handle the attribute-absent case with a sentinel + `delattr`.
- `ti.xcom_pull` sends **`GetXComSequenceSlice`** (present since 3.1.8), not `GetXCom`; a fake
  supervisor must answer it with `XComSequenceSliceResult(root=[...])`.
- **Raising from `send` for unseeded state does not work**: the SDK's secrets-backend resolution
  loop (`BaseHook.get_connection` et al.) swallows backend exceptions and raises its own
  `AirflowNotFoundException`, so a custom exception never reaches the user. The protocol-faithful
  answer — `ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND, detail={"hint": ...})` — makes the
  task fail exactly like a live deployment, with the seeding hint preserved in `detail`.
- `params` delivered as the synthetic DagRun's `conf` flow through `process_params` and are
  validated against declared Dag params exactly like a triggered run — no separate params plumbing.
- The 3.1.8 `DagRun` DTO additionally requires `start_date` (3.2+ relaxed it); always passing it
  is compatible with all three.
- `uuid6` is importable on all three certified releases (task-sdk dependency) — safe to use for
  `id`/`dag_version_id` without declaring it.

### `clear_db` registry

- Every registry spec resolves identically on 3.1.8 and 3.3.0, including
  `airflow.models.xcom.XComModel` (already present in 3.1 — the "3.1 may differ" risk was empty)
  and the three asset association `Table` objects (`association_table` == `dagrun_asset_event`,
  `alias_association_table`, `asset_alias_asset_event_association_table`).
- `dagrun_asset_event` must be a member of **both** the `runs` and `assets` groups: its rows must
  go when either side goes, and SQLite autoincrement id reuse would otherwise re-associate a new
  DagRun with a stale asset event. Repeated deletes are idempotent.
- The tuned SQLite profile does not enable `PRAGMA foreign_keys`, so implied-group expansion in
  code (`runs → task_instances → xcom`, `dags → serialized_dags → runs`, `bundles → dags`) is what
  prevents orphans — `serialized_dags` implies `runs` because `TaskInstance.dag_version_id` and
  `DagRun.created_dag_version_id` reference `dag_version`.
- Out of v1 scope and documented as such: triggers, deadlines, and the newer partition tables
  (`asset_partition_dag_run`, `partitioned_asset_key_log` — 3.3-only).

### pytest 9 specifics (defaults, reporting, self-tests)

- pytest core already guards junitxml on xdist workers; `_pytest.logging` has **no** such guard —
  per-worker `--log-file` suffixing is the plugin's job. pytest-cov manages its own parallel data
  files; only externally orchestrated `COVERAGE_FILE` needs worker suffixing.
- `filterwarnings` ini lines: later lines win (each application prepends to the warnings module's
  filter list). Plugin defaults must be **prepended** for user ini lines to keep precedence.
- `-r` report characters on 9.1: default `fE`; `l` is not a valid character (`getreportopt`
  appends it inertly). `a` expands to `sxXEf`.
- `config.inicfg` is deprecated (`PytestRemovedIn10Warning`). Overriding another plugin's ini
  *default* is done by re-registering the key with `parser.addini` from a `trylast`
  `pytest_addoption` — the last registration wins and user ini values still override defaults.
- `Pytester.parseconfig`/`runpytest_inprocess` are unusable for anything that exercises this
  plugin from inside its own suite: the outer session has imported Airflow, and
  `load_initial_state` refuses to bootstrap after that. Subprocess pytester everywhere; its
  children are invisible to coverage.
- A serial pytester child inherits the outer worker's `PYTEST_XDIST_WORKER` under `-n auto`;
  self-tests asserting worker identity must scrub it.

### macOS storage detection (plan gap, fixed)

`/proc/mounts` + Linux statfs magics cover only Linux; the plan treated all of macOS as
"conservatively network", which sent every macOS host through the loud writable-fallback path and
rejected explicit `--airflow-home` bases. Darwin `statfs(2)` provides `f_fstypename` (feeds the
same name classifier as the mount table) and the kernel `MNT_LOCAL` flag (authoritative fallback
for unknown names). The `statfs$INODE64` symbol must be preferred when present — Intel macOS keeps
the legacy pre-10.6 layout under the plain symbol; Apple silicon only ships the 64-bit-inode
layout. `afpfs`/`webdav` joined the network type set. Unprobeable paths stay conservatively
network.

### Coverage gate reality check

`fail_under = 100` with branch coverage is currently unenforceable: the Windows `windll` branch is
unreachable on every CI platform, the Darwin probe is unreachable on Linux CI (and vice versa),
subprocess pytester children are unmeasured, and the CI matrix runs plain `pytest` with no
coverage step at all. Resolving this needs platform-conditional exclusions plus coverage wiring in
CI, or a scoped gate — deferred, tracked in the plan's Corrections section.
