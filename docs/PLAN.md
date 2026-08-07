# `pytest-airflow-in-a-box` — v1 Implementation Plan

_Canonical implementation plan. Imported from source commit `abd7747` and revised on 2026-08-07
to remove environment-specific details and incorporate bootstrap validation findings._

## Status (2026-08-07, second revision)

Every numbered build-order step (1–13) is implemented and tested; see § Corrections below for
where the implementation deviated from this plan and why.

- **Done:** scaffold; `storage/locate.py` (+ Darwin `statfs` probe, a plan gap); `storage/sqlite.py`;
  `bootstrap.py`; `_compat/` capabilities; logging's 3 layers + `StructlogCapture`; `reporting.py`;
  `collection.py` (+ `--collect-dag-folder`/`airflow_collect_dags_folder`, double-collection guard);
  `db.py`/`_compat/db.py` (`TableGroup`, `clear_db`, implied-group expansion);
  fixtures `session`, `dag_maker` (with `create_dagrun`/`create_ti`/`run_ti`), `full_dag_bag`,
  `cap_structlog`, `run_task` (`_compat/in_process.py`), `api_server_url`/`api_client`
  (`fixtures/api.py`); markers incl. the `environment(name)` sentinel gate; `defaults.py`
  (zero-ini defaults + narrowed `filterwarnings`); bundled `tests/dags/` corpus + the 8-test
  end-user compat suite; README covering the full public surface; self-tests green serial and
  `-n auto`; CI green across Linux ×7, macOS, Linux ARM, and Alpine musl.
- **Remaining (post-v1 candidates):** pinned-`Param` Dag collection cases; `run_task`
  `finalize()`/callback dispatch; triggers/deadlines/partition tables in the `clear_db` registry;
  Windows platform-independent CI leg; coverage-in-CI wiring and gate restoration (gate lowered to
  the measured 90 % floor meanwhile); PyPI release mechanics.

## Corrections from implementation (2026-08-07)

Claims in this plan that implementation falsified, kept here so the sections below read against
the record:

- **`reporting.py` is smaller than planned.** pytest core already skips junitxml on xdist workers
  (`_pytest/junitxml.py` guards on `workerinput`); the real collisions are `--log-file` (no core
  guard — workers race on one file) and externally orchestrated `COVERAGE_FILE`. Both are rewritten
  per-worker; nothing else.
- **`caplog` is NOT overridden.** The builtin fixture's constructor is `_ispytest`-private, so a
  same-named override cannot delegate and would silently change stdlib capture semantics. Shipped
  `cap_structlog` (upstream's name) as a separate fixture instead.
- **`filterwarnings` precedence runs the other way.** pytest applies ini lines in order and each
  `warnings.filterwarnings` call prepends, so *later* lines win. The plan said "user-supplied
  filters still take precedence" of appended plugin lines — wrong. The plugin *prepends* its five
  filters so user ini lines, applied later, win. Verified with a downgrade test.
- **`-rasl` → `-ra`.** `l` is not a pytest 9 report character (`getreportopt` ignores it).
- **`config.inicfg` is deprecated (pytest 9, removed in 10).** The `tmp_path_retention_*` defaults
  are re-registered via `parser.addini` in a `trylast` `pytest_addoption` (last registration wins)
  instead of `inicfg.setdefault` in `pytest_load_initial_conftests`.
- **The Task SDK surface is uniform across 3.1.8/3.2.2/3.3.0** — `run(ti, context, log)` returning
  the 3-tuple, `CommsDecoder.send`, and `RuntimeTaskInstance.bundle_instance` *required on all
  three* (not 3.2/3.3 churn as assumed). `bundle_instance` is only touched by the unused `parse`
  path, so `model_construct` omits it. No new capability entries were needed for `run_task`.
- **`ti.xcom_pull` sends `GetXComSequenceSlice`,** not `GetXCom` — the fake answers both.
- **Unseeded Variable/Connection requests must answer with the protocol's `ErrorResponse`,** not a
  raised exception: the SDK's secrets-backend loop swallows exceptions from `send` and raises its
  own not-found error, so a raised `TaskRunError` never reached the user. The seeding hint travels
  in `ErrorResponse.detail` instead; `TaskRunError` was dropped.
- **The Darwin storage gap was real and load-bearing.** This plan's network-FS detection was
  Linux-only + "conservatively network" elsewhere, which made *every* macOS host take the loud
  writable-fallback path and rejected explicit `--airflow-home`. Fixed with a Darwin `statfs(2)`
  probe (`f_fstypename` + `MNT_LOCAL`, `statfs$INODE64` preferred for Intel).
- **All plugin-exercising `pytester` self-tests must use `runpytest_subprocess`:** the outer test
  session has already imported Airflow, and `load_initial_state` correctly refuses to bootstrap
  in-process after that. In-process pytester is unusable in this repo's own suite.
- **The 100 % branch-coverage gate is currently unenforceable as configured** — the Windows
  `windll` branch is unreachable on every CI platform, subprocess pytester children are unmeasured,
  and the CI matrix runs plain `pytest` without coverage. Resolved for now by lowering
  `fail_under` to the measured 90 % floor; restore with platform-conditional exclusions plus
  coverage-in-CI wiring later. TODO(redd).
- **The API server is per-worker and lazy, not controller-owned.** The plan folded a single
  controller-started server into bootstrap `pytest_sessionstart`. Implemented instead as a lazy
  session-scoped fixture: each process starts its own `airflow api-server --apps core` on a
  loopback ephemeral port against the shared database, only when an `api_test` actually requests
  it. No cross-worker coordination, no port election, zero cost on sessions without API tests.
- **Environment sentinels are an ini line list, not a pyproject table.** The planned
  `[tool.pytest-airflow-in-a-box.environments]` table needs `tomllib` (3.11+ stdlib) or `tomli`
  (not a dependency) on the 3.10 floor. The `airflow_environments` ini line list
  (`name = path`) rides pytest's own config machinery instead — same gate, zero parsing deps.

## Context

Airflow's own test harness (`devel-common`/`tests_common`) is explicitly, permanently internal —
Jarek Potiuk, Feb 2025: *"extremely unlikely we are going to release and publish the common test
code / fixtures in any way... It's far more efficient if people like you just copy them."* No public,
installable way to unit-test your own Dags exists. This plugin fills that gap.

It is a **generalization of a two-year-old production fork**,
not a greenfield design. That matters, because usage telemetry from that fork tells us exactly how
lean v1 can be, and its git history tells us which past designs (env-var bootstrapping, FileLock
convergence, a 9-layer logging stack) were actually failure-driven workarounds, not load-bearing
architecture.

Full research record: `~/code/docs/airflow-in-a-box-research.md` (21 sections, all decisions below
cite a section number there for the underlying evidence).

- **Package/PyPI:** `pytest-airflow-in-a-box` (verified available)
- **Repo:** `nredd/pytest-airflow-in-a-box` (verified available)
- **Airflow target:** 3.x only (`>=3.1,<4`), structured so 2.x *could* be added later
- **DB:** SQLite only, aggressively tuned. Postgres explicitly **out of scope for v1** (research
  retained in doc §9 for later)
- **Type checking:** complete `ty` coverage, no inline waivers, minimal `cast`
- **Build:** `uv_build`, no `hatch`/`tox`/`nox` (doc §14)
- **CI:** native GitHub Actions matrix, no Docker-based OS/ISA testing, reproducible locally via
  `act` (doc §16, §18)

---

## The evidence that drives the scope cut

Measured actual usage across the fork's ~2 years of production tests (doc §10):

| Fixture/util | Real usages | v1 verdict |
|---|---|---|
| `session` | 1064 | **ship** |
| `dag_maker` | 371 | **ship** |
| `caplog` (override) | 38 | **ship** |
| `full_dag_bag` (fork-local) | 14 | **ship** (generalized) |
| `run_task_instance` | 14 | **ship** |
| `run_task` | 12 | ship (Task SDK path) |
| `clear_db_*` | 2 files | ship (subset) |
| `create_dummy_dag`, `create_task_instance`, `create_runtime_ti`, `mock_supervisor_comms`, `cap_structlog`, `testing_dag_bundle`, `create_connection_without_db`, `create_dag_without_db`, `mock_xcom_backend`, `conf_vars` | **0 each** | **defer** |

Markers actually used: `db_test` (112), `compat` (122), `api_test` (22),
`real_environment` (17),
`smoke` (9), `xdist_group` (4), `need_serialized_dag` (3).

**Conclusion: the load-bearing surface is ~6 fixtures and ~5 markers.** Upstream's 5,767 lines of
`test_utils/` is mostly provider/system-test infrastructure we should not inherit.

Dependency audit (doc §13): upstream's 11-pytest-plugin `"pytest"` extra shrinks to
**`pytest`, `pytest-xdist`, `pytest-timeout`, `apache-airflow-core`,
`apache-airflow-providers-sqlite`** — `filelock` is no longer needed at all once the
`pytest_configure_node` bootstrap (below) replaces FileLock convergence. `sqlalchemy`/`psutil` are
transitive via `apache-airflow-core` already and are not redeclared.

---

## Decision: disposition of every upstream `test_utils` module

### DROP outright (~2,000 lines) — provider/system-test infrastructure

`gcp_system_helpers.py`, `azure_system_helpers.py`, `salesforce_system_helpers.py`,
`sftp_system_helpers.py`, `system_tests.py`, `system_tests_class.py`, `terraform.py`,
`integration_setup.py`, `logging_command_executor.py`, `otel_utils.py` (245 lines of OTel
span/metric extraction), `get_all_tests.py`, `providers.py`, `hdfs_utils.py`, `aiohttp.py`,
`common_sql.py`, `common_msg_queue.py`, `perf/perf_kit/`, `stream_capture_manager.py`,
`reset_warning_registry.py`, `file_task_handler.py`, `log_handlers.py`, `permissions.py`,
`fernet.py`, `watcher.py`, `format_datetime.py`, `file_loading.py`, `fake_datetime.py`
(superseded by `time-machine`, itself deferred to an optional extra — not a dependency), `mapping.py`,
`asserts.py`, `executor_loader.py`, `dag.py`, `paths.py`.

Rationale: these exist to test *Airflow itself* and *providers against live cloud services*. A Dag
author testing their own Dags needs none of it. `watcher.py` is for system-test Dags. `asserts.py`
is SQLAlchemy query-count assertions for core perf work.

### DROP — mocking modules (0 real usages)

`mock_operators.py`, `mock_executor.py`, `mock_plugins.py`, `mock_context.py` — fakes of *Airflow's
own internals*, built so Airflow can test Airflow. A Dag author mocks *their own* hooks with stdlib
`unittest.mock` (169 real usages in the fork vs. 2 for `pytest-mock`'s `mocker` — so `pytest-mock`
is also not a dependency). Where v1 *does* need a fake (Task SDK `SUPERVISOR_COMMS`), it's a
hand-written `Protocol`-conformant class, not `mock.create_autospec` — the `ty`-clean choice.

### DROP — `timetables.py`

Fixtures for testing custom `Timetable` implementations. Rare/advanced; those authors are testing
an Airflow extension point, not a Dag. Zero usages. Revisit only on request.

### DROP — API helpers, replaced by a small typed client

`api_fastapi.py` is entirely private helpers for asserting on Airflow's *own* REST responses —
not our concern. `api_client_helpers.py` is thin `requests` wrappers re-deriving host/token per
call. With the REST API v2 stabilized, v1 ships **one small typed client** bound to the `api_test`
server fixture (base URL + auth token already known) instead of free functions.

### KEEP, HEAVILY REDUCED — `db.py`: 1047 → ~150 lines

**481 of the 1047 lines are a hand-copied default-connections table.** Core Airflow now exposes
`get_default_connections()`/`create_default_connections()` directly
(`airflow-core/src/airflow/utils/db.py:201,209` — confirmed present and public). Delete the copy;
call core. Of the 26 `clear_db_*` functions, keep only what a Dag author touches (dags, runs, task
instances, serialized dags, xcom, variables, connections, assets, logs, bundles); drop
core-internals ones. Implement as one table-registry-driven function:
```python
def clear_db(*, tables: Collection[TableGroup] | None = None) -> None
```

### KEEP AS-IS (vendored, high value) — `taskinstance.py`

`run_task_instance()` (14 usages) and `ordered_task_instances()`. `run_task_instance()` is the
compat shim for `TaskInstance.run()`'s removal in Airflow 3.2
([apache/airflow#59835](https://github.com/apache/airflow/pull/59835)) — load-bearing glue every
3.2 adopter must otherwise reinvent. Its existing docstrings already explain *why*
dependency-checking is bypassed and *why* `session.expire_all()` is required. Vendor with
attribution and keep the reasoning intact.

### KEEP, REDUCED — `in_process_taskrun.py` + Task SDK fixtures

The DB-free, xdist-safe task execution path (`run_task`, 12 usages) is the single most compelling
"test without infrastructure" feature. Keep it; retype it cleanly (Protocol-based, no
`create_autospec`).

### UNIFY — config/env

Upstream's `config.py` has three overlapping things (`conf_vars`, `env_vars`,
`create_fresh_airflow_config`). Ship **one** context manager, usable as decorator or CM, handling
Airflow config keys *and* env vars through one code path. Keep `conf_vars` as a deprecated alias —
it's the one fixture *public Airflow docs* name by name (`core-concepts/dags.rst`).

### CONDENSE — logging: 9 layers → 3

Fork git archaeology (doc §10) found the same bug ("root logger's handlers get wiped, pytest's log
file goes silent") fixed reactively three separate times (2025-07, 2025-08→10, 2026-07) before the
root cause — `logging.config.dictConfig` unconditionally stripping root's handlers at unpredictable
points (Dag collection, provider init, API-server startup, `caplog` teardown) — was addressed
directly instead of patched at each newly discovered call site. Minimal correct footprint:

1. **`dictConfig` interception** — the actual fix. Reattach handlers after every call, regardless
   of which Airflow code path triggered it.
2. **`ensure_handlers()`** — one idempotent helper, called by the interception.
3. **`TestContextFilter`** — injects `worker_id`/`test_name` (orthogonal: attribution, not loss
   prevention).

**Dropped from the fork's version:** the per-test autouse fallback fixture (superseded — the
interception fires on *every* `dictConfig` call), commented-out dead vendored code (nothing to diff
against once we're not re-syncing from upstream), and — critically — coverage/XML-path bookkeeping,
which is not a logging concern and moves to its own function (`reporting.py`). This split also
makes both functions trivially typeable.

The fork's monkeypatch line currently carries `# ty: ignore[invalid-assignment]` — v1 solves this
via a typed module-level indirection (assign through a `Callable[[dict], None]`-typed variable, not
a bare reassignment of `logging.config.dictConfig`), no waiver needed.

**Self-test requirement (doc §16):** this is exactly the class of process-global mutation
`pytester.runpytest_inprocess` does not guarantee to clean up between runs (confirmed by pytest's
own test suite using `runpytest_subprocess` for its own logging-interaction tests). The
dictConfig-reattach regression test **must** use `runpytest_subprocess`.

### KEEP — `logs.py` reduced to `StructlogCapture`

Airflow 3 logs via structlog, so plain `caplog` misses records; 38 real usages make this
load-bearing. Keep the capture shim; drop `check_last_log` (core-internals testing).

### REDESIGN — `compat.py`/`version_compat.py`: capability-based, not version-based

The most important architectural decision for long-term survival. Upstream's pattern — bare
`try/except ImportError` scattered across ~10 blocks, plus `AIRFLOW_V_3_X_PLUS` booleans checked
inline at call sites, in a file explicitly commented *"THIS FILE IS COPIED MANUALLY IN OTHER
PROVIDERS DELIBERATELY"* — is exactly what makes upstream unpublishable: version branches leak into
every fixture.

**v1 design:** one `_compat/` subpackage is the *only* place that imports non-public Airflow paths
or branches on version; everything else imports from `_compat`. Prefer **capability probes**
(`HAS_RUNTIME_TASK_RUNNER`) over version checks (`AIRFLOW_V_3_2_PLUS`) — versions are a lossy proxy
for capabilities. Fail loudly once at session start by resolving every required internal symbol and
raising one actionable error, rather than an `AttributeError` 400 tests deep. Back this with a
documented support matrix and a CI job per supported Airflow minor (via `uv run --with
apache-airflow-core==X`, no extras/conflicts machinery — see Build/CI below).

### DROP the fork-specific production-host marker, keep the pattern

Generalize to a configurable "this test needs a real environment" gate (sentinel path from a
`[tool.pytest-airflow-in-a-box.environments]` table), same mechanism, zero hardcoded paths.

---

## Bootstrapping architecture — early controlled state, no FileLock-heavy convergence (doc §11)

**The fork's premise is false, and this changes the whole bootstrap design.** It assumes every
xdist worker independently re-runs `pytest_configure` with no way to know what an earlier process
decided, forcing convergence via files on disk: a `FileLock`-guarded port election (every worker
calls `get_open_port()` and races to write `airflow.cfg` first; late arrivals read the winner's
choice back out of the file), a second `FileLock` at `pytest_collection_finish` gating DB/API-server
init behind independent completion-marker files, and state threaded through a `Protocol`-typed
`_ExtConfig` accessed via `cast(_ExtConfig, config)` at every read site (because `pytest.Config` has
no typed extension point for plugin-owned state, absent `StashKey`).

**Correction validated 2026-08-07:** `workerinput` is unavailable while worker plugins and
conftests are imported, which is too late for Airflow configuration. The import-light `pytest11`
plugin establishes a unique run root and the minimum Airflow environment in
`pytest_load_initial_conftests`. Local xdist workers inherit that environment before import, then
`pytest_configure_node` and `pytest.StashKey` carry and validate typed state once pytest's normal
configuration objects exist. A subprocess launcher is available for users who need an absolute
ordering guarantee against unrelated auto-loaded plugins that import Airflow at module scope.

```python
state = load_early_environment()
validate_worker_state(config, state)
```

This dissolves three problems the fork solves with files on disk:

1. **Port election** — the controller binds and retains a loopback socket before starting the API
   child. No close-and-rebind race and no config-file round-trip.
2. **Cross-worker state** — inherited environment supplies pre-import state; `workerinput` validates
   JSON-serializable state after worker configuration.
3. **The `cast(_ExtConfig, config)` pattern** — replaced by `pytest.StashKey`, the sanctioned typed
   extension point for `pytest.Config`. Every read is type-safe with zero casts.

**What still needs synchronization:** DB initialization and the `api_test` live server run once in
controller-side `pytest_sessionstart(tryfirst=True)`, before xdist's session-start hook spawns local
workers. Serial runs use the same owner path. `Config.add_cleanup` handles partial startup failures;
normal teardown remains controller-owned. Remote xdist workers are explicitly unsupported because
they cannot share a controller-local SQLite database, filesystem, or loopback API server.

`sys.path`/`$PATH` sanitization (present in the fork for environment-specific tool-path allow-listing)
is a separate, optional concern — it does not belong in the core bootstrap and ships, if at all, as
an opt-in helper, not default behavior.

---

## Custom Dag-file collection (doc §12)

Goal: a user drops a real Dag `.py` file into a directory (e.g. `tests/dags/`), and the plugin
parses/validates it, registers it into the test DB, and collects it as one or more pytest items —
auto-marked `db_test`/`api_test` — including multiple pinned-`Param` test cases per Dag file.

**Verified end-to-end with a working prototype:**

- `pytest_collect_file(file_path, parent)` → custom `pytest.File` subclass, using
  `pytest.File.from_parent(parent, path=file_path)` (direct construction is deprecated).
- **Multiple parametrized items per file**: `collect()` yields one `Item` per declared test case
  via `Item.from_parent(self, name=case_name, params=pinned_params)`. Confirmed
  `pytest_generate_tests`/`metafunc.parametrize` do **not** compose with custom collectors — yielding
  items from `collect()` is the mechanism, not parametrize hooks.
- **Markers injected at collection**: `self.add_marker(pytest.mark.db_test)` inside the `Item`'s
  `__init__`; verified `-m db_test` correctly selects/deselects dynamically-collected items exactly
  like static ones.
- The Dag file is never imported by pytest's default Python collector when this is wired correctly.

**The double-collection trap, reproduced and fixed:** if a Dag file happens to match pytest's
default `python_files` pattern (e.g. named `test_something.py`, or passed directly on the command
line — `isinitpath` bypasses the pattern check entirely), the default Python collector *also*
collects it, producing duplicate/spurious items (reproduced: 5 items where 4 were correct).
`collect_ignore_glob = ["*.py"]` is **not** the fix — verified too blunt, it suppresses our own
collector too (drops to zero items). The working fix: `@pytest.hookimpl(tryfirst=True)` on
`pytest_collect_file` plus an explicit dedupe in `pytest_collection_modifyitems` dropping any
default-collector item under the Dag directory that isn't one of our own `Item` subclasses.

This needs dedicated self-test coverage for exactly this collision case, not just a happy-path
example — it's the single most likely place a user's naming convention silently breaks collection.

---

## AIRFLOW_HOME provisioning (the NFS problem)

**Constraint discovered on an NFS-backed CI host:** `$HOME` is NFS. Measured: tuned SQLite on NFS
is **148.5 ms vs 1.3 ms**
on local disk (~110× slower). Worse, it's a *correctness* hazard — SQLite creates `-wal`/`-shm`
sidecars on NFS and reports `journal_mode=wal`, so it **silently appears to work** while sitting on
the classic multi-process corruption vector. Since we deliberately share one DB across xdist workers
and subprocesses, the DB must never live on NFS.

The fork roots AIRFLOW_HOME at `config.cache._cachedir` (i.e. `.pytest_cache`, repo-relative) — a
latent bug on any NFS-hosted repo, which is *most* of the corp grid.

**Correction validated 2026-08-07:** `tmp_path_factory` is a fixture and is unavailable at the
pre-import bootstrap point. Resolve a safe base directory using stdlib filesystem APIs, then create
a unique per-run root with `tempfile.mkdtemp`. Honor explicit `--airflow-home`, `--basetemp`, and
`TMPDIR` candidates after network-filesystem checks. The controller publishes that root to local
workers through the early environment handoff. The observed layout was:

| | Path |
|---|---|
| worker basetemp | `/tmp/pytest-of-user/pytest-18/popen-gw2` |
| **`basetemp.parent`** | **`/tmp/pytest-of-user/pytest-18`** ← *shared by all workers* |

The shared-parent behavior remains useful evidence, but implementation cannot depend on fixture
construction timing. The plugin owns run-root cleanup and can retain failed-run state for diagnosis.

**Resolution order** (first viable wins), with the chosen path and reason logged once at session
start:
1. Explicit `--airflow-home=PATH` / ini option
2. `--basetemp` / `TMPDIR`, if not a network filesystem
3. `/dev/shm`, if tmpfs, writable, and adequately sized
4. Local-disk temp (`/tmp`), verified non-network
5. Anything writable + **loud warning** (failure mode is silent corruption, so this must be noisy)

**Network-FS detection** — verified working on both hosts via two independent methods (`psutil` is
absent on both, so neither is an option):
- `/proc/mounts` longest-prefix match against `{nfs, nfs4, cifs, smb*, fuse.sshfs, afs, 9p, lustre, gpfs, ceph}`
- `statfs(2)` f_type magic via `ctypes` (`0x6969` = NFS, `0xFF534D42` = CIFS, …)

Confirmed on the measured host: an NFS home reports `magic=0x6969, NETWORK=True`; `/tmp → xfs`,
`/dev/shm → tmpfs` both correctly non-network.

**How tmpfs interacts:** it's just step 3 of the same ladder — tmpfs is a *location*, and PRAGMAs do
the real work. Measured: tuned-on-disk (0.8 ms) beats untuned-on-tmpfs (1.8 ms); tuned tmpfs
(0.7 ms) is only marginally better than tuned disk. tmpfs is a nice-to-have, **not** required —
important because macOS/Windows have no tmpfs, and `/dev/shm` consumes RAM.

## SQLite tuning

PRAGMAs cannot go through `connect_args` (`sqlite3.connect()` has no pragma parameter). Apply via a
SQLAlchemy `connect` **event listener** — verified to apply correctly and persist for new pooled
connections after `engine.dispose()`. Hook it through Airflow's *documented* override point,
`create_metadata_engine()` (`settings.py:361-378`, whose docstring explicitly invites overriding it
in `airflow_local_settings.py` to register `do_connect` handlers) — **no monkeypatching**.

Defaults: `journal_mode=WAL`, `synchronous=OFF` (safe — a test DB is definitionally disposable, and
this is the single biggest lever: ~130× over untuned), `temp_store=MEMORY`, `mmap_size=256MB`,
`cache_size=-131072`, `busy_timeout=30000`, `page_size=8192`.

- **Never** `locking_mode=EXCLUSIVE` — it would lock out the subprocesses the harness needs.
- `mmap_size`/`cache_size` are **per connection**; scale from detected RAM/CPU
  (`os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')`) — the VM is 4 CPUs, the dev box 12.
- True in-memory (`:memory:`) is **impossible**: WAL silently downgrades to `memory`, each
  connection is a separate DB, and cross-process sharing cannot work (verified). This is also why
  in-memory forces `SingletonThreadPool`, which hard-rejects `pool_size`/`max_overflow`.

---

## `[tool.pytest.ini_options]` and `filterwarnings` (doc §13)

**Zero required ini config.** Verified: a plugin-only config with no `pytest.ini`/
`[tool.pytest.ini_options]` reproduces every piece of the fork's current boilerplate
(`--tb=short`, `-rasl`, verbosity, `--durations`, `filterwarnings`, `tmp_path_retention_*`, marker
registration) entirely from `pytest_configure`, via an "only set if unset" guard on `config.option`
— confirmed the guard preserves explicit user CLI flags (`--tb=long --durations=3` survived
unmodified). `testpaths` is excluded from this — project layout stays the user's call, never
guessed.

**`filterwarnings` narrowed to traced, third-party sources — not a blanket kill.** The fork's
`simplefilter("ignore", DeprecationWarning)` hides genuine Airflow deprecations along with the
noise. Traced both of the fork's named suppressions to their actual origin: `HTTP_422_...` is
**Starlette**'s renamed constant (Airflow has its own compat shim at
`api_fastapi/compat.py:34`); `"appbuilder.app is deprecated"` is **flask_appbuilder** (FAB
provider). Both third-party, not Airflow-core. v1 default filters (model taken from upstream
Airflow's own `pyproject.toml`, which does both narrow-ignore *and* promote-to-error):
```toml
"ignore::DeprecationWarning:flask_appbuilder",
"ignore::DeprecationWarning:flask_sqlalchemy",
"ignore:.*HTTP_422_UNPROCESSABLE_ENTITY.*:DeprecationWarning:starlette",
"error::pytest.PytestCollectionWarning",
"error::pytest.PytestUnraisableExceptionWarning",
```
Added via `config.addinivalue_line`, so user-supplied filters still take precedence.
**Airflow's own `DeprecationWarning`s stay visible by default** — the deliberate behavior change,
turning the plugin into an early-warning system for version breakage rather than a mute button.

---

## Package layout

Reconciles `conftest.py` + `tests/common/pytest_plugin.py` + `tests/unit/fixtures.py` into one
coherent installed package — the fork's split across those three files/layers is exactly the
"not properly structured like a real pytest plugin" problem being fixed. The configuration writer
is independently implemented from public Airflow behavior; corporate-only source is not copied.

```
pytest-airflow-in-a-box/
├── pyproject.toml                  # pytest11 entry point; ty/ruff pins; extras; NO [tool.pytest.*]
├── .pre-commit-config.yaml         # prek-managed Astral + repository hygiene hooks
├── .devcontainer/{devcontainer.json,Dockerfile}
├── .github/workflows/ci.yml        # native OS matrix; no tox/hatch/nox; act-reproducible
├── src/pytest_airflow_in_a_box/
│   ├── plugin.py                   # the ONLY pytest11-registered module; hooks live here or are
│   │                                 imported into it (pytest-django-style re-export, not
│   │                                 pytest_plugins= composition — that mechanism errors outside
│   │                                 a top-level conftest)
│   ├── _compat/                    # THE ONLY place touching Airflow internals or versions
│   │   ├── capabilities.py         # probe -> HAS_* booleans; session-start validation
│   │   └── taskrun.py              # run_task_instance, ordered_task_instances (vendored)
│   ├── bootstrap.py                # early env handoff + workerinput/StashKey validation
│   ├── storage/
│   │   ├── locate.py               # AIRFLOW_HOME resolution ladder + network-FS detection
│   │   └── sqlite.py               # PRAGMA profile + connect listener + engine override
│   ├── collection.py                # Dag-file pytest.File/Item collector + dedupe guard
│   ├── db.py                       # registry-driven clear_db; delegates default conns to core
│   ├── logging.py                  # 3 layers only
│   ├── reporting.py                # coverage/XML paths (split OUT of logging)
│   ├── config.py                   # unified airflow_config(); conf_vars alias
│   ├── airflow_cfg.py              # independently implemented config writer
│   ├── fixtures/{dag.py,session.py,taskrun.py,api.py,dagbag.py}
│   ├── markers.py                  # typed marker accessors (isinstance, not cast)
│   ├── types.py                    # public Protocols (DagMaker, RunTask, ...)
│   └── py.typed
└── tests/
    ├── conftest.py                  # `pytest_plugins = ["pytester"]` — the ONLY thing in it
    └── ...                          # self-tests, incl. dictConfig-reattach (subprocess) regression
```

**`pytest11` entry point** — the biggest ergonomics win over both upstream and the fork: `pip install`
and it self-registers, no `pytest_plugins = "..."` line for end users. Note this makes ordering
matter: the plugin must scrub env and configure paths *before* Airflow is imported (upstream
asserts `"airflow" not in sys.modules`), so `plugin.py` must stay import-light and establish
pre-import state in `pytest_load_initial_conftests`, never importing Airflow at module scope.

## Typing (`ty`, no waivers)

- `Protocol` + `Generic` for fixture factories — the pattern upstream *already* uses
  (`class DagMaker(Generic[Dag], Protocol)`), just actually checked.
- Annotate every hookimpl from pytest's public exports (`Parser`, `Config`, `Item`, `Session`,
  `CallInfo[T]`). Upstream leaves these bare, which is why it's excluded from checking.
- Marker args via typed accessors with real `isinstance` validation — never `cast`.
- Fakes as hand-written `Protocol`-conformant classes, not `create_autospec` (which returns `Any`).
- `pytest.StashKey` for all plugin-owned `Config`/`Node` state — replaces `cast(_ExtConfig, config)`
  entirely (see Bootstrapping above).
- Carry over the fork's strict config: `respect-type-ignore-comments = false` enforces the
  no-waivers rule at tool level. Also reject suppression comments in `prek`, because the source
  tree itself must not accumulate stale `# type: ignore`, `# ty: ignore`, or `# noqa` waivers.
- **Budgeted, unavoidable friction** (documented, not fought): pluggy's hookspec↔hookimpl matching is
  runtime-only — no checker can verify it; and `isinstance`/`inspect.is*function` dispatch in generic
  code needs a narrow ignore (even `pytest-asyncio` has 4). `ty` is beta (pin `==0.0.69`, current as
  of 2026-08-06) and lacks `Concatenate` — avoid decorators needing it.

## Self-testing with `pytester` (doc §16, §18)

`tests/conftest.py` declares `pytest_plugins = ["pytester"]` once, topmost — confirmed this must be
the topmost conftest, not per-file. Two run modes, used deliberately:

- `runpytest_inprocess` (default, fast) for ordinary fixture/marker/collection behavior.
- `runpytest_subprocess` for anything touching process-global state — **required** for the
  dictConfig-reattach test (confirmed via pytest's own suite using it for the same class of test)
  and for genuine cross-process xdist assertions.

**Directly adaptable template** for the `pytest_configure_node`/`workerinput` bootstrap tests:
`pytest-xdist`'s own `testing/acceptance_test.py::test_data_exchange` — a `pytester.makeconftest`
block setting `node.workerinput['a'] = 42` in `pytest_configure_node`, read back via
`hasattr(config, 'workerinput')` in `pytest_configure`, asserted via `runpytest("-v", p1, "-d",
"--tx=popen")` + `result.stdout.fnmatch_lines([...])`. Use `result.assert_outcomes(...)` for
outcome counts; drop to `--tx=popen` (real subprocess workers) specifically when asserting
cross-process behavior, matching xdist's own suite's threshold.

**Required self-test coverage, not optional**: dictConfig-reattach (subprocess), the double-collection
trap for Dag files matching `test_*.py` (§ Custom Dag-file collection above), the storage ladder's
NFS-detection branch (simulate by asserting detection logic against a known-NFS `/proc/mounts`
fixture, not by requiring a real NFS mount in CI), and controller→worker state handoff via
`workerinput` under both `-n0` and `-n2`.

## Compat suite — the representative tests the version matrix actually runs (doc §22)

The `compat` marker (run per `{OS} × {python} × {airflow-version}` cell) needs a *small, curated*
set of tests that exercise the shipped plugin as an end user would — build a Dag, run a task,
import a Dag bag, assert on the result — so a green compat matrix genuinely proves the public
surface works in each runtime environment. **This is not the fork's `compat` bucket, and none of it
is ported wholesale.**

Verified by scanning the fork's `tests/unit/` directly: its `compat` marker is a **48-file
catch-all** meaning "runs on the compat matrix," applied mostly to script/business-logic tests that
mock everything and never touch the plugin's own fixtures — only ~6 files actually pair `compat`
with `dag_maker`/`run_task_instance`. `smoke` (~9 tests) is closer to "minimal representative" in
shape, but is almost entirely DagBag/config validation bound to the private `dags/` tree. **No
existing fork test is a clean, dependency-free "end user drives the plugin" test** — the cleanest
structural templates all import private infrastructure and environment-specific tools. The v1
compat suite must be **newly authored** against benign,
in-test Dags/operators; the fork supplies idioms to copy, not shippable bodies.

**Four canonical idioms** (each verified present in the fork, cleanest exemplar cited):

- **A — DagBag import + structure assertion.** `tests/unit/dags/rtl/test_ip_integration_template.py`
  (39 lines, `@pytest.mark.compat` only, no DB): build a `DagBag`, assert `import_errors == {}`,
  assert leaf-task/`trigger_rule`/upstream-set topology. `full_dag_bag`
  (`tests/unit/fixtures.py:52-62`) is the whole-tree, session-scoped version — generalizes to
  "point at the user's Dag dir" for the plugin's public fixture, but the *minimal regression test*
  builds a fresh `DagBag` over a small bundled example dir instead, to stay self-contained.
- **B — custom operator subclass → run → assert output.** `create_task_instance_of_operator` +
  `session`, from `tests/unit/operators/test_bash.py:242-260`. Purest no-DB variant is
  `test_bash.py:32-52` (`test_bash_operator_init`): instantiate the subclass, assert default attrs,
  zero fixtures — the cheapest tripwire for Airflow API drift across the version matrix.
- **C — custom TaskFlow `@task` → run → assert XCom.** `tests/unit/decorators/test_bash.py:82-101`
  and `:152-174`: build via `dag_maker`, run via `run_task_instance`, assert
  `xcom_pull()`. Distinct plugin code path from B — Airflow churns operator-base and TaskFlow
  independently, so both need coverage.
- **D — inline Dag + run-all-TIs + assert DagRun SUCCESS.** The run-loop idiom repeated
  throughout the suite; cleanest compat-marked exemplar is
  `tests/unit/core/test_tasks.py::TestArchive.test_archive` (`@pytest.mark.compat` + `db_test` +
  `timeout(200)`), whose `archive` task body is nearly generic already. Use
  `ordered_task_instances()` instead of raw `get_task_instances()` once a Dag has real inter-task
  dependencies — Airflow 3.2's `_run_task` path doesn't dependency-sort for you.

**v1 compat suite (8 tests, all newly authored, zero private deps, ~20-40 lines each):**

| # | Test | Idiom | Fixtures | Proves |
|---|---|---|---|---|
| 1 | `test_dagbag_imports_example_dag` | A | `full_dag_bag`/fresh `DagBag` over bundled `tests/dags/` | Dag files parse with no import errors in this runtime |
| 2 | `test_dagbag_structure` | A | same | Topology assertions survive serialization round-trip |
| 3 | `test_custom_operator_executes` | B | `create_task_instance_of_operator`, `session` | An in-test operator subclass renders templates and `execute()`s correctly |
| 4 | `test_custom_operator_init_defaults` | B | none | Construction + default-attr assertions (no-DB, fastest, cheapest API-drift tripwire) |
| 5 | `test_taskflow_task_runs_and_xcoms` | C | `dag_maker`, `session` | A benign `@task` runs to SUCCESS and its XCom is pullable |
| 6 | `test_inline_dag_runs_to_success` | D | `dag_maker`, `session` | Multi-task inline Dag runs all TIs, DagRun state = SUCCESS |
| 7 | `test_skipped_task_state` | D | `dag_maker`, `session` | `AirflowSkipException` → `TaskInstanceState.SKIPPED` |
| 8 | `test_structlog_caplog_capture` | — | `caplog` (structlog override) | Airflow-3 structlog records are visible to the overridden `caplog` |

The example-Dag dir backing tests 1-2 is bundled fixture data, modeled on the fork's tiniest Dag
(`dags/examples/print_trigger_params_dag.py`, 35 lines: one `@dag` + one `@task`) — ship 2-3 tiny
Dags (single-task happy path, multi-task-with-dependency, deliberately-broken negative asserting
`import_errors` is populated). **This dir doubles as the Custom Dag-file collection fixture corpus**
above — one bundled asset serving two purposes. Note these 8 tests also implicitly exercise the
double-collection guard (a Dag named to look like `test_*.py` would collide), but that collision
needs its own dedicated `pytester` self-test — it's plugin-internal, not end-user-facing, so it
doesn't belong in the compat suite itself.

**Provenance boundary:** no corporate-only code, hostnames, paths, credentials, tests, or repository
history are copied. Public Airflow imports replace private facades, and all examples are newly
authored. `run_task_instance()`/`ordered_task_instances()` is vendored from the public Apache
Airflow source with its license header, modification notice, source path, and exact commit recorded.

## Developer tooling: `prek`, `ruff`, `ty`, and no inline waivers

The scaffold includes `.pre-commit-config.yaml` managed by `prek`, installed with
`uv run prek install`. Keep it aligned with the repo's Astral-first direction: hooks should invoke
`uv`/Astral tools directly, not `tox`, `nox`, `hatch`, or Makefile targets. Local hooks may
auto-fix; CI runs the equivalent check-mode commands.

**Dev dependency pins:** include `prek`, `uv`, `ruff==0.16.1`, and `ty==0.0.69` in the dev group.
`pytest-cov` remains test/CI-only and never moves into runtime dependencies.

**`prek` hook set:**

- `ruff format` and `ruff check --fix`
- `ty check`
- `uv lock --check`
- Standard hygiene: merge-conflict markers, trailing whitespace, end-of-file fixer, mixed line
  endings, YAML/TOML/JSON syntax, Python AST validity, debug statements, broken/destroyed symlinks,
  executable/shebang consistency, and added-large-files capped at 500 KB.
- Local `forbid-inline-waivers` hook rejecting suppression comments: `# noqa`, `# type: ignore`,
  `# ty: ignore`, `# pyright: ignore`, `# mypy: ignore`, and `# pylint: disable`.

**`ruff` policy:** match the lint families currently used in `infra-airflow_dags`:
`AIR`, `E`, `F`, `I`, `T20`, `TID`, `RUF`, `B`, `UP`, `SIM`, `C4`, `PIE`, `RET`, `ARG`, `PT`,
`PTH`, `DTZ`, and `PGH`. Do **not** carry over the fork's temporary ignores for `E501` or `F541`;
this package starts clean enough to let `ruff format` and real fixes handle those. Do **not** run
`ruff check --ignore-noqa`; inline waivers are rejected instead of bypassed. Keep import-boundary
rules package-appropriate: direct Airflow internals belong only under `_compat/`; other modules
import through `_compat`.

**`ty` policy:** carry over `error-on-warning = true` and
`respect-type-ignore-comments = false`. Check `src`, `tests`, and any committed `scripts`; include
`stubs` in `extra-paths` only if the repo actually creates a `stubs/` directory.

## Build and CI (doc §14, §16, §18, §20)

**Build backend: `uv_build`** (`requires = ["uv_build>=0.9"]`, `build-backend = "uv_build"`).
Verified: builds a real wheel from a `src/` layout with a `pytest11` entry point correctly landing
in `entry_points.txt`. No `hatch` (it depends on `uv` internally — redundant layering on a
uv-native repo), no `tox`/`nox` (the fork's own `[tool.tox]` env shape is portable but its CI
workflows are environment-specific and not inheritable; see below).

**Matrix testing needs no orchestrator, but does need Airflow's own constraints files — not
`update_deps.py`.** `uv run --isolated --with apache-airflow-core==X` resolves cleanly per Airflow
version with no shared state (verified). But verified separately that **unconstrained resolution
picks newer transitive deps than Airflow's own CI validated against that release** — e.g. for
3.1.8, unconstrained `alembic` resolved to 1.19.0 vs. Airflow's own pinned 1.18.4, `structlog`
26.1.0 vs. 25.5.0, `pydantic` 2.13.4 vs. 2.12.5. That's a real fidelity gap for a compat suite whose
purpose is catching version drift.

The fix is Airflow's own published `constraints-{version}/constraints-{python}.txt` files
(`https://github.com/apache/airflow/raw/constraints-{airflow_version}/constraints-{python_version}.txt`),
applied via `uv pip install --constraint` into an explicit venv — **not** `uv run --with`, which was
verified to silently ignore `UV_CONSTRAINT`/`--constraints` for ad-hoc ephemeral environments
(confirmed with a fresh cache dir to rule out staleness). Verified `uv pip install --constraint
<file> apache-airflow-core==X` reproduces Airflow's exact pins. Each CI matrix cell becomes:
```bash
uv venv --python "${{ matrix.python-version }}" .venv-ci
curl -sL "https://github.com/apache/airflow/raw/constraints-${{ matrix.airflow-version }}/constraints-${{ matrix.python-version }}.txt" -o constraints.txt
uv pip install --python .venv-ci --constraint constraints.txt "apache-airflow-core==${{ matrix.airflow-version }}"
.venv-ci/bin/pytest
```

**`scripts/update_deps.py` (the fork's 660-line constraints→pinned-extras generator) is explicitly
NOT adopted.** Its actual job is producing a *committed* dependency group of ~100 exactly-pinned
*provider* packages for a repo that deploys them to a real Airflow instance and needs `uv.lock` to
reproduce that deployed state — a concern this plugin never has. Ephemeral, per-matrix-cell
resolution against one Airflow core version for one test run is exactly what a constraints file
fed straight to `uv pip install --constraint` already does, with nothing generated and nothing to
keep in sync in a committed `pyproject.toml`. Revisit only if a committed, offline-reproducible
per-version lock becomes necessary for release-time verification — a "maybe later," not v1.

No `[tool.uv] conflicts` table or per-version pinned extras needed either way (those exist in the
fork because it *deploys* pinned providers, which this plugin never does).

**CI matrix: native OS runners, no Docker for OS/ISA coverage.** Confirmed structurally, not just
by preference: Docker containers share the host kernel, so "a macOS Dockerfile" is not a coherent
concept and Windows containers require a Windows kernel host — QEMU emulates CPU instructions, not
kernel syscalls. Confirmed via `psutil`/`cryptography`/`ruff`'s live CI workflows: all three cover
macOS/Windows exclusively via native runners, using Docker only for old-glibc/musl Linux userland
variance on a native Linux runner. Our own risk surface (SQLite/tmpfs/NFS filesystem semantics,
`fork` vs `spawn`, glibc vs musl) is kernel/libc-level, not ISA-level — exactly what native runners
cover and QEMU emulation does not touch.

```yaml
strategy:
  matrix:
    python-version: ["3.10", "3.11", "3.12", "3.13", "3.14"]
    airflow-version: ["3.1.8", "3.2.2", "3.3.0"]
```
The full Airflow compatibility matrix runs on Linux where matching constraints files exist, with
targeted macOS and Linux ARM jobs. Windows runs platform-independent unit tests only because
Airflow imports POSIX-only facilities. Plus one `container: python:3.x-alpine` job on
`ubuntu-latest` for the musl leg (no `tox-docker`,
just running pytest directly inside the container — matching `cryptography`'s own Alpine-job
pattern). `macos-13` (Intel) and `windows-11-arm` are optional broader-coverage additions, not
required for v1.

**Local reproduction via `act`** (verified end-to-end: real matrix substitution, `uv` install,
Python download, `pytest` run, `Job succeeded`). Document as the supported local-CI tool. Write
install steps shell-first (`curl -LsSf https://astral.sh/uv/install.sh | sh` rather than
`astral-sh/setup-uv@v5`) so they work under `act`'s default runner images without extra flags —
confirmed the Node-based marketplace-action pattern fails under `act`'s minimal images
([nektos/act#107](https://github.com/nektos/act/issues/107)); this also means fewer third-party
actions to pin/audit, independent of `act` compatibility. `act` cannot reproduce the macOS/Windows
legs (Linux-only, same kernel-sharing constraint) — those are verified by an actual push; this is an
acceptable split since most iteration happens on Linux.

**`pytest-cov`: CI-internal only, never a runtime dependency.** Lives in this repo's own `dev`/`test`
dependency group, wired into the CI job running our own test suite (coverage as an artifact/badge);
never appears in `[project.dependencies]` — installing `pytest-airflow-in-a-box` must never pull
coverage tooling into an end user's own environment.

## Devcontainer

`uv`-based, non-root, Python 3.12 default. Sets `TMPDIR` to a container-local path (never a mount)
so the AIRFLOW_HOME storage ladder lands on local disk by default; installs `ty`/`ruff` at the
pinned versions; mounts the repo only. Dockerfile doubles as the base image for the Alpine musl CI
leg's dependency layer where reasonable, to keep local dev and CI close.

---

## Build order

1. Repo scaffold: `pyproject.toml` (`uv_build`, pytest11 entry point, `prek`, `uv`,
   `ty==0.0.69`/`ruff==0.16.1` pins, strict `ruff`/`ty` config, extras),
   `.pre-commit-config.yaml`, devcontainer, `.github/workflows/ci.yml` skeleton
2. `storage/locate.py` + network-FS detection — **first**, since everything depends on where
   things live
3. `storage/sqlite.py` PRAGMA profile + `create_metadata_engine` override
4. `bootstrap.py`: early environment handoff plus `workerinput`/`StashKey` validation (replaces
   FileLock convergence) — controller-side DB init + `api_test` server start folded in here
5. `_compat/` capability probe + session-start validation
6. `logging.py` (3 layers) + `reporting.py` split, with the `runpytest_subprocess` reattach
   regression test
7. `collection.py`: Dag-file `pytest.File`/`Item` collector + `tryfirst` + dedupe guard, with a
   dedicated test for the `test_*.py`-named-Dag-file collision
8. `db.py` registry; delegate default connections to core
9. Fixtures in usage order: `session` → `dag_maker` → `run_task_instance` → `caplog` →
   `full_dag_bag` → `run_task`
10. `api_test` fixture (server start now lives in `bootstrap.py`, step 4) + small typed API client
11. Markers (`db_test`, `need_serialized_dag`, `api_test`, `compat`, env-sentinel) + typed accessors
12. `tests/conftest.py` (`pytest_plugins = ["pytester"]`); self-tests; `ty` clean; README with
    support matrix and `act` usage instructions
13. Bundled example-Dag corpus (`tests/dags/`: happy / multi-task-dep / deliberately-broken) +
    the 8-test compat suite (§ Compat suite above) — the end-user-facing tests the version matrix
    runs

## Verification

- `uv run prek run --all-files` passes, including the local inline-waiver rejection hook
- `uv run ruff format --check .`, `uv run ruff check .`, `uv run ty check`, and `uv lock --check`
  pass in CI check mode
- `ty check` passes with zero waivers; `respect-type-ignore-comments = false` and the `prek`
  waiver hook prove it
- Negative hook test: a temporary file containing `# noqa` or `# type: ignore` fails the waiver
  rejection hook
- Self-tests via `pytester`: dictConfig-reattach (`runpytest_subprocess`), `workerinput` handoff
  under `-n0`/`-n2` (adapted from xdist's `test_data_exchange`), Dag-file double-collection guard,
  storage ladder's NFS-detection logic
- Compat suite (8 newly-authored end-user-facing tests, zero private deps) green across the version
  matrix — the real proof the public surface works per Airflow version
- Real xdist run (`-n auto`) on both a local developer host and an NFS-home CI host — the latter is
  the real test, since it must place AIRFLOW_HOME off NFS automatically
- Benchmark tuned vs untuned SQLite to confirm the ~130× holds through the real fixture stack
- Local CI reproduction via `act` before every push during development
- Port a handful of the fork's actual tests as end-to-end proof
- CI matrix green across the native OS × Python × Airflow-version grid
