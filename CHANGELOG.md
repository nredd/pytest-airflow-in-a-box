# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

<!-- towncrier release notes start -->

## [0.9.0] - 2026-08-21

### Added

- Add the `airflow_local_settings` ini option to compose run-wide Airflow cluster policies without editing the generated file.
  ([#109](https://github.com/nredd/pytest-airflow-in-a-box/issues/109)).
- Add `check_component` for pure static conformance checks on custom timetables, listeners, and executors, catching shape bugs (a locally defined timetable, a listener hookimpl matching no hookspec or reachable from only one manager, a stale or wrong-type executor attribute) before they reach a scheduler.
  ([#110](https://github.com/nredd/pytest-airflow-in-a-box/issues/110)).
- Extend `check_component` to XCom backends, weight strategies, notifiers, secrets backends, policies, plugins, and providers, catching shape bugs (a removed `orm_deserialize_value`, an XCom backend override that cannot accept its real call shape, an unresolved abstract weight strategy or one that never overrides `__hash__`, a notifier missing `notify` or naming an unresolvable `template_fields` entry, a secrets backend getter that cannot return `None` on a miss, a policy hookimpl matching no hookspec or declaring an argument its hookspec does not accept, a plugin missing `name`, and a provider whose info dict fails schema validation, disagrees with its own distribution's name, or is never discovered at all for lack of an entry point) before they reach a scheduler, worker, or the Dag processor.
  ([#111](https://github.com/nredd/pytest-airflow-in-a-box/issues/111)).
- Add a `plugins/` directory to the generated `AIRFLOW_HOME`, plus the `airflow_plugins_folder`, `airflow_executor`, `airflow_xcom_backend`, `airflow_secrets_backend`, and `airflow_secrets_backend_kwargs` ini options, so a run can carry a plugins folder, an executor, an XCom backend, and a secrets backend, all fixed in `airflow.cfg` and the pre-import environment before the first Airflow import.
  ([#112](https://github.com/nredd/pytest-airflow-in-a-box/issues/112)).
- Add the `airflow_components` fixture yielding a `ComponentRegistry` that registers a custom plugin, listener, policy, secrets backend, or executor for one test and reverts every Airflow global registry it touched afterward, with every registration method running `check_component` first and raising on any conformance problem.
  ([#113](https://github.com/nredd/pytest-airflow-in-a-box/issues/113)).
- Add `timetable`, `priority_weight_strategy`, and `serialization_round_trip` methods to the `airflow_components` registry, teach `round_trip` to classify both new kinds, and make `dag_maker(schedule=...)` register a custom Airflow 3 timetable automatically before serialization so the `SerializedDagModel` round trip works without any plugin wiring.
  ([#114](https://github.com/nredd/pytest-airflow-in-a-box/issues/114)).
- Add the `airflow_isolated` marker, which runs marked tests in a one-shot child pytest process with a synthetic entry-point distribution on `PYTHONPATH` and `AIRFLOW__*` overrides applied before the first Airflow import, so `airflow.plugins`, `apache_airflow_provider`, and `airflow.policy` entry points -- and import-time-bound settings like a custom XCom backend -- are exercised through Airflow's real `importlib.metadata` discovery instead of monkeypatched caches; identically-marked module-mates share one child invocation.
  ([#115](https://github.com/nredd/pytest-airflow-in-a-box/issues/115)).
- Add the DB-free `task_context` fixture, opening a real Task SDK `RuntimeTaskInstance`-backed template context for hand-driven `execute()`/`post_execute()` testing.
  ([#191](https://github.com/nredd/pytest-airflow-in-a-box/issues/191)).
- `run_task`, `render_task`, and `task_context` now accept an operator that is not bound to any Dag: the fixture auto-creates and binds a synthetic `airflow.sdk.DAG` in place, named by an explicit `dag_id=` or a deterministic per-test, xdist-safe identifier, removing the `dag=DAG(...)` boilerplate DB-free tests previously required.
  ([#192](https://github.com/nredd/pytest-airflow-in-a-box/issues/192)).
- Add the `airflow_config` ini option for repo-wide Airflow configuration, applied as `AIRFLOW__SECTION__KEY` variables during pytest's initial parse -- before any consumer conftest is imported, and therefore before the first `full_dag_bag` parse. Lines are `section.key = value`; options this plugin's bootstrap owns (`database.sql_alchemy_conn`, `core.dags_folder`, and the rest of the isolation surface) are rejected with an error naming the supported knob instead, and `--airflow-doctor` echoes whatever is declared, redacting any value whose option name reads as a credential. `core.dagbag_import_timeout` is additionally rejected when the bundled smoke catalog is enabled, because the catalog pins that same variable from `airflow_dag_parse_timeout`.

  Add the session-scoped `airflow_configure` fixture, yielding a callable that applies `airflow_config` overrides for the rest of the session and restores them at teardown -- the supported form of the `scope="session", autouse=True` wrapper consumer repos were hand-rolling. Prefer the `airflow_config` ini option for anything that must precede the first Dag parse.

  Add the `airflow_home_path` and `airflow_dags_folder_path` fixtures, returning this run's isolated `AIRFLOW_HOME` and the Dag directory `full_dag_bag` parses, so consumer repos no longer import `pytest_airflow_in_a_box.bootstrap` internals to reach paths the plugin already resolves.
  ([#202](https://github.com/nredd/pytest-airflow-in-a-box/issues/202)).
- Suppress alembic's `path_separator` `DeprecationWarning` emitted from the plugin's own metadata-database bootstrap through the new `airflow_default_filterwarnings` ini option; redefining the option (even to an empty value) replaces the default filter list wholesale.
  ([#206](https://github.com/nredd/pytest-airflow-in-a-box/issues/206)).

### Changed

- Scaffold `docs/agents/` config (issue tracker, triage labels, domain docs) and an `## Agent skills` section in `AGENTS.md` for coding-agent tooling.
  ([#199](https://github.com/nredd/pytest-airflow-in-a-box/issues/199)).
- Flip release certification to probe-and-degrade: an uncertified Airflow 3.x release at or
  above the certified floor now resolves capabilities by live probing (`CertificationTier.PROBED`)
  instead of hard-failing, the component sandbox snapshots/clears unknown plugins-manager caches
  generically, and the degraded tier is surfaced via a once-per-session `UncertifiedAirflowWarning`,
  a `DEGRADED:` bullet in `--airflow-doctor`, and the new `ComponentReport.certification` field.
  The weekly canary now runs an explicit certification probe so a new upstream release files
  re-certification work before users hit the degraded tier.
  ([#212](https://github.com/nredd/pytest-airflow-in-a-box/issues/212)).
- Render the `--airflow-smoke` parse and serialization report tables with an owned plain-text renderer instead of Airflow's unstable `airflow.cli.simple_table.AirflowConsole` CLI internal. Row content, sort order, and log lines are unchanged; long paths are no longer wrapped to a fixed console width, and an empty report renders its header row instead of `No data found`.
  ([#213](https://github.com/nredd/pytest-airflow-in-a-box/issues/213)).
- Route the remaining runtime Airflow-internal imports outside `_compat` -- `airflow.utils.session.create_session`, `airflow.models.pool.Pool`, `airflow.configuration.conf`, `airflow.settings.configure_vars`, and `airflow.timetables.base.TimeRestriction` -- through deferred-import seams in `_compat`, and add a seam-guard test that fails on any new runtime `airflow` import outside `_compat`.
  ([#213](https://github.com/nredd/pytest-airflow-in-a-box/issues/213)).
- Deepen `v2_gate_message` into `require_v3(surface, detail)`, which performs the 2.x refusal
  itself (`pytest.fail(..., pytrace=False)`), and migrate all six gated fixtures to it. The
  remaining version decisions move into `_compat/capabilities.py`: the SQLite engine-override
  reliability boundary is now `sqlite_engine_override_reliable()` (reliable since Airflow 3.2.1,
  extrapolating to uncertified releases), and the migration CLI's supported-Python range is now
  `MIN_V3_PYTHON`/`MAX_V3_PYTHON` beside `MIN_V2_PYTHON`.
  ([#214](https://github.com/nredd/pytest-airflow-in-a-box/issues/214)).
- Unify the internals shared by the two component installation channels: both plugins-manager
  cache-invalidation surfaces (`clear_plugins_manager_caches`, `invalidate_component_lookup_caches`)
  now route through one private `_drop_caches` mechanic, the three timetable registration policies
  (strict `airflow_components.timetable`, the lenient `dag_maker(schedule=...)` gate, and
  `serialization_round_trip`) fold into one `_gate_and_register_timetable` implementation driven by
  policy data, and the precedence contract between the component ini options, the `airflow_config`
  overrides, and the `airflow_components` sandbox is now documented ("How the two channels compose"
  plus "Hazards at the sandbox seam" in the custom-components guide) and pinned by interaction tests:
  ini-seeded executors, secrets backends, plugins-folder plugins, and plugins-folder listeners all
  survive sandbox finalize, a sandbox-registered secrets backend stays visible through upstream's
  `ensure_secrets_loaded()` heuristic, and an `airflow_config` `core.executor` line outranks the
  `airflow_executor` ini.
  ([#215](https://github.com/nredd/pytest-airflow-in-a-box/issues/215)).
- Move `pytest-xdist` from a required runtime dependency to the new `xdist` optional extra (`pip install "pytest-airflow-in-a-box[xdist]"`).
  ([#217](https://github.com/nredd/pytest-airflow-in-a-box/issues/217)).
- Run the compat CI matrix under `-n auto --dist loadgroup` by default instead of serially, and add a `make test-xdist` target so contributors can reproduce that configuration locally. `make test` stays serial because the coverage gate depends on it.

### Fixed

- Guard against a foreign `airflow_local_settings` module silently shadowing the generated one and dropping SQLite engine tuning.
  ([#109](https://github.com/nredd/pytest-airflow-in-a-box/issues/109)).
- Harden `test_failed_context_body_does_not_persist_metadata` to raise a sentinel exception instead of `RuntimeError`, so an entry-path `DagPersistenceError` can no longer be masked as a `pytest.raises` regex mismatch.
  ([#153](https://github.com/nredd/pytest-airflow-in-a-box/issues/153)).
- Tolerate the Airflow 2.x `dag_code.fileloc_hash` UNIQUE-constraint race between
  pytest-xdist workers sharing one metadata database: the 2.x Dag metadata sync now rolls
  back and retries a concurrent-writer `IntegrityError` naming `dag_code`, and Dag cleanup
  no longer deletes the shared per-file `dag_code` row (which re-armed the race on every
  teardown). Other constraint violations, sessions carrying staged user state, and the 3.x
  family deliberately keep no retry.
  ([#157](https://github.com/nredd/pytest-airflow-in-a-box/issues/157)).
- Retry the Airflow constraints download in CI on transient network failures instead of
  failing the leg outright.
  ([#201](https://github.com/nredd/pytest-airflow-in-a-box/issues/201)).
- The SQLite engine-override version check now compares release tuples instead of string
  prefixes, so a future Airflow 3.10.x on the probed tier is no longer misclassified as
  override-unreliable by `startswith("3.1.")` and no longer gets the legacy listener installed
  on top of the working `create_metadata_engine` override.
  ([#214](https://github.com/nredd/pytest-airflow-in-a-box/issues/214)).
- Fix the custom-components guide's executor alias documentation: `airflow_config(executor=...)` is
  not a real signature, and no configuration surface can select a sandbox executor alias at all --
  the `airflow_executor` ini is resolved before the first Airflow import, when no alias exists yet,
  and a `core.executor` override is silently ignored because `ExecutorLoader` has already memoized
  its config parse by the time the alias exists. The alias resolves through
  `ExecutorLoader.load_executor(alias)` / `ExecutorLoader.lookup_executor_name_by_str(alias)`.
  ([#215](https://github.com/nredd/pytest-airflow-in-a-box/issues/215)).

## [0.8.0] - 2026-08-17

### Added

- `--airflow-parse-secrets` and the `airflow_parse_secrets` ini option select the parse-time
  resolution policy, `metastore` (default) or `off`; `off` leaves Airflow's own resolution
  in place for tests that assert the un-shimmed behavior
  ([#117](https://github.com/nredd/pytest-airflow-in-a-box/issues/117)).
- `Variable.get()` and `BaseHook.get_connection()` written at Dag *top level* now resolve
  from the rows `airflow_variables` / `airflow_connections` commit, instead of failing with
  `ImportError: cannot import name 'SUPERVISOR_COMMS'` on Airflow 3.1 or missing silently
  on 3.2+. Airflow 3 routes both lookups through a supervisor the test process does not
  have, so the plugin installs one for the duration of every parse it runs --
  `full_dag_bag`, the `--collect-dag-folder` import items, and the smoke catalog's corpus
  build. Lookups are lazy, so a Dag folder that never reads a Variable or Connection never
  opens the database. Airflow 2.x reads the metastore directly at parse time and is
  unaffected. Upstream has had this open since 2025
  ([apache/airflow#51816](https://github.com/apache/airflow/issues/51816),
  [#48554](https://github.com/apache/airflow/issues/48554)) and
  [PR #61630](https://github.com/apache/airflow/pull/61630) states plainly that it does not
  fix the root cause
  ([#117](https://github.com/nredd/pytest-airflow-in-a-box/issues/117)).
- `airflow_parse_secrets` fixture, resolving the same top-level lookups for a whole test
  rather than for one parse -- for a `Variable.get()` inside a `with dag_maker(...)` block
  or in the test body, where no Dag file is being parsed
  ([#117](https://github.com/nredd/pytest-airflow-in-a-box/issues/117)).
- A DB-free `render_task` fixture (and matching `_compat.render_task_in_process`) rendering
  one operator's `template_fields` through the Task SDK's public `render_template_fields`,
  plus a `rendered(...)` matcher for one-expression assertions -- the documented replacement
  for `TaskInstance.render_templates()`, which Airflow 3.2+ removed
  ([#118](https://github.com/nredd/pytest-airflow-in-a-box/issues/118)).
- Five Dag anti-pattern smoke checks, on by default whenever the catalog is enabled, each
  with an ini to disable or tune it: `test_no_top_level_variable_access` (AST scan plus
  runtime interception of `Variable`/`Connection` lookups during the corpus `DagBag` fill),
  `test_no_top_level_io` (import-resolved calls into known I/O modules, list configurable
  via `airflow_top_level_io_modules`), `test_dag_parse_budget` (relative-to-median parse
  budget, `airflow_dag_parse_budget_ratio`, floored at one second), `test_forbid_catchup`,
  and `test_no_unbounded_expand` (mapped tasks expanding over runtime data without
  `max_active_tis_per_dag`)
  ([#119](https://github.com/nredd/pytest-airflow-in-a-box/issues/119)).
- A "Concurrent local runs" note in `docs/development.md` documenting pytest's shared-tmpdir
  garbage-collector race that can exit a session non-zero despite an all-passed summary, why this
  plugin's `tmp_path_retention_policy = "failed"` default makes it likelier than a bare pytest
  install, and the `tmp_path_retention_policy=all` / `PYTEST_DEBUG_TEMPROOT` / `TMPDIR`
  workarounds (the last two with their `AIRFLOW_HOME` storage-ladder tradeoffs)
  ([#158](https://github.com/nredd/pytest-airflow-in-a-box/issues/158)).
- `airflow_smoke_disable` ini option to persistently drop any bundled smoke item from the
  catalog; disabling every serialization-backed item skips calling the Airflow DAG serializer
  entirely while building the corpus
  ([#162](https://github.com/nredd/pytest-airflow-in-a-box/issues/162)).
- A `run_dag` fixture drives a Dag pulled from `full_dag_bag` (or otherwise authored outside
  `dag_maker`) through a full DagRun and returns the same `DagRunResult` snapshot
  `dag_maker.run()` does, so a Dag already living in your `dags/` folder can be executed
  without adopting `dag_maker`'s inline-authoring shape
  ([#164](https://github.com/nredd/pytest-airflow-in-a-box/issues/164)).
- A "What a dagbag + callable test misses" section in `docs/guide/cookbook.md`, running one
  realistic multi-task `ingest` Dag through `dag_maker` to show task relations (trigger rules,
  branching, cross-task xcom), asset-triggered cross-Dag relations, depends-on-past/backfill
  DagRun sequences, and retry behavior (`up_for_retry`, `try_number`) that a dagbag-import-plus
  callable test cannot reach. Linked from README's `Why not...` section
  ([#165](https://github.com/nredd/pytest-airflow-in-a-box/issues/165)).
- Added `pytest_airflow_in_a_box.assets.evaluate_asset_schedules`, evaluating a consumer Dag's
  `AssetTriggeredTimetable`/`DatasetTriggeredTimetable` condition against queued asset/dataset
  events in the isolated metadata database and creating its `QUEUED` `DagRun` with
  `consumed_asset_events`/`consumed_dataset_events` attached. Closes the cross-Dag asset-triggering
  gap `dag_maker`/`full_dag_bag` left as static-only wiring assertions, on both the Airflow 3
  `Asset` and certified 2.x `Dataset` spellings
  ([#166](https://github.com/nredd/pytest-airflow-in-a-box/issues/166)).
- A "Retry behavior" recipe in `docs/guide/cookbook.md` covering `fail -> up_for_retry ->
  succeed` state-math progression: asserting `try_number` and `retry_delay` at the
  `TaskInstance` level and the user's `on_retry_callback` firing, without a wall-clock wait
  ([#167](https://github.com/nredd/pytest-airflow-in-a-box/issues/167)).
- A `docs/guide/testing-scope.md` page ("What to test") drawing the scope line between the
  Airflow code you wrote -- Dags, plus custom operators, hooks, sensors, decorators, and
  connection types -- and the Airflow mechanisms Airflow's own suite already covers, with both
  bounds named: mechanism tests below, provider/core development above (Breeze and
  `tests_common` territory), and the one legitimate exception -- a pre-upgrade regression suite
  pinned before a version bump. Restates the same scope in the `README.md`/`docs/index.md`
  intros
  ([#168](https://github.com/nredd/pytest-airflow-in-a-box/issues/168)).

### Changed

- The generated test `airflow.cfg` now pins `[scheduler] catchup_by_default = False` on both
  families, so an effective `dag.catchup` no longer flips with the installed family (2.x
  defaults it to `True`) for a value the Dag never set
  ([#119](https://github.com/nredd/pytest-airflow-in-a-box/issues/119)).
- The shared smoke corpus artifact schema is now version 2, carrying per-Dag `catchup` and
  `fileloc`, per-task mapping metadata, and recorded runtime secrets lookups; mixed-version
  workers reject stale artifacts loudly
  ([#119](https://github.com/nredd/pytest-airflow-in-a-box/issues/119)).
- The README quickstart, `docs/index.md`, and `docs/guide/task-execution.md` now lead with
  loading an existing Dag via `full_dag_bag` + `run_dag`; the inline `dag_maker` example moves
  to a clearly labeled secondary "adhoc Dag" path
  ([#164](https://github.com/nredd/pytest-airflow-in-a-box/issues/164)).
- The `docs/index.md` requirements bullet no longer reads "Apache Airflow 3.1 or newer, below 4",
  which contradicted the certified 2.7-2.11 tier documented in the same section and the
  `airflow2` extra; it now names both floors
  ([#168](https://github.com/nredd/pytest-airflow-in-a-box/issues/168)).

### Fixed

- The bundled smoke catalog no longer parses the Dag folder a second time, in parallel,
  when a `full_dag_bag` consumer lands on a different `pytest-xdist` worker under
  `--dist loadgroup`. When both are present in the run and would survive an active `-m`
  expression, the plugin now puts the catalog and one `full_dag_bag` consumer into a
  shared `xdist_group`, forcing `--dist loadgroup` to schedule them onto the same worker
  so the existing process-local `DagBag` cache has a chance to be reused instead of two
  workers each paying a full parse concurrently. An item that already carries its own
  explicit `xdist_group` is left untouched, and only one consumer is ever grouped, so a
  suite with many `full_dag_bag` consumers does not have all of their execution
  serialized onto a single worker just to save one parse
  ([#163](https://github.com/nredd/pytest-airflow-in-a-box/issues/163)).

### Security

- Bump the transitive `sqlparse` dependency to 0.6.0, fixing quadratic-CPU denial-of-service
  issues in the lexer, token grouping, and `format(sql, reindent=True)`
  (CVE-2026-59893, CVE-2026-54284, CVE-2026-71491), plus unescaped backslashes in the
  `python`/`php` output formatters that could break out of the generated string literal
  (CVE-2026-59894).
  ([#179](https://github.com/nredd/pytest-airflow-in-a-box/issues/179)).

## [0.7.2] - 2026-08-15

### Changed

- README intro now names the certified Airflow 2.7-2.11 compatibility tier alongside
  Airflow 3, matching the badge and `Requirements` table
  ([#160](https://github.com/nredd/pytest-airflow-in-a-box/pull/160)).

## [0.7.1] - 2026-08-15

### Fixed

- `pytest -p no:logging` under xdist no longer aborts every worker with
  `AttributeError: 'Namespace' object has no attribute 'log_file'`, nor the
  `KeyError: <WorkerController gw0>` that xdist's controller raises downstream over a node
  that never came up. `configure_reporting` now guards its `log_file` lookup the way the
  sibling `configure_report_dir` already did, so a disabled `logging` plugin leaves nothing
  to scope instead of crashing. Serial runs were never affected -- worker scoping returns
  early off the controller, so only workers reached the lookup. `-p no:junitxml` was
  already safe: `xmlpath` is only ever written behind its own `hasattr` guard
  ([#151](https://github.com/nredd/pytest-airflow-in-a-box/issues/151)).
- `make release` no longer pushes a tag it failed to create. The recipe now opens with
  `set -eu` and chains `git tag`, `git push`, and the success banner with `&&`, so a
  re-run against an already-existing tag stops at `git tag` instead of pushing whatever
  that tag already pointed at and then printing "Tag vX.Y.Z pushed."
  ([#152](https://github.com/nredd/pytest-airflow-in-a-box/issues/152)).

## [0.7.0] - 2026-08-15

### Added

- A "Dag coverage" guide page documenting how Dag-repo authors measure their own Dag
  files (`--cov=dags`), why the in-process `SourceFileLoader` parse makes it work, the
  `--cov=src`-only footgun, the two Dag folder options plus the scratch fallback to
  never feed to `--cov`, `relative_files = true` for CI merging, and the xdist
  per-worker data-file story
  ([#138](https://github.com/nredd/pytest-airflow-in-a-box/issues/138)).
- `--airflow-doctor` now prints a "Dag coverage" section: the resolved Dag and
  collection folders, whether `pytest-cov` is installed and active (detected without
  importing it), and whether the Dag folder sits inside a configured `--cov` source,
  with a copy-pasteable fix when it does not
  ([#138](https://github.com/nredd/pytest-airflow-in-a-box/issues/138)).
- Every run now names its isolated `AIRFLOW_HOME` in the pytest session header, along with the
  storage-ladder rung that chose the base and the metadata database backend, e.g.
  `pytest-airflow-in-a-box: AIRFLOW_HOME=/dev/shm/pytest-airflow-in-a-box-8f2a1c (storage:
  shared-memory, db: sqlite)`
  ([#140](https://github.com/nredd/pytest-airflow-in-a-box/issues/140)).
- `--airflow-home-retention` and the `airflow_home_retention_policy` ini option keep the isolated
  run directory after a session -- `all`, `failed`, or `none`, defaulting to `failed` to match
  the house `tmp_path_retention_policy`. A retained directory is reported again in the terminal
  summary, with an extra warning when it sits on RAM-backed `/dev/shm`, and on `stderr` from
  cleanup when the session died before either terminal channel could run. A session that started
  and died before pytest could record an outcome counts as failed, so a crash retains rather than
  discards; an invocation that never started a session at all (`pytest --help`, an argparse usage
  error, an abort during `pytest_configure`, `--airflow-doctor`, or a run that collected no
  tests) does not, since nothing ever touched the directory. The metadata database provisioner
  still stops on every policy, so a retained run never leaks a testcontainers Postgres container
  ([#140](https://github.com/nredd/pytest-airflow-in-a-box/issues/140)).
- `--airflow-report-dir=PATH` (ini: `airflow_report_dir`) derives pytest's own report
  destinations inside PATH: `pytest.log` at `--log-file-level=DEBUG` and `pytest.xml` in the
  stock `xunit2` family. Opt-in and inert when unset, every derived destination yields to an
  explicit `--log-file` / `--log-file-level` / `--log-level` / `--junit-xml`, and the log file
  is scoped per `pytest-xdist` worker like any other. `action/action.yml` gains a matching
  `report-dir` input (creates the directory, appends the flag to `PYTEST_ADDOPTS`, skipping
  the append when the installed plugin version predates the option) plus a `report-dir`
  output, and every CI pytest invocation now archives its reports with `if: always()`
  ([#137](https://github.com/nredd/pytest-airflow-in-a-box/issues/137)).
- The `Coverage` job now writes the full per-module `Stmts`/`Miss`/`Branch`/`BrPart`/`Cover`
  table -- not just the `TOTAL` row that `skip_covered = true` leaves on a green build -- to
  the run's job summary, viewable in the GitHub Actions web UI without downloading an
  artifact. The PR coverage comment stays a short `TOTAL` line by default, with the same
  table one click away in a collapsed `<details>` section and a link to the run's summary
  page ([#148](https://github.com/nredd/pytest-airflow-in-a-box/issues/148)).
- Certified Airflow 2.7.3 and 2.8.4, reaching the 2.x tier back across the whole 2.x era
  -- 24 alembic revisions and the FAB auth-manager package extraction below the previous
  floor. Both run the consumer contract (`tests/enduser`) on CPython 3.11 in the compat
  matrix, and the `airflow2` extra floor drops to `apache-airflow>=2.7,<3`
  ([#139](https://github.com/nredd/pytest-airflow-in-a-box/issues/139)).
- `AirflowCapabilities.max_python` carries each certified release's own Python ceiling,
  so the 2.x guard rejects, for example, 2.7.3 on CPython 3.12 (for which Airflow
  publishes no constraints file) instead of letting it fail opaquely later
  ([#139](https://github.com/nredd/pytest-airflow-in-a-box/issues/139)).
- `AirflowCapabilities.dag_requires_start_date` gates a `dag_maker` shim that supplies an
  implicit `start_date` on releases below 2.8, whose `DAG.add_task` rejects a Dag when
  neither it nor its tasks carry one even with no schedule declared. The injection is
  scoped to exactly the case 2.8 stopped rejecting, so a scheduled Dag without a
  `start_date` still raises on every certified release
  ([#139](https://github.com/nredd/pytest-airflow-in-a-box/issues/139)).

### Fixed

- A metadata-database provisioner failure no longer leaks the half-provisioned run directory.
  `PostgresProvisioner.start` reports an unreachable Docker daemon or a failed image pull as a
  `pytest.UsageError`, which the bootstrap cleanup path did not catch, so every failed
  `--airflow-db-backend=postgres` attempt left a full run root behind
  ([#140](https://github.com/nredd/pytest-airflow-in-a-box/issues/140)).
- `_register_v2_orm_models` now falls back to the in-tree
  `airflow.auth.managers.fab.models` when `airflow.providers.fab.auth_manager.models` is
  absent. Below Airflow 2.9 the FAB auth-manager models had not yet been extracted into
  `apache-airflow-providers-fab`, so the provider-only import failed unconditionally and
  the shim degraded to an INFO log, leaving `ab_user` unregistered in any process that
  did not itself migrate -- an xdist worker that loses the `ensure_database` race and
  then flushes a `TaskInstanceNote`
  ([#139](https://github.com/nredd/pytest-airflow-in-a-box/issues/139)).
- `airflow-migration-diff` defaults `--python-airflow2` from the requested
  `--airflow2-version`'s own ceiling rather than the family-wide one, so
  `--airflow2-version 2.7.3` on a CPython 3.12 host provisions 3.11
  ([#139](https://github.com/nredd/pytest-airflow-in-a-box/issues/139)).
- Flip the `action.yml` symlink direction so the repo root holds the real composite-action
  manifest and `action/action.yml` symlinks back to it, instead of the other way around.
  GitHub Marketplace's publish-eligibility check reads the root `action.yml` as a raw git
  blob and doesn't follow symlinks, so a `120000` symlink blob there kept the "Publish this
  Action to the GitHub Marketplace" banner from ever appearing even though the Contents API
  and the Actions runner both resolved it fine
  ([#142](https://github.com/nredd/pytest-airflow-in-a-box/issues/142)).

## [0.6.0] - 2026-08-15

### Added

- A composite GitHub Action (`action/action.yml`) wrapping constraints-pinned `uv` +
  Airflow + plugin setup for Dag repos' own CI, matrix-ready and installing the
  published PyPI package rather than an editable checkout; both the `airflow3` and
  `airflow2` extras are supported
  ([#38](https://github.com/nredd/pytest-airflow-in-a-box/issues/38)).
- `release.yml` now moves a `v<major>` tag (e.g. `v0`) to the latest published release on
  that major line, and a root-level `action.yml` symlinks to `action/action.yml` so the
  repo satisfies GitHub Marketplace's root-manifest requirement
  ([#132](https://github.com/nredd/pytest-airflow-in-a-box/issues/132)).

### Fixed

- `make release` and `release.yml`'s tag-version check no longer false-positive when `uv`
  colorizes `uv version --short` output (e.g. forced-color env vars); both now pass
  `--color never` ([#126](https://github.com/nredd/pytest-airflow-in-a-box/issues/126)).
- `-m smoke` combined with an explicit file or node-ID positional (e.g.
  `pytest test_foo.py -m smoke --airflow-smoke`) no longer silently selects nothing: an
  explicit `-m` expression that would select a smoke item now overrides the positional
  scoping that otherwise drops the bundled catalog
  ([#133](https://github.com/nredd/pytest-airflow-in-a-box/issues/133)).

## [0.5.0] - 2026-08-14

### Added

- Migration outcome diff: `--airflow-record=PATH` writes a versioned JSON artifact of
  per-test outcomes at session finish, `--airflow-baseline=PATH` compares live outcomes
  against a prior recording (seven categories: still-passing, broken-on-both,
  regression, fixed, gated, new, missing), `--airflow-baseline-select` filters
  collection by baseline outcome, and `--airflow-baseline-xfail=PATH` non-strict
  xfail-marks known regressions during the migration
  ([#42](https://github.com/nredd/pytest-airflow-in-a-box/issues/42)).
- The Airflow 2.x compatibility tier ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)),
  certified against 2.9.3, 2.10.5, and 2.11.2 on CPython 3.10-3.12
  ([#41](https://github.com/nredd/pytest-airflow-in-a-box/issues/41)): `dag_maker` (2.x
  `execution_date` interface, no bundles/DAG versioning), `dag_maker.run()` whole-DagRun
  execution with family-aware mapped-task expansion (2.x `MappedOperator` predates the
  `is_mapped` attribute, so detection is class-based there),
  `run_ti`/`run_task_instance`, `full_dag_bag`, `clear_db` (renamed `dataset*` tables,
  `BaseXCom`), seeding, params validation via `airflow.models.param`, and the bundled
  smoke checks all run on both families; `run_task`, `cap_structlog`, and the REST API
  fixtures fail on 2.x with actionable errors naming the 2.x alternative.
- `requires_airflow2` / `requires_airflow3` markers, auto-skipped on the other family,
  plus three 2.x CI legs (two-pass constraints install) running the end-user consumer
  contract; the 2.x-collectable surface is widened below
  ([#83](https://github.com/nredd/pytest-airflow-in-a-box/issues/83)).
- The bundled Dag corpus authors through a small dynamic resolver so the same files
  parse on both Airflow families ([#86](https://github.com/nredd/pytest-airflow-in-a-box/issues/86)).
- `--airflow-doctor` now reports the resolved `core.executor` and flags a 2.x SQLite run
  whose configuration overrides the plugin's `SequentialExecutor` default with a
  multi-threaded executor, which Airflow's `ready_to_reschedule` dependency rejects
  ([#105](https://github.com/nredd/pytest-airflow-in-a-box/issues/105)).
- Certified Airflow 3.3.1, which the `airflow3` extra now resolves fresh (the new
  extra-resolving CI smoke leg caught the drift on its first run). 3.3.1 regenerated
  the Task SDK comms models with every None-able field required-without-default, so the
  DB-free runner now sends explicit `None` for the declared `DagRun` fields
  (`end_date`, the new `partition_key`) and `FakeSupervisorComms` completes seeded
  `ConnectionResult` payloads the same way, keyed by validation alias.
- `--airflow-migration-strict` / `airflow_migration_strict` ini option: on the Airflow 2.x
  family, promotes `RemovedInAirflow3Warning` and `AirflowProviderDeprecationWarning` to
  test-phase errors, turning a 2.11 run into a forecast of 3.x breakage with no 3.x
  environment needed; a no-op (warned once) off 2.x
  ([#43](https://github.com/nredd/pytest-airflow-in-a-box/issues/43)). Fixing this also
  closed a latent bug independent of the new flag: `ensure_database` now runs under its own
  default-filter warnings context unconditionally, because on an `xdist` worker it executes
  from inside the runtest phase's own warning context, where any consumer `error::` filter
  covering a warning Airflow's own bootstrap happens to raise could already turn database
  initialization into a misleading `AirflowCompatibilityError` on that worker's first test.
- `airflow-migration-diff`, a console script that `uv`-provisions a disposable Airflow 2.x
  environment and a disposable Airflow 3.x environment, records outcomes on each with
  `--airflow-record`/`--airflow-baseline`, and prints the categorized migration diff; exit
  code 0 means no regressions, 1 means at least one was found, and 2 means the orchestrator
  itself failed. Categorization is `--airflow-record`'s own `compute_categories`, not a
  reimplementation ([#44](https://github.com/nredd/pytest-airflow-in-a-box/issues/44)).

### Changed

- Controller-to-worker `pytest-xdist` bootstrap handoff now has focused happy-path unit coverage
  in every leg of the full Airflow/Python compatibility matrix, complementing the live
  nested-worker scenarios added by [#45](https://github.com/nredd/pytest-airflow-in-a-box/issues/45)
  ([#102](https://github.com/nredd/pytest-airflow-in-a-box/issues/102)).
- BREAKING: Airflow is no longer a base dependency. The Airflow 2.x monolith and the 3.x
  core both install the `airflow` package, so the previous hard `apache-airflow-core>=3.1,<4`
  pin would silently corrupt any Airflow 2.x environment the plugin was installed into --
  the packaging prerequisite for the planned 2.x compatibility tier, superseding the 0.2.0
  note that 2.x support would require a parallel implementation
  ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)). Install the new
  `airflow3` extra (`apache-airflow>=3.1,<4` plus the sqlite provider) for the previous
  behavior, or keep pinning Airflow yourself. `sqlalchemy>=1.4.36,<3` and `packaging>=22`
  are newly direct base dependencies (previously transitive via Airflow): the storage
  layer imports `sqlalchemy` at plugin load and the capability probe parses versions
  with `packaging`. An `airflow2` extra (`apache-airflow>=2.9,<3`, resolving only on
  Python <= 3.12) ships ahead of the tier. Requesting both Airflow extras fails at
  resolution for pip and uv alike because the version ranges are disjoint; this repo's
  own `[tool.uv] conflicts` table additionally keeps the two extras lockable here (it is
  not wheel metadata and does not travel to consumers).
- The capability seam, bootstrap, config writer, storage ladder, and DB cleanup registry
  are family-aware (`AirflowFamily`, `BootstrapState.family`, bootstrap state version 4)
  ahead of the 2.x fixture tier ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)).
  On the 2.x family bootstrap env-pins `AIRFLOW__CORE__EXECUTOR=SequentialExecutor`:
  Airflow 2.x's `unit_test_mode` overlays its internal `unit_tests.cfg` (which hard-codes
  `LocalExecutor`) over any written `airflow.cfg`, and the `ready_to_reschedule`
  dependency rejects that combination with SQLite for poke- and reschedule-mode sensors
  alike; environment variables are the one channel that outranks the overlay, and
  `airflow_config` overrides still win for tests that need another executor.
- The first test that needs the metadata database now distinguishes the possible Airflow
  installation states with actionable single-line errors (`pytest.UsageError`, not an
  `INTERNALERROR` traceback; under `pytest-xdist` the same message renders as per-test
  setup errors instead of crashed workers): no Airflow at all, Airflow 2.x without the tier that
  supports it, an `apache-airflow` meta-package without `apache-airflow-core`, and the
  corrupt case of `apache-airflow<3` coexisting with `apache-airflow-core`. Runs without
  Airflow-facing tests remain untouched, and `--airflow-doctor` renders the same
  diagnosis ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)).
- The corpus `_resolve()` dynamic-import helper moves out of the 9 Dag files that each
  copy-pasted it into a shared `tests/dags/_family.py`, reached the same way `provider.py`
  already reaches `provider_package` (`sys.path` insertion plus `import_module`) since
  DagBag imports every corpus file as a standalone module
  ([#86](https://github.com/nredd/pytest-airflow-in-a-box/issues/86)).
- Eight of the eleven 3.x-only `tests/enduser/` contract modules -- including the
  whole-DagRun `dag_maker.run()` contract in `test_dag_run_result.py` -- now author
  dynamically through a shared `tests/enduser/_authoring.py` resolver and collect on the
  2.x family too, gating only the tests that touch a genuinely 3.x-only surface (the
  Task SDK's `run_task` runner) with `requires_airflow3` instead of the whole module;
  `test_assets.py`, `test_rest_api_compat.py`, and `test_structlog_events.py` stay
  `collect_ignore`'d, since Asset ORM persistence, the REST API server, and structlog
  capture have no 2.x equivalent
  ([#83](https://github.com/nredd/pytest-airflow-in-a-box/issues/83)).

### Fixed

- `_free_port` no longer hands two `pytest-xdist` workers the same loopback port for their
  isolated Airflow API servers: a subprocess that loses the bind race now retries with a
  freshly probed port (bounded to `API_SERVER_BIND_RETRIES` attempts) instead of leaving both
  workers pointed at one server
  ([#103](https://github.com/nredd/pytest-airflow-in-a-box/issues/103)).

## [0.4.0] - 2026-08-12

### Added

- `dag_maker.run()`, executing every task instance of one DagRun in dependency order
  (creating the DagRun when omitted, expanding mapped tasks mid-run, resuming deferrals with
  `run_triggerer=True`) and returning an inert `DagRunResult` snapshot with `states`, `xcoms`,
  `errors`, `order`, per-task `result["task_id"]` access, and an aligned per-task `repr`. Task
  failures are captured scheduler-shaped: downstreams settle `upstream_failed` and the DagRun
  state keeps Airflow's leaf-task semantics. The engine is public as
  `pytest_airflow_in_a_box.taskinstance.execute_dag_run`
  ([#90](https://github.com/nredd/pytest-airflow-in-a-box/issues/90)).
- `pytest_airflow_in_a_box.matchers` with `succeeded`, `failed`, `skipped`, `deferred`,
  `upstream_failed`, and `not_run` outcome matchers, `DagRunResult` equality against plain mappings for
  one-expression bulk assertions (`assert result == {"answer": succeeded(42)}`), and a
  `pytest_assertrepr_compare` hook rendering a per-task diff on mismatch
  ([#90](https://github.com/nredd/pytest-airflow-in-a-box/issues/90)).
- Ini option `airflow_pools`, seeding consumer-defined pools as `name = slots` lines before
  `test_pool_references_exist` runs, so a task's custom pool no longer needs private bootstrap
  code or deselecting the item. Seeding is idempotent, so the item stays safe under
  `pytest-xdist --dist each` and test reruns
  ([#70](https://github.com/nredd/pytest-airflow-in-a-box/issues/70)).
- A `docs/guide/cookbook.md` page with recipes for SQL operators against mocked connections,
  mocking custom hooks with `unittest.mock`, asserting rendered templates, `PYTEST_DAG_CASES`,
  deferrable operators, and asset outlet/consumer testing; four are adapted from a real test in
  `tests/enduser/` and two cross-reference existing guide pages
  ([#33](https://github.com/nredd/pytest-airflow-in-a-box/issues/33)).

### Fixed

- `airflow_dags_folder` ini values now resolve relative paths against `config.rootpath`
  instead of the process working directory, matching normal pytest configuration-file
  semantics; the `--dag-folder` CLI option remains relative to the invocation directory
  ([#71](https://github.com/nredd/pytest-airflow-in-a-box/issues/71)).
- `test_clear_db_triggers_clears_referencing_task_instances` and
  `test_clear_db_scoped_selection_leaves_other_groups` (`tests/test_db.py`) are now
  serial-only, matching their sibling whole-database-reset tests. Both call `clear_db`,
  an unscoped delete against the one metadata database every xdist worker shares; running
  either concurrently with another worker could delete a task instance a different worker
  had just persisted, surfacing as a flaky `ServerResponseError` /
  `RuntimeError: task failed to finish with a result` under `-n 4`
  ([#78](https://github.com/nredd/pytest-airflow-in-a-box/issues/78)).
- The bundled `--airflow-smoke` catalog no longer reparses the configured Dag folder from
  scratch when a `full_dag_bag` consumer already parsed it in the same worker process (the
  catalog is always collected last, so this is the common case). `full_dag_bag` caches its
  live `DagBag` on the session, and the smoke corpus builder reuses it when present instead
  of parsing again -- one full-corpus parse per process instead of one per consumer, with
  the catalog's configured `airflow_dag_parse_timeout` still applied either way. A `DagBag`
  shared this way should be treated as read-only, since mutating it is now visible to the
  smoke catalog's checks too
  ([#85](https://github.com/nredd/pytest-airflow-in-a-box/issues/85)).

## [0.3.0] - 2026-08-10

### Added

- Ini options `airflow_serialization_sample_size` and `airflow_serialization_sample_seed`,
  bounding the serialization-backed smoke checks to a deterministic hash-selected sample of the
  Dag corpus; the default (`0`) stays exhaustive
  ([#53](https://github.com/nredd/pytest-airflow-in-a-box/issues/53)).
- `test_dag_serialization_roundtrip` logs a slowest-first per-Dag serialization timing table,
  streams per-Dag progress at INFO, and carries a corpus-scaled `pytest-timeout` deadline so a
  pathological Dag is named before an outer CI timeout
  ([#53](https://github.com/nredd/pytest-airflow-in-a-box/issues/53)).

### Changed

- `test_dag_serialization_roundtrip`, `test_schedule_sanity`, and
  `test_dag_serialization_snapshot` share one run-scoped serialized-Dag cache instead of each check
  or worker serializing the whole corpus independently; `--airflow-smoke-update` now rejects a
  configured sampling ini rather than silently regenerating a snapshot subset
  ([#53](https://github.com/nredd/pytest-airflow-in-a-box/issues/53)).

### Fixed

- Bundled smoke items remain independently schedulable across `pytest-xdist` workers while sharing
  one worker-elected, serialized Dag corpus, avoiding a full Dag-folder parse in every participating
  process without turning the `smoke` marker into a scheduling constraint
  ([#55](https://github.com/nredd/pytest-airflow-in-a-box/issues/55)).
- `run_task_instance` resolves the task for a `dag_maker`-persisted Dag even when the task
  instance was queried through a separate consumer session (e.g. the `session` fixture),
  via a process-local registry of authoring Dags registered at persist time and removed at
  fixture cleanup. The terminal `TaskResolutionError` now also hints at passing
  `task=dag.get_task(...)` ([#56](https://github.com/nredd/pytest-airflow-in-a-box/issues/56)).
- Synthetic smoke items no longer bypass explicit pytest selection: file and node-ID
  positionals (`pytest tests/test_x.py::test_one`) drop the bundled catalog, while directory
  positionals, bare runs, and `testpaths`-driven runs keep it. `-k`/`-m`/`--deselect` continue
  to apply to smoke items as usual
  ([#54](https://github.com/nredd/pytest-airflow-in-a-box/issues/54)).
- The `api_test` marker now activates the isolated REST API server on its own, and every
  activated test -- marked or requesting `api_client`/`api_server_url` -- gets the selected URL
  published as `AIRFLOW__API__BASE_URL` for its duration, so application code can discover the
  endpoint through `conf.get("api", "base_url")`. The environment is restored exactly after each
  test via the new autouse `api_base_url` fixture
  ([#57](https://github.com/nredd/pytest-airflow-in-a-box/issues/57)).
- `tests/enduser/test_parallel_collection.py` passes `--dag-folder`/`--collect-dag-folder` in
  `--option=value` form instead of as separate argv entries, so a node-ID positional no longer
  lets pytest 8.0.0's rootdir determination treat the Dag folder path as an ambiguous
  positional and walk collection up to `/`
  ([#66](https://github.com/nredd/pytest-airflow-in-a-box/issues/66)).

## [0.2.0] - 2026-08-09

### Added

- Bundled catalog of zero-boilerplate smoke checks against the configured Dag folder,
  synthesized with no files written. Opt in with `--airflow-smoke` or the `airflow_smoke` ini
  option; every item carries the `smoke` marker so `-m smoke` / `-m "not smoke"` select exactly
  the bundled catalog. Items: `test_dag_bag_integrity` (fails on import errors and per-file
  parse timeouts via `airflow_dag_parse_timeout`, warns with `SlowDagParseWarning` on files
  above `airflow_dag_parse_slowpoke_ratio` of the timeout, and logs a slowest-first
  parse-timing table), `test_dag_serialization_roundtrip`, `test_no_duplicate_dag_ids`,
  `test_schedule_sanity`, and `test_pool_references_exist` (`db_test`), plus policy checks
  that appear only when their ini is configured: `airflow_dag_id_pattern`,
  `airflow_required_dag_tags`, and `airflow_forbid_default_owner`
  ([#10](https://github.com/nredd/pytest-airflow-in-a-box/issues/10)).
- `test_dag_serialization_snapshot`, a 9th bundled smoke item diffing each Dag's serialized
  structure (topology, schedule, params, task attrs) against a committed snapshot file, with
  run-dependent fields normalized away so diffs stay stable across machines and checkouts.
  Opt in with the new `airflow_dag_snapshot_dir` ini option; regenerate snapshots with
  `--airflow-smoke-update` ([#34](https://github.com/nredd/pytest-airflow-in-a-box/issues/34)).
- CLI option `--airflow-doctor`, which prints a one-shot, copy-pasteable diagnostics report --
  the storage ladder decision, resolved `AIRFLOW_HOME`/database URL scheme/backend tier, plugin/
  pytest/Python/Airflow versions plus the resolved capability table, and API server state -- then
  exits without collecting or running tests
  ([#37](https://github.com/nredd/pytest-airflow-in-a-box/issues/37)).
- Postgres metadata database backend, provisioned per session via testcontainers and shared by all
  `xdist` workers. Opt in with the new `postgres` extra plus a running Docker daemon; a missing
  extra or daemon fails loudly instead of silently skipping.
- CLI option `--airflow-db-backend` and ini option `airflow_db_backend` (`sqlite` or `postgres`)
  to select the metadata database backend.
- `postgres` marker for tests requiring a provisioned Postgres metadata database.
- `config` module with `airflow_config()`, one context manager -- usable as a decorator -- that
  overrides Airflow configuration options and plain environment variables through a single code
  path, restoring every name exactly on exit including names that were previously absent. A `None`
  value makes a name absent for the duration of the context. Opt-in `refresh_settings=True`
  recomputes the `airflow.settings` configuration globals.
- `config.env_var_name()` and `config.ENV_VAR_PREFIX`, which reproduce Airflow's
  `AIRFLOW__SECTION__KEY` name mangling without importing Airflow.
- `airflow_variables` and `airflow_connections` fixtures seeding committed Variable and Connection
  rows for database-backed tests, taking the same field shapes `run_task(variables=...)` and
  `run_task(connections=...)` take. Each row the fixture inserted is deleted on teardown, an
  existing row is never overwritten, and an identifier already shadowed by an
  `AIRFLOW_VAR_*`/`AIRFLOW_CONN_*` variable fails loudly rather than being silently outranked by
  Airflow's environment secrets backend
  ([#35](https://github.com/nredd/pytest-airflow-in-a-box/issues/35)).

- Consumer-style compatibility coverage for operators, TaskFlow mapping and branching, hooks,
  connections, provider SQL, sensors, deferral, callbacks, retries, assets, provider-shaped
  packages, Dag collection, and REST API CRUD.
- On-demand mapped task-instance expansion through `dag_maker.create_ti` and `run_ti`.
- Inline persisted-trigger execution and deferred-task resumption through
  `dag_maker.run_ti(..., run_triggerer=True)`.
- `taskinstance.run_trigger(trigger, *, timeout=...)`, which drives one trigger's async `run()` to
  its first `TriggerEvent` on a private event loop with no triggerer job, DagRun, or metadata
  database, always running `cleanup()`. A trigger that never fires raises the new
  `TriggerExecutionError` rather than hanging the suite
  ([#36](https://github.com/nredd/pytest-airflow-in-a-box/issues/36)).
- `trigger_timeout` on `run_ti` and `run_task_instance`, bounding the persisted trigger's first
  event when `run_triggerer=True`. Defaults to `taskinstance.DEFAULT_TRIGGER_TIMEOUT`.
- Synthetic attempt selection and retry callback behavior through `run_task(..., try_number=...)`.
- Widened the certified compatibility matrix to cover every non-yanked patch release
  across the 3.1.x and 3.2.x lines: `3.1.1`, `3.1.2`, `3.1.3`, `3.1.5`, `3.1.6`, `3.1.7`,
  and `3.2.1`. Each was verified to expose an identical private-API surface to its
  bracketing certified release before being added ([#15](https://github.com/nredd/pytest-airflow-in-a-box/issues/15)).
- Split the CI compat matrix into a reusable `.github/workflows/compat.yml` workflow so
  branch-protection rules can require one stable `Coverage` check regardless of how many
  Airflow/Python legs the matrix grows to.

### Deprecated

- `config.conf_vars()`, an alias provided under the name public Airflow documentation teaches. It
  emits a `DeprecationWarning`; use `config.airflow_config()` instead.

### Fixed

- `cap_structlog` now keeps capturing across mid-test `structlog.configure()` /
  `structlog.configure_once()` calls -- Airflow reconfigures structlog in several startup
  paths, which previously replaced the processor chain and silently dropped the capture.
  The fixture intercepts configuration while active and re-inserts its capture processor
  into the new chain; teardown restores the exact original callables, nested-safe
  ([#9](https://github.com/nredd/pytest-airflow-in-a-box/issues/9)).
- Pinned one Fernet key per run root as `AIRFLOW__CORE__FERNET_KEY`. Airflow's `unit_test_mode`
  generates a fresh random key in every process, so an encrypted connection password or Variable
  value written by the pytest process was undecryptable in the `api_client` server subprocess.

### Changed

- Widened the supported pytest range from 9.1+ to 8+, with the exact 8.0.0 floor exercised in CI.
  `pytest-timeout` remains a required dependency because it bounds the complete bundled Dag
  integrity smoke item in addition to Airflow's per-file parse timeout.
- Metadata database initialization is now lazy: it moved from session start to the first test
  that requires the database (a `db_test`/`api_test` marker or a database-backed plugin fixture),
  coordinated across `pytest-xdist` workers by an advisory lock plus ready sentinel in the run
  root so exactly one process migrates. Runs without Airflow-facing tests no longer import
  Airflow or create the database; disable the plugin with `-p no:pytest_airflow_in_a_box`
  ([#26](https://github.com/nredd/pytest-airflow-in-a-box/issues/26)).
- DB-free task context now includes a logical data interval and accepts active asset
  inlet/outlet validation.
- Airflow 2.x remains unsupported: it predates the Task SDK, DAG bundles/versions, and the
  `airflow.sdk` authoring package this plugin's compatibility layer depends on, and ships
  under a different distribution name (`apache-airflow` rather than `apache-airflow-core`).
  Supporting it would require a parallel, DB-backed `_compat` implementation rather than an
  incremental addition to the current one.

## [0.1.2] - 2026-08-07

### Added

- `pytest11` autoregistration via a single installable package -- no `conftest.py` wiring required.
- Isolated bootstrap: a disposable, per-run Airflow metadata database and `AIRFLOW_HOME`, with
  automatic network-filesystem detection so state never lands on NFS/SMB by accident.
- Fixtures: `session`, `dag_maker`, `full_dag_bag`, `run_task`, `cap_structlog`, `api_server_url`,
  `api_client`.
- Markers: `db_test`, `api_test`, `compat`, `need_serialized_dag`, `environment`.
- CLI options: `--airflow-home`, `--allow-network-airflow-home`, `--collect-dag-folder`.
- Ini options: `airflow_home`, `airflow_dags_folder`, `airflow_collect_dags_folder`,
  `airflow_environments`, `allow_network_airflow_home`.
- Opt-in Dag-file collection as import-check test items, deduplicated against pytest's default
  Python test discovery.
- Modules: `db`, `taskinstance`, `types`, `collection`, `logging`, `reporting`.
- `pytest-xdist` support, including worker-suffixed report artifacts and coordinated database
  setup/teardown across workers.
- Zero-ini defaults: sane `--tb`/`-ra`/`--durations`/`tmp_path` retention and warning-filter
  behavior out of the box, always overridable by explicit user configuration.
- Verified support matrix across supported Python and Apache Airflow versions (see README).

[Unreleased]: https://github.com/nredd/pytest-airflow-in-a-box/compare/v0.8.0...HEAD
[0.8.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.8.0
[0.7.2]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.7.2
[0.7.1]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.7.1
[0.7.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.7.0
[0.6.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.6.0
[0.5.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.5.0
[0.4.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.4.0
[0.3.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.3.0
[0.2.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.2.0
[0.1.2]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.1.2
