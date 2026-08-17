# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A DB-free `render_task` fixture (and matching `_compat.render_task_in_process`) rendering
  one operator's `template_fields` through the Task SDK's public `render_template_fields`,
  plus a `rendered(...)` matcher for one-expression assertions -- the documented replacement
  for `TaskInstance.render_templates()`, which Airflow 3.2+ removed
  ([#118](https://github.com/nredd/pytest-airflow-in-a-box/issues/118)).

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

[Unreleased]: https://github.com/nredd/pytest-airflow-in-a-box/compare/v0.7.2...HEAD
[0.7.2]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.7.2
[0.7.1]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.7.1
[0.7.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.7.0
[0.6.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.6.0
[0.5.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.5.0
[0.4.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.4.0
[0.3.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.3.0
[0.2.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.2.0
[0.1.2]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.1.2
