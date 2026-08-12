# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `dag_maker.run()`, executing every task instance of one DagRun in dependency order
  (creating the DagRun when omitted, expanding mapped tasks mid-run, resuming deferrals with
  `run_triggerer=True`) and returning an inert `DagRunResult` snapshot with `states`, `xcoms`,
  `errors`, `order`, per-task `result["task_id"]` access, and an aligned per-task `repr`. Task
  failures are captured scheduler-shaped: downstreams settle `upstream_failed` and
  `result.success` reports `False`. The engine is public as
  `pytest_airflow_in_a_box.taskinstance.execute_dag_run`
  ([#90](https://github.com/nredd/pytest-airflow-in-a-box/issues/90)).
- `pytest_airflow_in_a_box.matchers` with `succeeded`, `failed`, `skipped`, `deferred`, and
  `upstream_failed` outcome matchers, `DagRunResult` equality against plain mappings for
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

[Unreleased]: https://github.com/nredd/pytest-airflow-in-a-box/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.3.0
[0.2.0]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.2.0
[0.1.2]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.1.2
