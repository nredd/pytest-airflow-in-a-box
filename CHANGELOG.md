# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

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

[Unreleased]: https://github.com/nredd/pytest-airflow-in-a-box/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/nredd/pytest-airflow-in-a-box/releases/tag/v0.1.2
