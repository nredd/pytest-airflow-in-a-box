# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Consumer-style compatibility coverage for operators, TaskFlow mapping and branching, hooks,
  connections, provider SQL, sensors, deferral, callbacks, retries, assets, provider-shaped
  packages, Dag collection, and REST API CRUD.
- On-demand mapped task-instance expansion through `dag_maker.create_ti` and `run_ti`.
- Inline persisted-trigger execution and deferred-task resumption through
  `dag_maker.run_ti(..., run_triggerer=True)`.
- Synthetic attempt selection and retry callback behavior through `run_task(..., try_number=...)`.
- Widened the certified compatibility matrix to cover every non-yanked patch release
  across the 3.1.x and 3.2.x lines: `3.1.1`, `3.1.2`, `3.1.3`, `3.1.5`, `3.1.6`, `3.1.7`,
  and `3.2.1`. Each was verified to expose an identical private-API surface to its
  bracketing certified release before being added ([#15](https://github.com/nredd/pytest-airflow-in-a-box/issues/15)).
- Split the CI compat matrix into a reusable `.github/workflows/compat.yml` workflow so
  branch-protection rules can require one stable `Coverage` check regardless of how many
  Airflow/Python legs the matrix grows to.

### Changed

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
