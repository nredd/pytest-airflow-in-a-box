# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `config` module with `airflow_config()`, one context manager -- usable as a decorator -- that
  overrides Airflow configuration options and plain environment variables through a single code
  path, restoring every name exactly on exit including names that were previously absent. A `None`
  value makes a name absent for the duration of the context. Opt-in `refresh_settings=True`
  recomputes the `airflow.settings` configuration globals.
- `config.env_var_name()` and `config.ENV_VAR_PREFIX`, which reproduce Airflow's
  `AIRFLOW__SECTION__KEY` name mangling without importing Airflow.

### Deprecated

- `config.conf_vars()`, an alias provided under the name public Airflow documentation teaches. It
  emits a `DeprecationWarning`; use `config.airflow_config()` instead.

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
