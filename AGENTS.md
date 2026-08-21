# Agent guide

Guidance for coding agents (and humans in a hurry) working in this repo. `CLAUDE.md` is a
symlink here -- edit this file only.

## What this is

A pytest plugin for testing Apache Airflow 3 DAGs without a live deployment. It registers
via the `pytest11` entry point, bootstraps an isolated `AIRFLOW_HOME` + disposable metadata
DB (SQLite WAL by default, testcontainers Postgres via `--airflow-db-backend=postgres`),
and exposes typed fixtures for Dags, DagRuns, task instances, sessions, and a live REST
API. Airflow import and DB migration are lazy -- the plugin is inert for non-Airflow runs.

Supports CPython 3.10-3.14 and Airflow 3.1-3.3. Windows is unsupported (Airflow has no
native Windows support) -- use WSL2 or the devcontainer.

## Commands

- `make install` -- `uv sync` + `uv run prek install`. Run this first, always. Hooks
  (pre-commit + pre-push) gate commits and manual runs do not
- `make format` / `make lint` / `make type` -- ruff format, ruff check, ty check
- `make test` -- the real coverage gate. Installs a coverage `.pth` so pytester
  subprocess runs are measured; plain `uv run pytest` is fine for iteration but
  undercounts coverage and will miss the 100% gate
- `make all` -- format, lint, type, test, lock, build. The pre-PR gate, together with
  `uv run prek run --all-files`
- `make release` -- tag + push only. Publishing to PyPI happens when the GitHub release
  is published (trusted publishing, `release.yml`)

Everything goes through `uv`. There is no pytest ini section -- `defaults.py` supplies
zero-ini defaults on purpose.

## Hard rules

- NO inline waivers, ever. `noqa`, `type: ignore`, `ty: ignore`, `pyright: ignore`,
  `pylint: disable` are all rejected by `scripts/check_inline_waivers.py` (a prek hook),
  and ty runs with `respect-type-ignore-comments = false` so they would not work anyway.
  Fix the source
- 100% branch coverage (`fail_under = 100`), enforced locally by `make test` and in CI as
  the union across all matrix legs. Platform-specific branches are covered with fake
  probes (see existing tests), not extra CI legs
- `src/pytest_airflow_in_a_box/plugin.py` stays import-light and must never import
  Airflow at module scope. Airflow import + DB init are deferred until a test needs them
- Any use of Airflow *internals* goes behind `src/pytest_airflow_in_a_box/_compat/` with
  a capability probe. `tests/enduser/` (marked `compat`) is the consumer contract run
  across the whole matrix
- Dev tools are exact-pinned and ruff enforces `required-version == 0.16.1`. Do not bump
  tools ad hoc -- Dependabot does that weekly. `uv.lock` is committed and `uv lock
  --check` runs in CI and as a hook
- The version lives in BOTH `pyproject.toml` and `__init__.py` `__version__`, and
  `make release` / `release.yml` hard-fail on mismatch. Bump both, plus `CHANGELOG.md`
  (Keep a Changelog + SemVer, link issue/PR numbers)
- Upstream-derived code must be recorded in `PROVENANCE.md` (currently only
  `_compat/taskrun.py::run_task_instance`, adapted from Apache Airflow). Never add
  proprietary source, credentials, hostnames, or internal paths

## Layout

`src/pytest_airflow_in_a_box/`:

- `plugin.py` -- import-light pytest entry point: options, markers, xdist validation
- `bootstrap.py` / `airflow_cfg.py` -- isolated `AIRFLOW_HOME` + deterministic test cfg
- `config.py` -- `airflow_config()` context manager/decorator for option + env overrides
- `db.py` -- registry-driven metadata DB cleanup (`clear_db`, `TableGroup`)
- `components.py` -- registry-driven static conformance checks for custom timetables,
  listeners, and executors (`check_component`, `ComponentKind`)
- `defaults.py` -- zero-ini pytest defaults and narrowed warning filters
- `ini_config.py` -- the `airflow_config` ini option: grammar, bootstrap-owned denylist,
  and pre-conftest application
- `collection.py` / `smoke.py` / `doctor.py` -- Dag import collection, `--airflow-smoke`
  checks, `--airflow-doctor` diagnostics
- `taskinstance.py` -- `run_trigger`, `ordered_task_instances`
- `types.py` -- public typing contracts for fixtures
- `fixtures/` -- `dag_maker`, `full_dag_bag`, sessions, DB-free
  `run_task`/`render_task`/`task_context`, REST API server + client, `airflow_variables`/`airflow_connections`, `cap_structlog`, `airflow_configure`, `airflow_home_path`/`airflow_dags_folder_path`
- `storage/` -- storage-ladder selection, SQLite tuning, Postgres provisioning
- `_compat/` -- private Airflow-version shims, each guarded by `capabilities.py` probes

`tests/` mirrors `src/` (`tests/test_<module>.py` plus `bootstrap/`, `compat/`,
`fixtures/`, `storage/`, `enduser/`). `tests/dags/` is a Dag corpus -- data, not test
modules (`broken.py` is intentionally broken). Many tests drive the plugin through
pytester `runpytest_subprocess`, which is why the coverage `.pth` exists. Markers:
`db_test`, `api_test`, `postgres`, `compat`, `smoke`, `need_serialized_dag`,
`environment`.

## Style

- Docstrings use `Parameters:` / `Returns:` / `Raises:` / `References:` sections with
  repeated types, on tests and private helpers too. Enforced by review, not ruff
- PEP 604 unions, `from __future__ import annotations`, annotate every return
- Module-level `LOGGER`, f-strings in log calls (house style -- do not "fix" to `%s`)

## CI

`ci.yml`: check job (lint, type, hooks, build), 25-leg compat matrix (`compat.yml`,
Airflow 3.1.0-3.3.0 x Python 3.10-3.14 via Airflow constraints files, plus pytest-floor,
xdist, macOS, arm, musl legs), a real-Docker postgres job, and a coverage-combine job
that enforces the 100% union. `airflow-canary.yml` runs weekly against the newest
Airflow and files an issue on failure. `act pull_request` can run the Linux workflow
locally.

## Agent skills

### Issue tracker

Issues live in GitHub Issues (`nredd/pytest-airflow-in-a-box`), via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default label vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`CONTEXT.md` + `docs/adr/` at repo root, created lazily by `/domain-modeling`). See `docs/agents/domain.md`.
