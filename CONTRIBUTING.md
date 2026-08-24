# Contributing

## Setup

Use Python 3.10 or newer and install the locked development environment:

```console
git submodule update --init
uv sync
uv run prek install
```

`uv run prek install` registers the pre-commit and pre-push hooks that gate every commit.
Running checks manually (`make lint`, etc.) does not replace them -- install the hooks
first, always. For the Postgres backend extra, see the "Development" section of
`README.md`.

The dev environment carries Airflow 3.x through the `dev` dependency group. To experiment
against an Airflow 2.x resolution instead (the `airflow2` extra conflicts with the default
`dev` group by design):

```console
uv sync --no-default-groups --extra airflow2
```

## Before opening a pull request

```console
make all
uv run prek run --all-files
```

`make all` chains `format`, `lint`, `type`, `test`, `lock`, and `build` -- it is the same
pipeline CI runs. `make help` lists every target with a one-line description.

Changes require tests, complete type annotations, and documentation for public behavior.
Fix type and lint failures at the source; inline suppression directives (`noqa`, `type:
ignore`, `ty: ignore`, `pylint: disable`) are rejected by a prek hook and by `ty`
configuration, so they would not work anyway. Keep the registered `plugin.py` module
import-light and never import Airflow there at module scope.

## Reproducing CI locally

Run the GitHub Actions workflow locally on Linux with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce native macOS or Windows behavior, so the `macos`/`arm`/`musl` legs
of the compat matrix (`.github/workflows/compat.yml`) still need a real PR to exercise.

## The coverage gate

`make test` enforces 100% branch coverage (`fail_under = 100` in
`[tool.coverage.report]`), checked locally per-run and in CI as the union across the
whole 24-leg compat matrix. Two things make that number honest instead of a lie:

- **Subprocess measurement.** Many tests drive the plugin through pytester's
  `runpytest_subprocess`, which spawns a real child interpreter. `make test` installs a
  `.pth` file (`scripts/install_coverage_pth.py`) that calls `coverage.process_startup()`
  in every Python process started from that environment; combined with
  `COVERAGE_PROCESS_START` and `parallel = true`, each subprocess writes its own suffixed
  data file, and `coverage combine` merges them before the report runs. Running plain `uv
  run pytest` skips this and will undercount coverage -- use `make test` (or `uv run
  coverage run -m pytest` with the same env vars) when you need a real number.
- **Probe doubles for platform-only branches.** Code that branches on OS (e.g.
  `storage/locate.py`'s `ctypes.CDLL`/`ctypes.windll` calls) is covered by faking the
  platform object, not by adding an OS-specific CI leg. See
  `tests/storage/test_locate.py` for the pattern: `_FakeLinuxLibc`/`_FakeDarwinLibc`
  monkeypatch `ctypes.CDLL` to hit the Linux/Darwin `statfs` branches, and
  `test_windows_drive_probe_with_fake_windll` forces `os.name = "nt"` and monkeypatches
  `ctypes.windll` to hit the Windows branch -- all reachable and asserted on whatever OS
  CI happens to run on. Reuse this pattern rather than skipping coverage on
  platform-specific code.

## Adding a compat-suite test

`tests/enduser/` is the consumer contract: every test there carries module-level
`pytestmark = pytest.mark.compat` and runs across the full compat matrix (Airflow
3.1.0-3.3.0 x Python 3.10-3.14, plus the pytest-floor/macOS/arm/musl legs -- see
`.github/workflows/compat.yml`). Those legs run under `-n auto --dist loadgroup`, so a
new test must be xdist-safe: give it a `dag_id` no other test uses, and do not reset the
whole database. `make test` is serial, so it will not catch a parallel-only break --
reproduce CI's configuration with `make test-xdist` before pushing. That target skips the
coverage gate on purpose: the `SERIAL_ONLY` tests skip themselves on a worker, so a
parallel run legitimately reports under 100%. To add one:

- Build a real DAG with `airflow.sdk.DAG` and real operators/sensors, and drive it
  through the plugin's own public fixtures (`run_task`, `dag_maker`, etc. -- see
  `tests/enduser/test_sensors.py` for a worked example), not through Airflow internals
  directly.
- Mark `pytestmark = pytest.mark.compat` at module level; opt individual tests into DB
  access with `@pytest.mark.db_test` only where the assertion actually needs it.
- If the test needs an Airflow internal not already exposed, add it behind
  `src/pytest_airflow_in_a_box/_compat/` with a capability probe in
  `_compat/capabilities.py` first -- `resolve_capabilities()` checks the installed
  Airflow's real signatures/fields against the certified contract for that release and
  raises `AirflowCompatibilityError` on drift, which is what lets the plugin support
  three minor Airflow releases without silently breaking on a fourth.

## Good first issues

Issues labeled [`good first
issue`](https://github.com/nredd/pytest-airflow-in-a-box/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
are scoped for a first contribution.

Report security issues through the process in `SECURITY.md`, not a public issue.
