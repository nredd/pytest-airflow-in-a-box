# GitHub Actions and reports

Use `nredd/pytest-airflow-in-a-box/action@v0` to create a reproducible Airflow test
environment from Apache Airflow's published constraints. The action provisions the
environment; your workflow still runs pytest and uploads its reports.

<!-- readme-sync:start:action-example -->
```yaml
name: Airflow tests

on: [pull_request]

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: nredd/pytest-airflow-in-a-box/action@v0
        id: airflow-env
        with:
          airflow-version: "3.3.1"
          python-version: "3.13"
      - run: ${{ steps.airflow-env.outputs.python-path }} -m pytest
```
<!-- readme-sync:end:action-example -->

Always invoke pytest through `python-path`; the action does not add its virtual environment
to `PATH`. It also does not run tests, cache packages, upload artifacts, or start Docker.

The action targets POSIX runners. Use GitHub-hosted Linux or macOS, or a self-hosted runner
with Bash, `curl`, and network access to GitHub, PyPI, and `astral.sh`. Native Windows is not
supported.

## Across a version matrix

Put each exact Airflow and Python pair in the matrix and pass both values to the action:

```yaml
jobs:
  test:
    strategy:
      fail-fast: false
      matrix:
        include:
          - airflow-version: "3.2.2"
            python-version: "3.12"
          - airflow-version: "3.3.1"
            python-version: "3.13"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: nredd/pytest-airflow-in-a-box/action@v0
        id: airflow-env
        with:
          airflow-version: ${{ matrix.airflow-version }}
          python-version: ${{ matrix.python-version }}
      - run: ${{ steps.airflow-env.outputs.python-path }} -m pytest
```

Choose pairs from
[Compatibility and certification](../../internals/compat-layer.md#what-ci-actually-exercises).
The action validates input syntax, not whether Airflow publishes constraints for the pair. An
unsupported pair therefore fails while downloading constraints or resolving dependencies.

## Inputs

| Input | Required | Default | What it does |
| --- | --- | --- | --- |
| `airflow-version` | yes | -- | Installs an exact `X.Y.Z` release and selects its `constraints-<version>` branch. |
| `python-version` | yes | -- | Provisions an `X.Y` interpreter and selects `constraints-<python-version>.txt`. |
| `extra` | no | `airflow3` | Selects the plugin's `airflow3` or `airflow2` extra. Other values fail validation. |
| `plugin-version` | no | latest on PyPI | Pins `pytest-airflow-in-a-box` to an exact version. |
| `uv-version` | no | `0.12.2` | Selects the `uv` release installed into `RUNNER_TEMP`. |
| `working-directory` | no | `.` | Directory containing the virtual environment and resolving `requirements-file`. It must already exist. |
| `requirements-file` | no | (none) | Installs additional operators, providers, and test dependencies after the constrained environment. The path is relative to `working-directory`. |
| `report-dir` | no | (none) | Creates a report directory and configures pytest to write `pytest.log` and `pytest.xml`. Relative paths resolve from `working-directory`. |

`airflow-version`, `python-version`, and `extra` are checked before installation. Invalid
syntax produces a GitHub `::error::` annotation immediately.

## Outputs

| Output | What it holds |
| --- | --- |
| `python-path` | Absolute path to the environment's Python interpreter. Use it for pytest. |
| `venv-path` | Absolute path to the virtual environment. Use it to reach installed console scripts. |
| `report-dir` | Absolute report-directory path, or an empty string when `report-dir` was not set. |

## What gets installed

The action creates `<working-directory>/.venv-airflow-in-a-box` and downloads the constraints
file for the requested Airflow and Python versions.

For `extra: airflow3`, one constrained transaction installs the plugin, the exact
`apache-airflow-core` release, and `apache-airflow-providers-sqlite>=4.1,<5`. Installing core
instead of the `apache-airflow` meta-package avoids the default provider set while retaining
the SQLite provider required by the default metadata backend.

For `extra: airflow2`, Airflow is installed under its constraints first. The plugin and
`pytest>=8,<9` are then installed without those constraints because Airflow 2 constraints can
pin pytest below the plugin's pytest 8 floor.

The optional `requirements-file` is installed last and unconstrained. Use it for your Dag
repository's providers and test tools, but pin it deliberately: its requirements can replace
versions chosen by the Airflow constraints.

The action's `extra` input accepts only an Airflow family. To run with xdist or the disposable
Postgres backend, install their dependencies through `requirements-file`, then invoke pytest
with `-n auto --dist loadgroup` or `--airflow-db-backend=postgres`. Postgres also requires an
available Docker daemon. See
[Dependencies and extras](../../reference/dependencies.md).

## Report artifacts

Set `report-dir` to append `--airflow-report-dir=<absolute path>` to `PYTEST_ADDOPTS`. Upload
that directory in a separate `always()` step:

```yaml
      - uses: nredd/pytest-airflow-in-a-box/action@v0
        id: airflow-env
        with:
          airflow-version: "3.3.1"
          python-version: "3.13"
          report-dir: ${{ github.workspace }}/reports
      - run: ${{ steps.airflow-env.outputs.python-path }} -m pytest --airflow-smoke
      - if: always()
        uses: actions/upload-artifact@v7
        with:
          name: airflow-test-reports
          path: ${{ github.workspace }}/reports
          if-no-files-found: warn
```

Use the same literal report path for upload. If provisioning fails, the action never sets its
`report-dir` output; passing that empty output to `upload-artifact` can obscure the original
failure.

The plugin writes `pytest.log` at `DEBUG` and `pytest.xml` in xunit2 format. Explicit
`--log-file`, log-level, or `--junit-xml` settings take precedence. Under xdist, each worker
writes a suffixed log such as `pytest.gw0.log`, while the controller writes JUnit XML.
`airflow_isolated` children suffix both artifacts so they cannot overwrite the parent files.
When `COVERAGE_FILE` is set, isolated children also receive suffixed coverage filenames unless
`pytest-cov` is loaded and already owns them.

An older `plugin-version` may not support `--airflow-report-dir`. The action probes
`pytest --help`; if the option is absent or the probe fails, it emits a warning and leaves
`PYTEST_ADDOPTS` unchanged.

!!! note "DEBUG logging can change `caplog`"

    Pytest implements `--log-file-level` through the session's root logger. Tests that assert
    exact `caplog` counts may capture more records with report generation enabled. Set an
    explicit `--log-file-level` or `--log-level` when the threshold is part of the test.

## Migration runs

Use `venv-path` for the installed `airflow-migration-diff` script. Migration recording and
comparison flags run through `python-path` like any other pytest invocation. See
[Running both families in CI](../migration.md#running-both-families-in-ci).

## Pinning

Use `@v0` to follow the newest stable 0.x action release without crossing a major version.
Use a full tag such as `@v0.12.0` for an immutable release pin. Moving major tags advance only
after a stable release is published; prereleases do not move them.

The action's inputs and outputs are a published interface and follow the plugin's
major-version compatibility promise.
