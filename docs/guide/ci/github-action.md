# The GitHub Action

Your CI job needs an Airflow that actually resolves. Installing `apache-airflow==3.3.0` with
plain `pip` picks whatever transitive versions today's index happens to offer, so a job that
passed last week fails this week on a dependency you never named.

`nredd/pytest-airflow-in-a-box/action@v0` is a composite action that provisions the same
constraints-pinned `uv` environment this repo's own compat matrix uses, then stops. It never
invokes `pytest` -- you always write the invocation.

```yaml
- uses: actions/checkout@v5
- uses: nredd/pytest-airflow-in-a-box/action@v0
  id: airflow-env
  with:
    airflow-version: "3.3.0"
    python-version: "3.12"
- run: ${{ steps.airflow-env.outputs.python-path }} -m pytest
```

The action does not put its venv on `PATH`. Drive it through the `python-path` output, as
above.

## Across a version matrix

One step with scalar inputs, so it drops straight into `strategy.matrix`:

```yaml
jobs:
  test:
    strategy:
      fail-fast: false
      matrix:
        airflow-version: ["3.2.2", "3.3.0", "3.3.1"]
        python-version: ["3.11", "3.12", "3.13"]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: nredd/pytest-airflow-in-a-box/action@v0
        id: airflow-env
        with:
          airflow-version: ${{ matrix.airflow-version }}
          python-version: ${{ matrix.python-version }}
      - run: ${{ steps.airflow-env.outputs.python-path }} -m pytest
```

Which pairs are legal is on [Certification](../../internals/certification.md#what-ci-actually-exercises).
The action does not validate the pair against the support matrix -- it only rejects
malformed inputs.

## Inputs

| Input | Required | Default | What it does |
| --- | --- | --- | --- |
| `airflow-version` | yes | -- | Exact Airflow release to install. Must be `X.Y.Z`, e.g. `3.3.0`. Also selects the `constraints-<version>` branch pulled from `apache/airflow`. |
| `python-version` | yes | -- | Python to provision. Must be `X.Y`, e.g. `3.12`. Also selects `constraints-<python-version>.txt`. |
| `extra` | no | `airflow3` | Which plugin extra to install: `airflow3` or `airflow2`. Anything else fails the step. |
| `plugin-version` | no | latest on PyPI | Exact `pytest-airflow-in-a-box` version, pinned as `==<version>`. |
| `uv-version` | no | `0.12.2` | `uv` release installed from `astral.sh` into `RUNNER_TEMP`. |
| `working-directory` | no | `.` | Where the venv is created and `requirements-file` is resolved from. |
| `requirements-file` | no | (none) | Path, relative to `working-directory`, to a requirements file installed *after* the primary install -- your own operators, hooks, and providers. Not constrained. |
| `report-dir` | no | (none) | Directory for `pytest.log` and `pytest.xml`. Relative to `working-directory`, or absolute. See [report artifacts](#report-artifacts). |

`airflow-version`, `python-version`, and `extra` are validated in the first step, before
anything is installed, so a typo fails in seconds with a `::error::` annotation rather than
after a five-minute resolve.

## Outputs

| Output | What it holds |
| --- | --- |
| `python-path` | Absolute path to the provisioned venv's `python`. This is how you invoke `pytest`. |
| `venv-path` | Absolute path to the venv directory, for console scripts: `${{ steps.airflow-env.outputs.venv-path }}/bin/airflow-migration-diff`. |
| `report-dir` | Absolute path to the report directory. Empty string when the `report-dir` input was not set. |

## What gets installed

The venv is `.venv-airflow-in-a-box`, inside `working-directory`. Constraints come from
`https://github.com/apache/airflow/raw/constraints-<airflow-version>/constraints-<python-version>.txt`.

On `extra: airflow3`, one constrained install of three specs together:

- `pytest-airflow-in-a-box[airflow3]`
- `apache-airflow-core==<airflow-version>`
- `apache-airflow-providers-sqlite>=4.1,<5`

`apache-airflow-core`, *not* `apache-airflow`: the meta-package drags in the full default
provider set, which a Dag-testing job does not need. The sqlite provider is then named
explicitly because the default metadata DB backend needs it and the core package does not
pull it.

On `extra: airflow2`, two passes instead of one, and the reason is not obvious:

1. `apache-airflow==<airflow-version>` under constraints
2. `pytest-airflow-in-a-box[airflow2]` and `pytest>=8,<9`, *unconstrained*

Airflow 2.x constraints pin `pytest` as low as `7.4.4`, below this plugin's `pytest>=8`
floor. A single constrained pass is unsatisfiable. The second pass therefore drops the
constraint file and gives `pytest` an explicit ceiling instead. This mirrors what
`compat.yml` does for the repo's own 2.x legs.

`requirements-file`, when set, installs last and unconstrained, so it can override anything
the constrained pass resolved.

## Report artifacts

Setting `report-dir` creates the directory and appends
`--airflow-report-dir='<abs path>'` to `PYTEST_ADDOPTS`, so a plain `pytest` invocation picks
it up with no change to your command. The absolute path comes back as the `report-dir`
output:

```yaml
- uses: nredd/pytest-airflow-in-a-box/action@v0
  id: airflow-env
  with:
    airflow-version: "3.3.0"
    python-version: "3.12"
    report-dir: reports
- run: ${{ steps.airflow-env.outputs.python-path }} -m pytest --airflow-smoke
- if: always()
  uses: actions/upload-artifact@v7
  with:
    name: reports
    path: ${{ steps.airflow-env.outputs.report-dir }}
```

The upload step stays yours, because the action never runs pytest.

Two behaviors worth knowing:

- A `plugin-version` older than the release that added `--airflow-report-dir` is detected. The
  action probes `pytest --help` and, on a miss, emits a `::warning::` and leaves
  `PYTEST_ADDOPTS` alone rather than poisoning every run with an unparseable flag. The same
  warning fires if the probe itself cannot run
- On a provisioning failure the `report-dir` output is empty, and `upload-artifact` fails on
  an empty `path`, burying the real error. Use a literal path in the upload step if you want
  the original failure to stay legible

What lands in those files, and the collisions the plugin fixes on your behalf, are on
[Logs and JUnit XML you can trust](../reports.md).

## Migration runs

The `venv-path` output is what makes the Airflow 2 to 3 tooling reachable -- the
`airflow-migration-diff` console script lives in the venv's `bin/`, and `pytest --airflow-record`
/ `--airflow-baseline` run through `python-path` like any other invocation. Wiring for both
families in one workflow is on
[Running both families in CI](../migration/orchestrator-in-ci.md).

## Pinning

`@v0` is the pin to use. `release.yml` moves a `v<major>` tag to the newest published stable
release on that major line after each release (`v0` while pre-1.0, `v1` once `1.0.0` ships),
and refuses to move it for a prerelease or when the tag being released is not the newest
stable tag on that major. So `@v0` tracks the latest 0.x and never crosses a major bump.

Pin a full release tag -- `@v0.11.1` -- when you want an exact, non-moving reference.

The action is a *published interface*: its inputs and outputs follow the same major-version
promise as the plugin's Python API.
