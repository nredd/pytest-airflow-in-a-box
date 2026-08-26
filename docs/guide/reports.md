# Logs and JUnit XML you can trust

Two of your CI artifacts are wrong before you read them, and neither failure announces
itself. The plugin fixes both on every run, with no flag.

## The log file every xdist worker opens

pytest's own `_pytest/logging.py` has no worker guard. Under `pytest -n auto`, every worker
opens the same `--log-file` destination and the writers race. You get one file, silently
interleaved, with records from different tests spliced together and an unknowable amount
missing. The run is green, the artifact is garbage, and the only time you read it is when you
are already debugging something else.

The plugin scopes `log_file` per worker instead: `reports/pytest.gw0.log`,
`reports/pytest.gw1.log`, and so on. Controller and serial runs are untouched.

`-n auto` is the default CI shape for the repo this plugin is for. If your workflow runs
xdist and archives a log file, you were hitting this.

JUnit XML needs no equivalent handling: pytest's `junitxml` plugin already skips writing on
xdist workers, so only the controller writes it.

## The isolated child that overwrites the parent's XML

`@pytest.mark.airflow_isolated` runs marked tests in a child pytest process. That child is
*not* an xdist worker, so pytest's controller-only guard does not apply to it -- it inherits
the parent's configuration, writes `pytest.xml`, and clobbers the parent's file with just its
own batch. Your JUnit report ends up holding three tests instead of four hundred.

The child carries `PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER` in its environment, and the
plugin uses that identity to scope its XML destination exactly like a worker id:
`pytest.<identity>.xml`. It scopes the child's `log_file` the same way.

What the marker is for: [Entry points and packaging](isolated-tests.md).

## Coverage data files, but only when nothing else owns them

The same identity scopes `COVERAGE_FILE` when it is set in the environment -- that's the
case where an *externally* orchestrated coverage run would otherwise collide: one data
file, several writers.

It is scoped only when `pytest-cov` is not loaded. `pytest-cov` does its own per-worker
data-file handling, and a second layer of renaming on top of it breaks the combine step. Run
`pytest --cov` and the plugin keeps its hands off.

## `--airflow-report-dir`, the convenience on top

Since all three collisions are handled anyway, the flag's job is narrower than it looks: it
saves you wiring three unrelated pytest options, and it puts the derived destinations
somewhere the scoping above can act on them.

```console
pytest --airflow-report-dir=reports
```

That creates the directory (parents included) and derives two destinations inside it:

- `reports/pytest.log` -- pytest's own `--log-file`, at `--log-file-level=DEBUG`
- `reports/pytest.xml` -- pytest's own `--junit-xml`, in the stock `xunit2` family

Or persistently, via the `airflow_report_dir` ini option:

```ini
[pytest]
airflow_report_dir = reports
```

The plugin writes no report files unless one of those is set. Report artifacts deliberately
sit outside the [zero-ini defaults](configuration.md), which are display-only and never put
files in your repository.

Every derived destination yields to an explicit one, so `--log-file`, `--log-file-level`,
`--log-level`, and `--junit-xml` (and their ini forms) always win. Mixing is fine:
`--airflow-report-dir=reports --log-file-level=INFO` still writes both artifacts, at `INFO`.

Deriving runs before scoping, which is what lets a derived log file get worker-suffixed
exactly like a hand-written one.

!!! note "The derived `DEBUG` level is not write-only"

    `--log-file-level` is how pytest decides what reaches the log file, and pytest implements
    it by lowering the *root* logger to that level for the session. So a run with
    `--airflow-report-dir` and no level of its own captures more in `caplog` than the same run
    without it -- a test asserting an exact `len(caplog.records)` can start failing on the flag
    alone. Set `--log-file-level` or `--log-level` (pytest's own fallback for the former, and
    honored here for exactly that reason) to keep the session's level where it was.

## In CI

Archive on both outcomes. A report from a *green* run is nice, a report from a red one is the
point:

```yaml
- run: pytest --airflow-smoke --airflow-report-dir=${{ github.workspace }}/reports
- if: always()
  uses: actions/upload-artifact@v7
  with:
    name: reports
    path: ${{ github.workspace }}/reports
```

The composite action has a `report-dir` input that does the flag half for you and hands the
absolute path back as an output. See [The GitHub Action](ci/github-action.md).
