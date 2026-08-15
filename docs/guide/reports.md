# Report artifacts

One opt-in flag turns a run into archivable evidence:

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

The plugin writes no report files unless one of those is set -- this deliberately sits outside
the [zero-ini defaults](../reference/defaults.md), which are display-only and never put files
in your repository.

Every derived destination yields to an explicit one, so `--log-file`, `--log-file-level`, and
`--junit-xml` (and their ini forms) always win. Mixing is fine: `--airflow-report-dir=reports
--log-file-level=INFO` still writes both artifacts, at `INFO`.

Under `pytest-xdist` the log file is scoped per worker (`reports/pytest.gw0.log`,
`reports/pytest.gw1.log`, ...) because pytest's logging plugin has no worker guard and the
writers would otherwise race. The JUnit XML needs no such handling: pytest already writes it
once, from the controller.

## In CI

Point the flag at a directory and archive it on both outcomes -- a report from a *green* run is
nice, a report from a red one is the point:

```yaml
- run: pytest --airflow-smoke --airflow-report-dir=${{ github.workspace }}/reports
- if: always()
  uses: actions/upload-artifact@v7
  with:
    name: reports
    path: ${{ github.workspace }}/reports
```

The [composite action](https://github.com/nredd/pytest-airflow-in-a-box/tree/main/action) has a
`report-dir` input that does the flag half for you: it creates the directory and appends
`--airflow-report-dir` to `PYTEST_ADDOPTS`, so your plain `pytest` invocation picks it up with
no changes, and exposes the absolute path as the `report-dir` output for the upload step:

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

The action provisions and stops -- it never invokes pytest -- so the upload step stays yours. A
`plugin-version` older than the release that introduced `--airflow-report-dir` is detected and
skipped with a warning annotation rather than poisoning `PYTEST_ADDOPTS` with a flag that
installation cannot parse.
