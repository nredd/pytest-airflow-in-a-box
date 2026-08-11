# Dag-file collection

Point the collector at a directory of real Dag files and every `*.py` file below it is collected
as a `dag-import` test item that fails on import errors or a Dag-free file. Off unless configured:

```console
pytest --collect-dag-folder=dags/
```

or persistently via the `airflow_collect_dags_folder` ini option. Collected items are auto-marked
`db_test`; files also matching `test_*.py` naming are deduplicated against pytest's default Python
collector.

A Dag file may pin param cases through a module-level literal, read without importing the file:

```python
PYTEST_DAG_CASES = {
    "dev": {"environment": "dev"},
    "prod": {"environment": "prod"},
}
```

Each case collects as a sibling `dag-params[...]` item that validates the pinned values against
every Dag the file declares -- undeclared keys and schema violations fail the case.
