# Provenance

`pytest-airflow-in-a-box` is an independent project informed by public Apache Airflow source,
documentation, issue discussions, and mailing-list discussions.

`src/pytest_airflow_in_a_box/_compat/taskrun.py::run_task_instance` is adapted from Apache Airflow
`devel-common/src/tests_common/test_utils/taskinstance.py` at commit
`2d374f71bc81202204ac0208df07b07c280668fa`, introduced by merge
`960973bfd8341040150ac312302cd795bf72bc20`. The local version defers Airflow imports, resolves an
optional task, preserves dependency flags on both execution paths, implements Airflow 3.2+
`mark_success`, commits before Execution API access, and refreshes the original ORM task instance.
The ASF license header is retained in that module.

`ordered_task_instances` and all DagMaker extensions are independently authored for this project.

No proprietary source code, credentials, hostnames, internal paths, or private repository history
may be included in this project.

## References

- Apache Airflow: https://github.com/apache/airflow
- Apache Airflow license: https://github.com/apache/airflow/blob/main/LICENSE
- Adapted task-instance helper: https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
- pytest plugin documentation: https://docs.pytest.org/en/stable/how-to/writing_plugins.html
