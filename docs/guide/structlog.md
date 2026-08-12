# Structlog capture

Airflow 3 logs through structlog, where pytest's builtin `caplog` cannot see records. The
`cap_structlog` fixture records every event emitted during the test:

```python
def test_logging(cap_structlog, dag_maker):
    ...
    assert "task_event" in cap_structlog
    assert {"answer": 42, "log_level": "warning"} in cap_structlog
```
