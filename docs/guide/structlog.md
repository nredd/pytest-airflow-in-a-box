# Asserting on what a task logged

On Airflow 3, `caplog` does not see task logs. It does not fail -- it comes back **empty**, and
an empty capture makes a whole family of assertions vacuous:

```python
def test_no_error_logged(caplog, dag_maker):
    ...
    dag_maker.run_ti("load")

    # Passes. Always. `caplog.records` is `[]`.
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert all(record.levelno < logging.ERROR for record in caplog.records)
    assert "quarantining" not in caplog.text
```

Every one of those is green whether or not your operator logged the error, and stays green
after you invert the branch that produces it. Airflow 3 emits its records through structlog,
which never hands them to a stdlib logger, so pytest's `caplog` handler has nothing to catch.

`cap_structlog` catches them:

```python
def test_logging(cap_structlog, dag_maker):
    with dag_maker():

        @task
        def speak() -> None:
            structlog.get_logger("task_logger").warning("task_event", answer=42)

        speak()

    dag_maker.run_ti("speak")

    assert "task_event" in cap_structlog
    assert {"answer": 42, "log_level": "warning"} in cap_structlog
```

## When a log line is worth asserting on

Asserting on a log line is a weaker contract than asserting on a value, and
[deciding which failures are yours](testing-scope.md) never lists it for that reason. Use it when the log line
is the *only* observable your code produces:

- A sensor rescheduling: `poke()` returned `False` and the reason lives in the message
- An operator taking its fallback path -- same return value, different route
- A task that warns and then skips, or warns and then returns `None`

When you have a return value, an XCom, a task state, or a rendered field to assert on, assert
on that instead.

## Why not `structlog.testing.capture_logs`

structlog ships its own capture and it loses here for two reasons:

- **It does not survive Airflow reconfiguring structlog.** `capture_logs` installs a processor
  list and Airflow's own `structlog.configure` / `configure_once` mid-test replaces it, taking
  the capture with it. `cap_structlog` intercepts both calls so the capture processor is
  reinstalled on the new chain, and teardown restores whatever chain the test left behind,
  minus the capture. Regression-tested at `tests/fixtures/test_cap_structlog.py:29-70`
- **It swallows the output.** `capture_logs` raises `DropEvent`, so nothing renders and a
  failing test shows you no logs. `cap_structlog` is pass-through: it records the event and
  hands it to the next processor unchanged, so your normal log output is still there.

Known limit: bound loggers created *before* the fixture keep their frozen processor chain. New
loggers are covered -- `cache_logger_on_first_use` is disabled while the capture is installed.

## Reading the capture

`StructlogCapture` gives you four views, and they answer different questions:

| Form | Matches |
|---|---|
| `"task_event" in cap_structlog` | an event whose `event` key equals the string, exactly |
| `{"answer": 42} in cap_structlog` | an event whose items are a **superset** of the dict |
| `cap_structlog.text` | the event names only, newline-joined, for substring assertions |
| `cap_structlog.entries` | the raw `list[dict]`, for anything the above cannot phrase |

The dict form is a subset test, not equality -- `{"answer": 42, "log_level": "warning"}` matches
an event carrying a dozen other bound values. `log_level` is added by the capture from the
logger method name, so `warning`, `info`, and friends are matchable without touching the
renderer. Anything a probe cannot express goes through `entries`:

```python
assert [entry["event"] for entry in cap_structlog.entries if entry["log_level"] == "warning"] == [
    "rate_limited",
    "falling_back",
]
```

`in` accepts a `str` or a `dict` and raises `TypeError` on anything else.

Airflow 2.x logs through stdlib logging, so `cap_structlog` fails there with an actionable
message telling you to use `caplog` -- which on 2.x works.
