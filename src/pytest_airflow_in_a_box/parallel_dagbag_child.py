"""Bare-script child worker for one Dag bag fan-out shard.

Invoked as ``sys.executable -m pytest_airflow_in_a_box.parallel_dagbag_child`` by
`parallel_dagbag.fan_out_dag_bag` -- never by entry point, and never a pytest session:
this child runs no tests, it only imports its assigned shard of Dag files and writes a
JSON payload the parent process merges. A nested pytest session (plugin registration,
conftest discovery, collection machinery) would be pure overhead a script this narrow
never needs; that machinery only earns its cost for `isolated.py`'s children, which
exist specifically to run real pytest test items.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/243
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pytest_airflow_in_a_box._compat.dagbag import build_partial_dag_bag
from pytest_airflow_in_a_box._compat.introspection import record_secrets_lookups
from pytest_airflow_in_a_box._compat.parse_time import ParseTimeComms
from pytest_airflow_in_a_box.bootstrap import _state_from_environment
from pytest_airflow_in_a_box.parallel_dagbag import (
    SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE,
    SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE,
    encode_shard,
    shard_from_payload,
)

_EXIT_OK = 0
_EXIT_FAILURE = 1


def _write_payload(path: Path, payload: dict[str, object]) -> None:
    """Atomically publish one shard's output payload.

    Parameters:
        path: pathlib.Path naming the final output artifact.
        payload: dict[str, object] to serialize.
    """

    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(payload), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    """Parse one shard of Dag files and write its portable payload.

    Returns:
        int containing the process exit status.
    """

    spec_path = os.environ.get(SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE)
    output_path = os.environ.get(SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE)
    if not spec_path or not output_path:
        return _EXIT_FAILURE
    try:
        spec = shard_from_payload(json.loads(Path(spec_path).read_text(encoding="utf-8")))
        state = _state_from_environment(role="Dag bag fan-out shard")
        os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = str(spec.dagbag_import_timeout)
        comms = ParseTimeComms(run_root=state.root) if spec.needs_comms else None
        dag_folder = Path(spec.dag_folder)
        with record_secrets_lookups(dag_folder) as recorded:
            dag_bag, stats = build_partial_dag_bag(dag_folder, spec.file_paths, comms=comms)
        payload = encode_shard(
            dag_bag.dags, dag_bag.import_errors, stats, tuple(dict.fromkeys(recorded))
        )
    except Exception:
        return _EXIT_FAILURE
    _write_payload(Path(output_path), payload)
    return _EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
