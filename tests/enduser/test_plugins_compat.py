"""Verify an empty `plugins/` directory loads as a no-op against a real Airflow install.

Named risk from issue #112: writing `[core] plugins_folder` (and pinning
`AIRFLOW__CORE__PLUGINS_FOLDER`) makes Airflow's plugin loader start succeeding where an
unset plugins folder previously raised `ValueError("Plugins folder is not set")` on
releases predating a later fallback default, so anything that transitively loads plugins
now scans a real directory. The directory is empty by default and `LOAD_EXAMPLES` is
already `False`, so the load must be a genuine no-op: zero plugins discovered, no
exception raised.

`get_plugin_info` (unlike the private, cached `_get_plugins`) is the one plugin-loading
entry point with an identical name and signature on both the 2.x and 3.x families, so it
is used here instead of a `_compat/` shim.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/112
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.compat


def test_empty_plugins_folder_loads_as_a_noop(pytester: pytest.Pytester) -> None:
    """Load zero plugins from the run's default, empty `plugins/` directory.

    Parameters:
        pytester: pytest.Pytester running the assertion in a real bootstrapped subprocess.
    """

    pytester.makepyfile(
        """
        def test_plugins_load_is_a_noop():
            from airflow import plugins_manager

            assert plugins_manager.get_plugin_info() == []
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
