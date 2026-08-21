"""Exercise real entry-point discovery for a consumer-style provider distribution.

The `airflow_isolated` marker is the consumer contract under test: a provider-shaped
user package's `apache_airflow_provider` entry point resolves through a live
``ProvidersManager`` in a one-shot child process, with no monkeypatching of Airflow's
cached entry-point grouping anywhere. This retires the compatibility suite's former
"real distribution discovery is out of scope" caveat (`docs/development.md`).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.compat


def test_provider_distribution_resolves_through_a_live_providers_manager(
    pytester: pytest.Pytester,
) -> None:
    """Discover a consumer provider's entry point through real metadata resolution.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makepyfile(
        consumer_provider="""
        def get_provider_info() -> dict[str, object]:
            return {
                "package-name": "consumer-provider",
                "name": "Consumer Provider",
                "description": "Consumer-style provider distribution.",
                "versions": ["0.0.0"],
            }
        """,
        test_consumer_provider="""
        import pytest


        @pytest.mark.airflow_isolated(
            entry_points={
                "apache_airflow_provider": (
                    "provider_info = consumer_provider:get_provider_info"
                )
            },
            name="consumer-provider",
        )
        def test_provider_is_discovered() -> None:
            from airflow.providers_manager import ProvidersManager

            manager = ProvidersManager()
            assert "consumer-provider" in manager.providers
            # The manager reads the version from the synthetic distribution's own
            # metadata, which the marker pins at `0.0.0`, not from the info dict.
            assert manager.providers["consumer-provider"].version == "0.0.0"
        """,
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
