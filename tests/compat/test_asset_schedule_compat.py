"""Test release-varying branches of `_compat.asset_schedule` on the installed 3.x family.

`_asset_unique_key_type`'s SDK-location branch is real on every certified 3.1.x
release, but the installed dev environment resolves to the newer serialization
location -- `airflow.sdk.definitions.asset.AssetUniqueKey` still exists on later
releases too, just not as the canonical one `dags_needing_dagruns` reads from, so this
exercises the fallback directly rather than through a full capability fake.
"""

from __future__ import annotations

from types import SimpleNamespace

from pytest_airflow_in_a_box._compat import asset_schedule as schedule_module
from pytest_airflow_in_a_box._compat.capabilities import AssetUniqueKeyLocation


def test_asset_unique_key_type_falls_back_to_the_sdk_location() -> None:
    """Resolve `airflow.sdk.definitions.asset.AssetUniqueKey` off the SDK location."""

    from airflow.sdk.definitions.asset import AssetUniqueKey

    capabilities = SimpleNamespace(asset_unique_key_location=AssetUniqueKeyLocation.SDK)

    assert schedule_module._asset_unique_key_type(capabilities) is AssetUniqueKey


def test_asset_unique_key_type_uses_the_serialization_location() -> None:
    """Resolve `SerializedAssetUniqueKey` off the newer serialization location."""

    from airflow.serialization.definitions.assets import SerializedAssetUniqueKey

    capabilities = SimpleNamespace(asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION)

    assert schedule_module._asset_unique_key_type(capabilities) is SerializedAssetUniqueKey
