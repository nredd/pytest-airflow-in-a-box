"""Public storage-location API.

References:
    https://man7.org/linux/man-pages/man5/proc_mounts.5.html
    https://man7.org/linux/man-pages/man2/statfs.2.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box.storage.locate import (
    Mount,
    StorageFallbackWarning,
    StorageLocation,
    StorageReason,
    is_network_filesystem,
    locate_storage,
    parse_proc_mounts,
)
from pytest_airflow_in_a_box.storage.sqlite import (
    PragmaProfile,
    calculate_profile,
    create_metadata_engine,
    write_local_settings,
)

__all__ = (
    "Mount",
    "PragmaProfile",
    "StorageFallbackWarning",
    "StorageLocation",
    "StorageReason",
    "calculate_profile",
    "create_metadata_engine",
    "is_network_filesystem",
    "locate_storage",
    "parse_proc_mounts",
    "write_local_settings",
)
