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

__all__ = (
    "Mount",
    "StorageFallbackWarning",
    "StorageLocation",
    "StorageReason",
    "is_network_filesystem",
    "locate_storage",
    "parse_proc_mounts",
)
