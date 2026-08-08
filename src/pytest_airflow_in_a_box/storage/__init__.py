"""Public storage-location API.

References:
    https://man7.org/linux/man-pages/man5/proc_mounts.5.html
    https://man7.org/linux/man-pages/man2/statfs.2.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box.storage.locate import (
    DarwinFilesystem,
    Mount,
    StorageFallbackWarning,
    StorageLocation,
    StorageReason,
    is_network_filesystem,
    locate_storage,
    parse_proc_mounts,
)
from pytest_airflow_in_a_box.storage.postgres import PostgresProvisioner
from pytest_airflow_in_a_box.storage.provision import (
    DbBackend,
    Provisioner,
    SqliteProvisioner,
    select_provisioner,
)
from pytest_airflow_in_a_box.storage.sqlite import (
    PragmaProfile,
    calculate_profile,
    create_metadata_engine,
    install_legacy_sqlite_listener,
    write_local_settings,
)

__all__ = (
    "DarwinFilesystem",
    "DbBackend",
    "Mount",
    "PostgresProvisioner",
    "PragmaProfile",
    "Provisioner",
    "SqliteProvisioner",
    "StorageFallbackWarning",
    "StorageLocation",
    "StorageReason",
    "calculate_profile",
    "create_metadata_engine",
    "install_legacy_sqlite_listener",
    "is_network_filesystem",
    "locate_storage",
    "parse_proc_mounts",
    "select_provisioner",
    "write_local_settings",
)
