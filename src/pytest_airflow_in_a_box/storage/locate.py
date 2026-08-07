"""Select local storage and create an isolated directory for one test run.

References:
    https://man7.org/linux/man-pages/man5/proc_mounts.5.html
    https://man7.org/linux/man-pages/man2/statfs.2.html
    https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/statfs.2.html
    https://docs.python.org/3/library/tempfile.html#tempfile.mkdtemp
"""

from __future__ import annotations

import ctypes
import os
import re
import shutil
import stat
import sys
import tempfile
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class StrEnum(str, Enum):
    """Provide stdlib-compatible ``StrEnum`` semantics on Python 3.10."""

    def __str__(self) -> str:
        """Return the string value.

        Returns:
            str containing the enum value.
        """

        return self.value


DEFAULT_MINIMUM_SHARED_MEMORY_BYTES = 512 * 1024 * 1024
RUN_DIRECTORY_PREFIX = "pytest-airflow-in-a-box-"
SHARED_MEMORY_PATH = Path("/dev/shm")
PROC_MOUNTS_PATH = Path("/proc/mounts")

PROC_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")
NETWORK_FILESYSTEM_TYPES = frozenset(
    {"9p", "afpfs", "afs", "ceph", "cifs", "fuse.sshfs", "gpfs", "lustre", "nfs", "nfs4", "webdav"}
)
LOCAL_FILESYSTEM_TYPES = frozenset(
    {
        "apfs",
        "btrfs",
        "devtmpfs",
        "exfat",
        "ext2",
        "ext3",
        "ext4",
        "f2fs",
        "hfs",
        "hfsplus",
        "overlay",
        "ramfs",
        "tmpfs",
        "ufs",
        "vfat",
        "xfs",
        "zfs",
    }
)
NETWORK_STATFS_MAGICS = frozenset(
    {
        0x0000564C,  # NCP
        0x00006969,  # NFS
        0x00C36400,  # Ceph
        0x01021997,  # 9P
        0x0BD00BD0,  # Lustre
        0x47504653,  # GPFS
        0x517B,  # SMB
        0x5346414F,  # AFS
        0x73757245,  # Coda
        0xFF534D42,  # CIFS
    }
)
LOCAL_STATFS_MAGICS = frozenset(
    {
        0x0000EF53,  # ext2/ext3/ext4
        0x01021994,  # tmpfs
        0x58465342,  # XFS
        0x794C7630,  # overlayfs
        0x9123683E,  # Btrfs
    }
)
TMPFS_MAGIC = 0x01021994
DARWIN_MNT_LOCAL = 0x00001000


class StorageReason(StrEnum):
    """Reason that a storage base won the location search."""

    EXPLICIT = "explicit"
    CALLER_TEMP = "caller-temp"
    SHARED_MEMORY = "shared-memory"
    SYSTEM_TEMP = "system-temp"
    WRITABLE_FALLBACK = "writable-fallback"


class StorageFallbackWarning(RuntimeWarning):
    """Warn that no verified local filesystem was available."""


@dataclass(frozen=True)
class Mount:
    """One parsed mount table entry.

    Parameters:
        path: pathlib.Path containing the decoded mount point.
        filesystem_type: str containing the kernel filesystem type.
    """

    path: Path
    filesystem_type: str


@dataclass(frozen=True)
class StorageLocation:
    """Storage directory selected for one run.

    Parameters:
        path: pathlib.Path containing the newly created run directory.
        reason: StorageReason explaining the selected base directory.
        network: bool indicating whether the base is on a detected or conservatively
            assumed network filesystem.
    """

    path: Path
    reason: StorageReason
    network: bool


@dataclass(frozen=True)
class _FilesystemInfo:
    """Internal filesystem classification.

    Parameters:
        filesystem_type: str | None reported by the matching mount entry.
        magic: int | None reported by Linux ``statfs(2)``.
        network: bool indicating whether network semantics must be assumed.
    """

    filesystem_type: str | None
    magic: int | None
    network: bool


class _LinuxStatfs(ctypes.Structure):
    """Linux ``struct statfs`` storage used by the libc call."""

    _fields_ = [
        ("f_type", ctypes.c_long),
        ("f_bsize", ctypes.c_long),
        ("f_blocks", ctypes.c_ulong),
        ("f_bfree", ctypes.c_ulong),
        ("f_bavail", ctypes.c_ulong),
        ("f_files", ctypes.c_ulong),
        ("f_ffree", ctypes.c_ulong),
        ("f_fsid", ctypes.c_int * 2),
        ("f_namelen", ctypes.c_long),
        ("f_frsize", ctypes.c_long),
        ("f_flags", ctypes.c_long),
        ("f_spare", ctypes.c_long * 4),
    ]


class _DarwinStatfs(ctypes.Structure):
    """Darwin 64-bit-inode ``struct statfs`` storage used by the libc call."""

    _fields_ = [
        ("f_bsize", ctypes.c_uint32),
        ("f_iosize", ctypes.c_int32),
        ("f_blocks", ctypes.c_uint64),
        ("f_bfree", ctypes.c_uint64),
        ("f_bavail", ctypes.c_uint64),
        ("f_files", ctypes.c_uint64),
        ("f_ffree", ctypes.c_uint64),
        ("f_fsid", ctypes.c_int32 * 2),
        ("f_owner", ctypes.c_uint32),
        ("f_type", ctypes.c_uint32),
        ("f_flags", ctypes.c_uint32),
        ("f_fssubtype", ctypes.c_uint32),
        ("f_fstypename", ctypes.c_char * 16),
        ("f_mntonname", ctypes.c_char * 1024),
        ("f_mntfromname", ctypes.c_char * 1024),
        ("f_flags_ext", ctypes.c_uint32),
        ("f_reserved", ctypes.c_uint32 * 7),
    ]


@dataclass(frozen=True)
class DarwinFilesystem:
    """Darwin ``statfs(2)`` classification inputs.

    Parameters:
        filesystem_type: str containing the reported ``f_fstypename``.
        local_flag: bool containing the kernel ``MNT_LOCAL`` mount flag.
    """

    filesystem_type: str
    local_flag: bool


StatfsReader = Callable[[Path], int | None]
DarwinStatfsReader = Callable[[Path], DarwinFilesystem | None]


def _decode_mount_path(value: str) -> str:
    """Decode octal escapes used by ``/proc/mounts``.

    Parameters:
        value: str containing an escaped mount point.

    Returns:
        str containing the decoded mount point.
    """

    return PROC_MOUNT_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), value)


def parse_proc_mounts(contents: str) -> tuple[Mount, ...]:
    """Parse Linux ``/proc/mounts`` contents.

    Parameters:
        contents: str containing a proc mount table.

    Returns:
        tuple[Mount, ...] containing valid entries in source order.
    """

    mounts: list[Mount] = []
    for line in contents.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        mounts.append(
            Mount(
                path=Path(_decode_mount_path(fields[1])),
                filesystem_type=fields[2].lower(),
            )
        )
    return tuple(mounts)


def _read_mounts(contents: str | None, platform_name: str) -> tuple[Mount, ...]:
    """Read the real Linux mount table or parse injected contents.

    Parameters:
        contents: str | None containing an injected mount table.
        platform_name: str identifying the target platform.

    Returns:
        tuple[Mount, ...] containing parsed mount entries, or an empty tuple when the
        platform has no proc mount table.
    """

    if contents is not None:
        return parse_proc_mounts(contents)
    if not platform_name.startswith("linux"):
        return ()
    try:
        return parse_proc_mounts(PROC_MOUNTS_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError):
        return ()


def _matching_mount(path: Path, mounts: Sequence[Mount]) -> Mount | None:
    """Find the deepest mount point containing a path.

    Parameters:
        path: pathlib.Path to classify.
        mounts: Sequence[Mount] containing candidate mount points.

    Returns:
        Mount | None containing the longest-prefix match.
    """

    matches = [mount for mount in mounts if mount.path == path or mount.path in path.parents]
    if not matches:
        return None
    return max(matches, key=lambda mount: len(mount.path.parts))


def _filesystem_type_is_network(filesystem_type: str) -> bool | None:
    """Classify a mount filesystem type when it is known.

    Parameters:
        filesystem_type: str reported by the mount table.

    Returns:
        bool | None indicating network, local, or unknown classification.
    """

    normalized = filesystem_type.lower()
    if normalized in NETWORK_FILESYSTEM_TYPES or normalized.startswith("smb"):
        return True
    if normalized in LOCAL_FILESYSTEM_TYPES:
        return False
    return None


def _linux_statfs_magic(path: Path) -> int | None:
    """Read Linux filesystem magic through ``statfs(2)``.

    Parameters:
        path: pathlib.Path to inspect.

    Returns:
        int | None containing an unsigned 32-bit filesystem magic, or ``None`` when
        libc cannot inspect the path.
    """

    libc = ctypes.CDLL(None, use_errno=True)
    statfs = libc.statfs
    statfs.argtypes = [ctypes.c_char_p, ctypes.POINTER(_LinuxStatfs)]
    statfs.restype = ctypes.c_int
    result = _LinuxStatfs()
    if statfs(os.fsencode(path), ctypes.byref(result)) != 0:
        return None
    return int(result.f_type) & 0xFFFFFFFF


def _darwin_statfs(path: Path) -> DarwinFilesystem | None:
    """Read the Darwin filesystem type name and locality flag via ``statfs(2)``.

    Intel macOS exposes the legacy pre-10.6 layout under the plain ``statfs``
    symbol, so the 64-bit-inode ``statfs$INODE64`` symbol must win when present;
    Apple silicon only ships the 64-bit-inode layout under the plain name.

    Parameters:
        path: pathlib.Path to inspect.

    Returns:
        DarwinFilesystem | None containing the reported classification inputs, or
        ``None`` when libc cannot inspect the path.
    """

    libc = ctypes.CDLL(None, use_errno=True)
    try:
        statfs = libc["statfs$INODE64"]
    except (AttributeError, KeyError):
        statfs = libc.statfs
    statfs.argtypes = [ctypes.c_char_p, ctypes.POINTER(_DarwinStatfs)]
    statfs.restype = ctypes.c_int
    result = _DarwinStatfs()
    if statfs(os.fsencode(path), ctypes.byref(result)) != 0:
        return None
    filesystem_type = bytes(result.f_fstypename).split(b"\x00", 1)[0]
    if not filesystem_type:
        return None
    return DarwinFilesystem(
        filesystem_type=filesystem_type.decode("ascii", "replace").lower(),
        local_flag=bool(int(result.f_flags) & DARWIN_MNT_LOCAL),
    )


def _windows_path_is_network(path: Path) -> bool:
    """Classify a Windows path without assuming unknown drives are local.

    Parameters:
        path: pathlib.Path to inspect.

    Returns:
        bool indicating a remote or conservatively unknown drive.
    """

    value = str(path)
    if value.startswith(("\\\\", "//")):
        return True
    if os.name != "nt" or not path.drive:
        return True
    windll = getattr(ctypes, "windll", None)
    if windll is None:
        return True
    drive_type = int(windll.kernel32.GetDriveTypeW(f"{path.drive}\\"))
    return drive_type not in {2, 3, 6}


def _inspect_filesystem(
    path: Path,
    mounts: Sequence[Mount],
    platform_name: str,
    statfs_reader: StatfsReader | None,
    darwin_statfs_reader: DarwinStatfsReader | None,
) -> _FilesystemInfo:
    """Combine mount data and platform-specific filesystem inspection.

    Parameters:
        path: pathlib.Path to classify.
        mounts: Sequence[Mount] containing parsed mount entries.
        platform_name: str identifying the target platform.
        statfs_reader: StatfsReader | None overriding the Linux libc probe.
        darwin_statfs_reader: DarwinStatfsReader | None overriding the Darwin libc probe.

    Returns:
        _FilesystemInfo containing type, magic, and network classification.
    """

    resolved = Path(str(path)).resolve()
    mount = _matching_mount(resolved, mounts)
    filesystem_type = mount.filesystem_type if mount is not None else None
    type_classification = (
        _filesystem_type_is_network(filesystem_type) if filesystem_type is not None else None
    )
    if type_classification is not None:
        return _FilesystemInfo(filesystem_type, None, type_classification)

    if platform_name.startswith("linux"):
        reader = statfs_reader or _linux_statfs_magic
        magic = reader(resolved)
        if magic is not None:
            normalized_magic = magic & 0xFFFFFFFF
            if normalized_magic in NETWORK_STATFS_MAGICS:
                return _FilesystemInfo(filesystem_type, normalized_magic, True)
            if normalized_magic in LOCAL_STATFS_MAGICS:
                return _FilesystemInfo(filesystem_type, normalized_magic, False)
        return _FilesystemInfo(filesystem_type, magic, True)

    if platform_name.startswith("darwin"):
        darwin_reader = darwin_statfs_reader or _darwin_statfs
        darwin_info = darwin_reader(resolved)
        if darwin_info is None:
            return _FilesystemInfo(filesystem_type, None, True)
        darwin_classification = _filesystem_type_is_network(darwin_info.filesystem_type)
        if darwin_classification is None:
            darwin_classification = not darwin_info.local_flag
        return _FilesystemInfo(darwin_info.filesystem_type, None, darwin_classification)

    if platform_name.startswith("win"):
        return _FilesystemInfo(filesystem_type, None, _windows_path_is_network(resolved))
    return _FilesystemInfo(filesystem_type, None, True)


def is_network_filesystem(
    path: Path,
    *,
    mounts_text: str | None = None,
    platform_name: str | None = None,
    statfs_reader: StatfsReader | None = None,
    darwin_statfs_reader: DarwinStatfsReader | None = None,
) -> bool:
    """Determine whether a path has network filesystem semantics.

    macOS classification uses the Darwin ``statfs(2)`` type name and ``MNT_LOCAL``
    flag. Unprobeable macOS paths and unknown Windows drives are conservatively
    treated as network-backed.

    Parameters:
        path: pathlib.Path to inspect.
        mounts_text: str | None containing an injectable proc mount table.
        platform_name: str | None overriding ``sys.platform`` for deterministic tests.
        statfs_reader: StatfsReader | None overriding the Linux libc probe.
        darwin_statfs_reader: DarwinStatfsReader | None overriding the Darwin libc probe.

    Returns:
        bool indicating whether network semantics were detected or must be assumed.
    """

    selected_platform = platform_name or sys.platform
    mounts = _read_mounts(mounts_text, selected_platform)
    return _inspect_filesystem(
        path, mounts, selected_platform, statfs_reader, darwin_statfs_reader
    ).network


def _is_writable_directory(path: Path) -> bool:
    """Check that an existing path can contain a run directory.

    Parameters:
        path: pathlib.Path to validate.

    Returns:
        bool indicating whether the path is a writable, searchable directory.
    """

    try:
        mode = path.stat().st_mode
    except OSError:
        return False
    return (
        path.is_dir()
        and bool(mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
        and os.access(path, os.W_OK | os.X_OK)
    )


def _validate_explicit_path(path: Path) -> Path:
    """Validate an explicit base before filesystem detection starts.

    Parameters:
        path: pathlib.Path explicitly supplied by the caller.

    Returns:
        pathlib.Path containing the absolute validated directory.

    Raises:
        FileNotFoundError: The explicit path does not exist.
        ValueError: The explicit path is not a directory or is not writable.
    """

    resolved = Path(str(path)).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Explicit storage path does not exist: '{resolved}'")
    if not resolved.is_dir():
        raise ValueError(f"Explicit storage path is not a directory: '{resolved}'")
    if not _is_writable_directory(resolved):
        raise ValueError(f"Explicit storage path is not writable: '{resolved}'")
    return resolved


def _free_bytes(path: Path) -> int:
    """Return available bytes for a candidate directory.

    Parameters:
        path: pathlib.Path to inspect.

    Returns:
        int containing the available byte count.
    """

    return shutil.disk_usage(path).free


def _create_run_directory(base: Path) -> Path:
    """Create one unique run directory below a selected base.

    Parameters:
        base: pathlib.Path containing the selected base directory.

    Returns:
        pathlib.Path containing the newly created directory.

    Raises:
        OSError: ``tempfile.mkdtemp`` cannot create the directory.
    """

    return Path(tempfile.mkdtemp(prefix=RUN_DIRECTORY_PREFIX, dir=base)).resolve()


def locate_storage(
    explicit: Path | None = None,
    candidate: Path | None = None,
    *,
    allow_network: bool = False,
    minimum_shared_memory_bytes: int = DEFAULT_MINIMUM_SHARED_MEMORY_BYTES,
    mounts_text: str | None = None,
    platform_name: str | None = None,
    statfs_reader: StatfsReader | None = None,
    darwin_statfs_reader: DarwinStatfsReader | None = None,
) -> StorageLocation:
    """Select a storage base and create a unique directory for one run.

    The caller owns cleanup of the returned directory. ``candidate`` is the already
    resolved ``--basetemp`` or ``TMPDIR`` choice; this layer does not inspect process
    environment or pytest state.

    Parameters:
        explicit: pathlib.Path | None containing an explicit storage override.
        candidate: pathlib.Path | None containing a caller-selected ``--basetemp`` or
            ``TMPDIR`` base.
        allow_network: bool allowing an explicit network path when true.
        minimum_shared_memory_bytes: int required before selecting ``/dev/shm``.
        mounts_text: str | None containing an injectable proc mount table.
        platform_name: str | None overriding ``sys.platform`` for deterministic tests.
        statfs_reader: StatfsReader | None overriding the Linux libc probe.
        darwin_statfs_reader: DarwinStatfsReader | None overriding the Darwin libc probe.

    Returns:
        StorageLocation containing the new path and its selection metadata.

    Raises:
        FileNotFoundError: An explicit path does not exist.
        ValueError: An argument is invalid or an explicit path is unsafe.
        OSError: No writable candidate exists or directory creation fails.
    """

    if minimum_shared_memory_bytes < 0:
        raise ValueError(
            f"`minimum_shared_memory_bytes` must be non-negative: '{minimum_shared_memory_bytes}'"
        )

    selected_platform = platform_name or sys.platform
    if explicit is not None:
        explicit_path = _validate_explicit_path(explicit)
        mounts = _read_mounts(mounts_text, selected_platform)
        info = _inspect_filesystem(
            explicit_path, mounts, selected_platform, statfs_reader, darwin_statfs_reader
        )
        if info.network and not allow_network:
            raise ValueError(
                "Explicit storage path is on a network or unknown filesystem; pass "
                f"`allow_network=True` to accept it: '{explicit_path}'"
            )
        try:
            run_path = _create_run_directory(explicit_path)
        except OSError as error:
            raise OSError(
                f"Could not create a run directory below explicit path: '{explicit_path}': {error}"
            ) from error
        return StorageLocation(run_path, StorageReason.EXPLICIT, info.network)

    mounts = _read_mounts(mounts_text, selected_platform)
    fallback_bases: list[Path] = []
    seen_bases: set[Path] = set()

    def try_local(base: Path, reason: StorageReason) -> StorageLocation | None:
        """Try one ordinary candidate and remember writable network fallbacks.

        Parameters:
            base: pathlib.Path containing the candidate base.
            reason: StorageReason associated with the candidate.

        Returns:
            StorageLocation | None containing a created run directory on success.
        """

        resolved = Path(str(base)).resolve()
        if resolved in seen_bases or not _is_writable_directory(resolved):
            return None
        seen_bases.add(resolved)
        info = _inspect_filesystem(
            resolved, mounts, selected_platform, statfs_reader, darwin_statfs_reader
        )
        if info.network:
            fallback_bases.append(resolved)
            return None
        try:
            return StorageLocation(_create_run_directory(resolved), reason, False)
        except OSError:
            return None

    if candidate is not None:
        location = try_local(candidate, StorageReason.CALLER_TEMP)
        if location is not None:
            return location

    shared_memory_path = SHARED_MEMORY_PATH.resolve()
    if not selected_platform.startswith("win") and _is_writable_directory(shared_memory_path):
        shared_info = _inspect_filesystem(
            shared_memory_path, mounts, selected_platform, statfs_reader, darwin_statfs_reader
        )
        is_memory_filesystem = (
            shared_info.filesystem_type == "tmpfs" or shared_info.magic == TMPFS_MAGIC
        )
        try:
            has_space = _free_bytes(shared_memory_path) >= minimum_shared_memory_bytes
        except OSError:
            has_space = False
        if not shared_info.network and is_memory_filesystem and has_space:
            try:
                return StorageLocation(
                    _create_run_directory(shared_memory_path),
                    StorageReason.SHARED_MEMORY,
                    False,
                )
            except OSError:
                pass

    system_temp = Path(tempfile.gettempdir())
    location = try_local(system_temp, StorageReason.SYSTEM_TEMP)
    if location is not None:
        return location

    for fallback_base in fallback_bases:
        warnings.warn(
            "UNSAFE STORAGE FALLBACK: no verified local filesystem is writable; "
            f"using network or unknown storage at '{fallback_base}'",
            StorageFallbackWarning,
            stacklevel=2,
        )
        try:
            return StorageLocation(
                _create_run_directory(fallback_base),
                StorageReason.WRITABLE_FALLBACK,
                True,
            )
        except OSError:
            continue

    raise OSError("No writable storage directory is available")


__all__ = (
    "DarwinFilesystem",
    "Mount",
    "StorageFallbackWarning",
    "StorageLocation",
    "StorageReason",
    "is_network_filesystem",
    "locate_storage",
    "parse_proc_mounts",
)
