"""Test storage selection and network filesystem detection."""

from __future__ import annotations

import stat
import sys
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.storage import (
    StorageFallbackWarning,
    StorageReason,
    is_network_filesystem,
    locate_storage,
    parse_proc_mounts,
)
from pytest_airflow_in_a_box.storage import locate as locate_module


def _mount(path: Path, filesystem_type: str) -> str:
    """Build one proc mount line with kernel-style path escaping."""
    encoded = str(path).replace("\\", "\\134").replace(" ", "\\040")
    return f"device {encoded} {filesystem_type} rw 0 0"


def _unknown_statfs(path: Path) -> None:
    """Return no filesystem magic for deterministic conservative tests."""
    del path


@pytest.mark.parametrize(
    ("filesystem_type", "expected"),
    [
        ("nfs", True),
        ("nfs4", True),
        ("cifs", True),
        ("smb3", True),
        ("fuse.sshfs", True),
        ("afs", True),
        ("9p", True),
        ("lustre", True),
        ("gpfs", True),
        ("ceph", True),
        ("ext4", False),
        ("xfs", False),
        ("tmpfs", False),
        ("overlay", False),
    ],
)
def test_mount_filesystem_types_are_classified(
    tmp_path: Path, filesystem_type: str, expected: bool
) -> None:
    """Recognize required local and remote mount types."""
    assert (
        is_network_filesystem(
            tmp_path,
            mounts_text=_mount(tmp_path, filesystem_type),
            platform_name="linux",
            statfs_reader=_unknown_statfs,
        )
        is expected
    )


def test_mount_matching_uses_longest_path_prefix(tmp_path: Path) -> None:
    """Let a nested local mount override its network-backed parent."""
    nested = tmp_path / "local" / "work"
    nested.mkdir(parents=True)
    mounts = "\n".join((_mount(tmp_path, "nfs4"), _mount(tmp_path / "local", "xfs")))

    assert not is_network_filesystem(
        nested,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )


def test_mount_prefix_requires_a_path_component_boundary(tmp_path: Path) -> None:
    """Do not confuse sibling names sharing a textual prefix."""
    mounted = tmp_path / "data"
    sibling = tmp_path / "database"
    mounted.mkdir()
    sibling.mkdir()
    mounts = "\n".join((_mount(tmp_path, "ext4"), _mount(mounted, "nfs")))

    assert not is_network_filesystem(
        sibling,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )


def test_parse_proc_mounts_decodes_escaped_paths(tmp_path: Path) -> None:
    """Decode spaces and backslashes from proc mount entries."""
    mount_path = tmp_path / "space and\\slash"
    mount_path.mkdir()

    parsed = parse_proc_mounts(_mount(mount_path, "nfs4"))

    assert len(parsed) == 1
    assert parsed[0].path == mount_path
    assert parsed[0].filesystem_type == "nfs4"


def test_parse_proc_mounts_skips_malformed_lines() -> None:
    """Ignore incomplete proc mount records."""
    assert parse_proc_mounts("bad line\n") == ()


@pytest.mark.parametrize("magic", [0x6969, 0xFF534D42, 0x01021997])
def test_linux_statfs_magic_detects_network_filesystems(tmp_path: Path, magic: int) -> None:
    """Use Linux statfs magic when the mount type is unavailable."""
    assert is_network_filesystem(
        tmp_path,
        mounts_text="",
        platform_name="linux",
        statfs_reader=lambda path: magic if path == tmp_path.resolve() else None,
    )


def test_linux_statfs_magic_detects_local_filesystem(tmp_path: Path) -> None:
    """Accept a known local Linux statfs magic without mount data."""
    assert not is_network_filesystem(
        tmp_path,
        mounts_text="",
        platform_name="linux",
        statfs_reader=lambda path: 0x58465342 if path == tmp_path.resolve() else None,
    )


def test_windows_unknown_filesystems_are_conservatively_network(tmp_path: Path) -> None:
    """Avoid silently treating unknown Windows storage as local."""
    assert is_network_filesystem(tmp_path, mounts_text="", platform_name="win32")


def _failed_darwin_statfs(path: Path) -> None:
    """Return no Darwin filesystem data for deterministic conservative tests."""
    del path


def test_darwin_unprobeable_path_is_conservatively_network(tmp_path: Path) -> None:
    """Avoid treating macOS storage as local when the statfs probe fails."""
    assert is_network_filesystem(
        tmp_path,
        mounts_text="",
        platform_name="darwin",
        darwin_statfs_reader=_failed_darwin_statfs,
    )


@pytest.mark.parametrize(
    ("filesystem_type", "local_flag", "expected"),
    [
        ("apfs", True, False),
        ("hfs", True, False),
        ("nfs", True, True),
        ("smbfs", False, True),
        ("webdav", False, True),
        ("afpfs", False, True),
        ("weirdfs", True, False),
        ("weirdfs", False, True),
    ],
)
def test_darwin_statfs_classifies_by_type_then_local_flag(
    tmp_path: Path,
    filesystem_type: str,
    local_flag: bool,
    expected: bool,
) -> None:
    """Classify known Darwin type names first and defer to ``MNT_LOCAL`` otherwise."""
    reader_calls: list[Path] = []

    def reader(path: Path) -> locate_module.DarwinFilesystem:
        reader_calls.append(path)
        return locate_module.DarwinFilesystem(
            filesystem_type=filesystem_type, local_flag=local_flag
        )

    assert (
        is_network_filesystem(
            tmp_path,
            mounts_text="",
            platform_name="darwin",
            darwin_statfs_reader=reader,
        )
        is expected
    )
    assert reader_calls == [tmp_path.resolve()]


@pytest.mark.skipif(not sys.platform.startswith("darwin"), reason="requires a Darwin libc")
def test_darwin_statfs_probes_the_root_filesystem() -> None:
    """Report a local, named filesystem for the macOS root through real libc."""
    probed = locate_module._darwin_statfs(Path("/"))

    assert probed is not None
    assert probed.filesystem_type
    assert probed.local_flag


@pytest.mark.parametrize("platform_name", ["darwin", "win32"])
def test_injected_local_mounts_are_platform_independent(
    tmp_path: Path, platform_name: str
) -> None:
    """Honor deterministic mount data independently of the host platform."""
    assert not is_network_filesystem(
        tmp_path,
        mounts_text=_mount(tmp_path, "apfs"),
        platform_name=platform_name,
    )


def test_explicit_storage_wins_and_reports_reason(tmp_path: Path) -> None:
    """Select a valid explicit local path before every automatic candidate."""
    explicit = tmp_path / "explicit"
    candidate = tmp_path / "candidate"
    explicit.mkdir()
    candidate.mkdir()
    mounts = "\n".join((_mount(explicit, "xfs"), _mount(candidate, "xfs")))

    location = locate_storage(
        explicit,
        candidate,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )

    assert location.path.parent == explicit
    assert location.reason is StorageReason.EXPLICIT
    assert not location.network


def test_explicit_network_storage_requires_opt_in(tmp_path: Path) -> None:
    """Reject an explicit network path unless the caller accepts the risk."""
    explicit = tmp_path / "explicit"
    explicit.mkdir()
    mounts = _mount(explicit, "nfs4")

    with pytest.raises(ValueError, match="allow_network=True"):
        locate_storage(explicit, mounts_text=mounts, platform_name="linux")

    location = locate_storage(
        explicit,
        allow_network=True,
        mounts_text=mounts,
        platform_name="linux",
    )
    assert location.path.parent == explicit
    assert location.reason is StorageReason.EXPLICIT
    assert location.network


def test_missing_explicit_path_is_validated_before_detection(tmp_path: Path) -> None:
    """Fail a missing explicit path before invoking an expensive statfs probe."""
    missing = tmp_path / "missing"

    def fail_statfs(path: Path) -> int:
        """Fail if path validation does not happen first."""
        raise AssertionError(f"Unexpected statfs call for '{path}'")

    with pytest.raises(FileNotFoundError, match="does not exist"):
        locate_storage(
            missing,
            mounts_text="",
            platform_name="linux",
            statfs_reader=fail_statfs,
        )


def test_explicit_file_is_rejected(tmp_path: Path) -> None:
    """Reject an explicit path that names a regular file."""
    explicit = tmp_path / "file"
    explicit.touch()

    with pytest.raises(ValueError, match="not a directory"):
        locate_storage(explicit)


def test_unwritable_explicit_directory_is_rejected(tmp_path: Path) -> None:
    """Reject an explicit directory without any write mode bit."""
    explicit = tmp_path / "readonly"
    explicit.mkdir(mode=stat.S_IRUSR | stat.S_IXUSR)

    with pytest.raises(ValueError, match="not writable"):
        locate_storage(explicit)


def test_negative_shared_memory_requirement_is_rejected() -> None:
    """Reject an invalid space threshold before location work starts."""
    with pytest.raises(ValueError, match="must be non-negative"):
        locate_storage(minimum_shared_memory_bytes=-1)


def test_caller_temp_candidate_precedes_shared_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Select the caller's basetemp or TMPDIR candidate before shared memory."""
    candidate = tmp_path / "candidate"
    shared = tmp_path / "shm"
    candidate.mkdir()
    shared.mkdir()
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", shared)
    mounts = "\n".join((_mount(candidate, "xfs"), _mount(shared, "tmpfs")))

    location = locate_storage(
        candidate=candidate,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )

    assert location.path.parent == candidate
    assert location.reason is StorageReason.CALLER_TEMP


def test_shared_memory_precedes_system_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Select adequately sized local tmpfs before the system temp directory."""
    shared = tmp_path / "shm"
    system_temp = tmp_path / "system"
    shared.mkdir()
    system_temp.mkdir()
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", shared)
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(system_temp))
    monkeypatch.setattr(locate_module, "_free_bytes", lambda _path: 4096)
    mounts = "\n".join((_mount(shared, "tmpfs"), _mount(system_temp, "xfs")))

    location = locate_storage(
        minimum_shared_memory_bytes=4096,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )

    assert location.path.parent == shared
    assert location.reason is StorageReason.SHARED_MEMORY


def test_low_space_shared_memory_falls_back_to_system_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skip tmpfs when its available capacity is below the required threshold."""
    shared = tmp_path / "shm"
    system_temp = tmp_path / "system"
    shared.mkdir()
    system_temp.mkdir()
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", shared)
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(system_temp))
    monkeypatch.setattr(locate_module, "_free_bytes", lambda _path: 1023)
    mounts = "\n".join((_mount(shared, "tmpfs"), _mount(system_temp, "xfs")))

    location = locate_storage(
        minimum_shared_memory_bytes=1024,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )

    assert location.path.parent == system_temp
    assert location.reason is StorageReason.SYSTEM_TEMP


def test_network_candidates_use_loud_writable_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Warn loudly before using the first writable network candidate."""
    candidate = tmp_path / "candidate"
    system_temp = tmp_path / "system"
    missing_shared = tmp_path / "missing-shm"
    candidate.mkdir()
    system_temp.mkdir()
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", missing_shared)
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(system_temp))
    mounts = "\n".join((_mount(candidate, "nfs4"), _mount(system_temp, "cifs")))

    with pytest.warns(StorageFallbackWarning, match="UNSAFE STORAGE FALLBACK"):
        location = locate_storage(
            candidate=candidate,
            mounts_text=mounts,
            platform_name="linux",
            statfs_reader=_unknown_statfs,
        )

    assert location.path.parent == candidate
    assert location.reason is StorageReason.WRITABLE_FALLBACK
    assert location.network


def test_each_call_creates_a_unique_directory(tmp_path: Path) -> None:
    """Use mkdtemp to isolate repeated runs sharing one base."""
    mounts = _mount(tmp_path, "xfs")

    first = locate_storage(
        candidate=tmp_path,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )
    second = locate_storage(
        candidate=tmp_path,
        mounts_text=mounts,
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )

    assert first.path != second.path
    assert first.path.is_dir()
    assert second.path.is_dir()
    assert first.path.name.startswith("pytest-airflow-in-a-box-")
