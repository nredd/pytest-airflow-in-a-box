"""Test storage selection and network filesystem detection."""

from __future__ import annotations

import stat
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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


def test_read_mounts_tolerates_missing_proc_mounts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return no mounts when the Linux mount table cannot be read."""
    monkeypatch.setattr(locate_module, "PROC_MOUNTS_PATH", Path("/nonexistent-proc/mounts"))

    assert locate_module._read_mounts(None, "linux") == ()


class _FakeStatfsFunction:
    """Callable statfs double accepting ctypes attribute assignment."""

    def __init__(self, fill: Any) -> None:
        """Store the struct-filling callable.

        Parameters:
            fill: Any invoked with the encoded path and byref wrapper.
        """

        self.fill = fill

    def __call__(self, path: bytes, result_ref: Any) -> int:
        """Delegate to the struct-filling callable.

        Parameters:
            path: bytes containing the encoded probe path.
            result_ref: Any containing the ctypes byref wrapper.

        Returns:
            int containing the configured statfs result.
        """

        return int(self.fill(path, result_ref))


class _FakeLinuxLibc:
    """Scriptable libc double for the Linux statfs probe."""

    def __init__(self, *, result: int, f_type: int) -> None:
        """Configure the fake statfs call.

        Parameters:
            result: int returned by the statfs call.
            f_type: int written into ``f_type``.
        """

        self.result = result
        self.f_type = f_type

    def __getattr__(self, name: str) -> Any:
        """Resolve the statfs symbol.

        Parameters:
            name: str containing the requested symbol name.

        Returns:
            Any containing the fake statfs callable.
        """

        if name == "statfs":
            return _FakeStatfsFunction(self._fill)
        raise AttributeError(name)

    def _fill(self, _path: bytes, result_ref: Any) -> int:
        """Fill the caller's struct and report the configured result.

        Parameters:
            _path: bytes containing the encoded probe path.
            result_ref: Any containing the ctypes byref wrapper.

        Returns:
            int containing the configured statfs result.
        """

        result_ref._obj.f_type = self.f_type
        return self.result


@pytest.mark.parametrize(
    ("libc", "expected"),
    [
        (_FakeLinuxLibc(result=-1, f_type=0), None),
        (_FakeLinuxLibc(result=0, f_type=0x6969), 0x6969),
        (_FakeLinuxLibc(result=0, f_type=-1), 0xFFFFFFFF),
    ],
)
def test_linux_statfs_magic_with_fake_libc(
    monkeypatch: pytest.MonkeyPatch,
    libc: Any,
    expected: object,
) -> None:
    """Cover the failure return and the unsigned magic normalization."""
    monkeypatch.setattr(locate_module.ctypes, "CDLL", lambda *_a, **_k: libc)

    assert locate_module._linux_statfs_magic(Path("/probe")) == expected


class _FakeDarwinLibc:
    """Scriptable libc double for the Darwin statfs probe."""

    def __init__(self, *, result: int, fstypename: bytes, flags: int, inode64: bool) -> None:
        """Configure the fake statfs call.

        Parameters:
            result: int returned by the statfs call.
            fstypename: bytes written into ``f_fstypename``.
            flags: int written into ``f_flags``.
            inode64: bool exposing the ``statfs$INODE64`` symbol.
        """

        self.result = result
        self.fstypename = fstypename
        self.flags = flags
        self.inode64 = inode64

    def __getitem__(self, name: str) -> Any:
        """Resolve the 64-bit-inode symbol when configured.

        Parameters:
            name: str containing the requested symbol name.

        Returns:
            Any containing the fake statfs callable.

        Raises:
            AttributeError: The symbol is not exposed.
        """

        if name == "statfs$INODE64" and self.inode64:
            return _FakeStatfsFunction(self._fill)
        raise AttributeError(name)

    def __getattr__(self, name: str) -> Any:
        """Resolve the plain statfs symbol.

        Parameters:
            name: str containing the requested symbol name.

        Returns:
            Any containing the fake statfs callable.
        """

        if name == "statfs":
            return _FakeStatfsFunction(self._fill)
        raise AttributeError(name)

    def _fill(self, _path: bytes, result_ref: Any) -> int:
        """Fill the caller's struct and report the configured result.

        Parameters:
            _path: bytes containing the encoded probe path.
            result_ref: Any containing the ctypes byref wrapper.

        Returns:
            int containing the configured statfs result.
        """

        structure = result_ref._obj
        structure.f_fstypename = self.fstypename
        structure.f_flags = self.flags
        return self.result


@pytest.mark.parametrize(
    ("libc", "expected"),
    [
        (_FakeDarwinLibc(result=-1, fstypename=b"apfs", flags=0x1000, inode64=True), None),
        (_FakeDarwinLibc(result=0, fstypename=b"", flags=0x1000, inode64=True), None),
        (
            _FakeDarwinLibc(result=0, fstypename=b"apfs", flags=0x1000, inode64=True),
            locate_module.DarwinFilesystem(filesystem_type="apfs", local_flag=True),
        ),
        (
            _FakeDarwinLibc(result=0, fstypename=b"nfs", flags=0, inode64=False),
            locate_module.DarwinFilesystem(filesystem_type="nfs", local_flag=False),
        ),
    ],
)
def test_darwin_statfs_paths_with_fake_libc(
    monkeypatch: pytest.MonkeyPatch,
    libc: Any,
    expected: object,
) -> None:
    """Cover both symbol branches plus failure and empty-name returns."""
    monkeypatch.setattr(locate_module.ctypes, "CDLL", lambda *_a, **_k: libc)

    assert locate_module._darwin_statfs(Path("/probe")) == expected


class _FakeWindowsPath:
    """Path double carrying a Windows drive on POSIX."""

    def __init__(self, value: str, drive: str) -> None:
        """Store the rendered value and drive.

        Parameters:
            value: str returned by ``str()``.
            drive: str containing the Windows drive.
        """

        self.value = value
        self.drive = drive

    def __str__(self) -> str:
        """Render the fake path.

        Returns:
            str containing the rendered value.
        """

        return self.value


def test_windows_drive_probe_with_fake_windll(monkeypatch: pytest.MonkeyPatch) -> None:
    """Classify fixed, remote, and unknown drives through a fake windll."""
    monkeypatch.setattr(locate_module.os, "name", "nt")
    path: Any = _FakeWindowsPath("C:/data", "C:")

    monkeypatch.delattr(locate_module.ctypes, "windll", raising=False)
    assert locate_module._windows_path_is_network(path) is True

    class _Kernel32:
        def __init__(self, drive_type: int) -> None:
            self.drive_type = drive_type

        def GetDriveTypeW(self, _root: str) -> int:
            return self.drive_type

    monkeypatch.setattr(
        locate_module.ctypes,
        "windll",
        SimpleNamespace(kernel32=_Kernel32(3)),
        raising=False,
    )
    assert locate_module._windows_path_is_network(path) is False

    monkeypatch.setattr(
        locate_module.ctypes,
        "windll",
        SimpleNamespace(kernel32=_Kernel32(4)),
        raising=False,
    )
    assert locate_module._windows_path_is_network(path) is True


def test_explicit_run_directory_creation_failure_is_wrapped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the explicit base when its run directory cannot be created."""
    explicit = tmp_path / "explicit"
    explicit.mkdir()

    def fail_mkdtemp(**_kwargs: Any) -> str:
        raise OSError("disk full")

    monkeypatch.setattr(locate_module.tempfile, "mkdtemp", fail_mkdtemp)

    with pytest.raises(OSError, match="below explicit path"):
        locate_storage(
            explicit,
            mounts_text=_mount(explicit, "xfs"),
            platform_name="linux",
        )


def test_shared_memory_branches_and_candidate_dedupe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Select tmpfs shared memory, tolerate its failures, and dedupe bases."""
    shm = tmp_path / "shm"
    temp = tmp_path / "temp"
    shm.mkdir()
    temp.mkdir()
    mounts = "\n".join((_mount(shm, "tmpfs"), _mount(temp, "xfs")))
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", shm)
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(temp))

    selected = locate_storage(
        candidate=temp,
        mounts_text=mounts,
        platform_name="linux",
        minimum_shared_memory_bytes=0,
    )
    assert selected.reason is StorageReason.CALLER_TEMP

    no_candidate = locate_storage(
        mounts_text=mounts,
        platform_name="linux",
        minimum_shared_memory_bytes=0,
    )
    assert no_candidate.reason is StorageReason.SHARED_MEMORY

    monkeypatch.setattr(
        locate_module, "_free_bytes", lambda _path: (_ for _ in ()).throw(OSError())
    )
    sized_out = locate_storage(
        mounts_text=mounts,
        platform_name="linux",
        minimum_shared_memory_bytes=0,
    )
    assert sized_out.reason is StorageReason.SYSTEM_TEMP

    duplicate = locate_storage(
        candidate=temp,
        mounts_text="\n".join((_mount(shm, "nfs"), _mount(temp, "xfs"))),
        platform_name="linux",
        minimum_shared_memory_bytes=0,
    )
    assert duplicate.reason is StorageReason.CALLER_TEMP


def test_exhausted_storage_raises_after_fallback_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Warn, retry every network fallback, and finally raise."""
    base = tmp_path / "network-base"
    base.mkdir()
    mounts = _mount(tmp_path, "nfs")
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", tmp_path / "no-shm")
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(base))

    def fail_mkdtemp(**_kwargs: Any) -> str:
        raise OSError("disk full")

    monkeypatch.setattr(locate_module.tempfile, "mkdtemp", fail_mkdtemp)

    with (
        pytest.warns(StorageFallbackWarning),
        pytest.raises(OSError, match="No writable storage directory is available"),
    ):
        locate_storage(
            candidate=base,
            mounts_text=mounts,
            platform_name="linux",
        )


def test_local_candidate_creation_failure_falls_through(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip a local candidate whose run directory cannot be created."""
    candidate = tmp_path / "candidate"
    temp = tmp_path / "temp"
    candidate.mkdir()
    temp.mkdir()
    mounts = "\n".join((_mount(candidate, "xfs"), _mount(temp, "xfs")))
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", tmp_path / "no-shm")
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(temp))
    real_mkdtemp = locate_module.tempfile.mkdtemp

    def flaky_mkdtemp(**kwargs: Any) -> str:
        if Path(str(kwargs.get("dir", ""))).resolve() == candidate.resolve():
            raise OSError("candidate refused")
        return real_mkdtemp(**kwargs)

    monkeypatch.setattr(locate_module.tempfile, "mkdtemp", flaky_mkdtemp)

    selected = locate_storage(
        candidate=candidate,
        mounts_text=mounts,
        platform_name="linux",
    )

    assert selected.reason is StorageReason.SYSTEM_TEMP


def test_unc_paths_are_always_network() -> None:
    """Classify UNC paths as network before any drive inspection."""
    unc: Any = _FakeWindowsPath("//server/share", "")

    assert locate_module._windows_path_is_network(unc) is True


def test_windows_platform_inspection_uses_the_drive_probe(tmp_path: Path) -> None:
    """Route win32 classification through the Windows drive probe."""
    assert is_network_filesystem(tmp_path, mounts_text="", platform_name="win32")


def test_unknown_platform_is_conservatively_network(tmp_path: Path) -> None:
    """Assume network semantics on platforms without an inspector."""
    assert is_network_filesystem(tmp_path, mounts_text="", platform_name="sunos5")


def test_shared_memory_creation_failure_falls_through(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall to the system temp when the tmpfs run directory cannot be made."""
    shm = tmp_path / "shm"
    temp = tmp_path / "temp"
    shm.mkdir()
    temp.mkdir()
    mounts = "\n".join((_mount(shm, "tmpfs"), _mount(temp, "xfs")))
    monkeypatch.setattr(locate_module, "SHARED_MEMORY_PATH", shm)
    monkeypatch.setattr(locate_module.tempfile, "gettempdir", lambda: str(temp))
    real_mkdtemp = locate_module.tempfile.mkdtemp

    def flaky_mkdtemp(**kwargs: Any) -> str:
        if Path(str(kwargs.get("dir", ""))).resolve() == shm.resolve():
            raise OSError("tmpfs refused")
        return real_mkdtemp(**kwargs)

    monkeypatch.setattr(locate_module.tempfile, "mkdtemp", flaky_mkdtemp)

    selected = locate_storage(
        mounts_text=mounts,
        platform_name="linux",
        minimum_shared_memory_bytes=0,
    )

    assert selected.reason is StorageReason.SYSTEM_TEMP


def test_unrecognized_statfs_magic_is_conservatively_network(tmp_path: Path) -> None:
    """Assume network semantics for a magic in neither classification set."""
    assert is_network_filesystem(
        tmp_path,
        mounts_text="",
        platform_name="linux",
        statfs_reader=lambda _path: 0x0BADF00D,
    )


def test_failed_statfs_probe_is_conservatively_network(tmp_path: Path) -> None:
    """Assume network semantics when the Linux probe returns nothing."""
    assert is_network_filesystem(
        tmp_path,
        mounts_text="",
        platform_name="linux",
        statfs_reader=_unknown_statfs,
    )
