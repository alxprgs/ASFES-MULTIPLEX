from __future__ import annotations

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from server.core.config import Settings
from server.host_ops import HostOpsService, HostOpsError
from server.models import ToolExecutionContext
from server.mcp.plugins.file_manager import (
    list_directory,
    read_file,
    write_file,
    append_file,
    move_path,
    delete_path,
    make_directory,
)


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        root1 = tmp_path / "root1"
        root1.mkdir()
        root2 = tmp_path / "root2"
        root2.mkdir()
        backup = tmp_path / "backup"
        backup.mkdir()

        # Write some sample files
        (root1 / "file1.txt").write_text("Hello World from root1", encoding="utf-8")
        (root2 / "file2.txt").write_text("Hello World from root2", encoding="utf-8")

        yield {
            "root1": root1,
            "root2": root2,
            "backup": backup,
            "tmpdir": tmp_path,
        }


@pytest.fixture
def host_ops(temp_workspace):
    s = Settings()
    s.host_ops.managed_file_roots = [
        temp_workspace["root1"],
        temp_workspace["root2"],
    ]
    s.host_ops.backup_directory = temp_workspace["backup"]
    s.host_ops.database_profiles_directory = temp_workspace["tmpdir"] / "db_profiles"
    s.host_ops.vpn_profiles_directory = temp_workspace["tmpdir"] / "vpn_profiles"
    s.host_ops.ssl_profiles_directory = temp_workspace["tmpdir"] / "ssl_profiles"
    return HostOpsService(s)


def test_host_ops_resolve_managed_path(host_ops, temp_workspace) -> None:
    # 1. Resolve relative path inside root1
    res1 = host_ops.resolve_managed_path("file1.txt")
    assert res1 == temp_workspace["root1"] / "file1.txt"

    # 2. Resolve relative path inside root2 (scans both roots)
    res2 = host_ops.resolve_managed_path("file2.txt")
    assert res2 == temp_workspace["root2"] / "file2.txt"

    # 3. Path outside managed roots raises error
    with pytest.raises(HostOpsError) as exc:
        host_ops.resolve_managed_path("../file.txt")
    assert "escapes managed roots" in str(exc.value)

    # 4. Empty roots configuration
    host_ops.config.managed_file_roots = []
    with pytest.raises(HostOpsError) as exc:
        host_ops.resolve_managed_path("file.txt")
    assert "No managed roots" in str(exc.value)


def test_host_ops_list_directory(host_ops, temp_workspace) -> None:
    res = host_ops.list_directory(".")
    assert res["count"] == 1
    assert res["entries"][0]["name"] == "file1.txt"

    # Directory does not exist
    with pytest.raises(HostOpsError) as exc:
        host_ops.list_directory("non_existent_dir")
    assert "Directory does not exist" in str(exc.value)

    # Path is not a directory
    with pytest.raises(HostOpsError) as exc:
        host_ops.list_directory("file1.txt")
    assert "Target path is not a directory" in str(exc.value)


def test_host_ops_read_and_write(host_ops, temp_workspace) -> None:
    # 1. Read existing file
    read_res = host_ops.read_text("file1.txt")
    assert read_res["content"] == "Hello World from root1"
    assert read_res["truncated"] is False

    # Read with offset and limit
    read_res_limit = host_ops.read_text("file1.txt", offset=6, max_bytes=5)
    assert read_res_limit["content"] == "World"
    assert read_res_limit["truncated"] is True

    # 2. Atomic write (new file)
    write_res = host_ops.atomic_write_text(
        "new_file.txt", "New Content", backup_existing=False
    )
    assert write_res["written"] == 11
    assert (temp_workspace["root1"] / "new_file.txt").read_text(
        encoding="utf-8"
    ) == "New Content"

    # 3. Append file
    append_res = host_ops.atomic_write_text("new_file.txt", " Added", append=True)
    assert append_res["appended"] is True
    assert (temp_workspace["root1"] / "new_file.txt").read_text(
        encoding="utf-8"
    ) == "New Content Added"


def test_host_ops_tail_mkdir_move_delete(host_ops, temp_workspace) -> None:
    # 1. Tail file
    tail_res = host_ops.tail_text("file1.txt", tail_lines=2)
    assert tail_res["content"] == "Hello World from root1"

    # 2. Mkdir
    mkdir_res = host_ops.mkdir("new_subdir")
    assert mkdir_res["created"] is True
    assert (temp_workspace["root1"] / "new_subdir").is_dir()

    # 3. Move path
    move_res = host_ops.move_path("file1.txt", "new_subdir/moved.txt")
    assert move_res["moved"] is True
    assert not (temp_workspace["root1"] / "file1.txt").exists()
    assert (temp_workspace["root1"] / "new_subdir" / "moved.txt").exists()

    # 4. Delete path
    # Non-recursive on dir raises error
    with pytest.raises(HostOpsError) as exc:
        host_ops.delete_path("new_subdir", recursive=False)
    assert "Refusing to delete a directory" in str(exc.value)

    # Recursive delete works
    del_res = host_ops.delete_path("new_subdir", recursive=True)
    assert del_res["deleted"] is True
    assert not (temp_workspace["root1"] / "new_subdir").exists()


@pytest.mark.asyncio
async def test_file_manager_plugin_handlers(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    services.settings = host_ops.settings
    context = ToolExecutionContext(
        user=MagicMock(),
        services=services,
        request_meta={},
    )

    # 1. list_directory
    list_res = await list_directory(context, {"path": "."})
    assert "entries" in list_res

    # 2. read_file
    read_res = await read_file(
        context, {"path": "file2.txt", "offset": 0, "max_bytes": 100}
    )
    assert read_res["content"] == "Hello World from root2"

    # 3. write_file
    write_res = await write_file(
        context, {"path": "file3.txt", "content": "File 3 data"}
    )
    assert write_res["written"] == 11

    # 4. append_file
    append_res = await append_file(context, {"path": "file3.txt", "content": " Append"})
    assert append_res["appended"] is True

    # 5. make_directory
    mkdir_res = await make_directory(context, {"path": "subdir"})
    assert mkdir_res["created"] is True

    # 6. move_path
    move_res = await move_path(
        context, {"source": "file3.txt", "destination": "subdir/file3_moved.txt"}
    )
    assert move_res["moved"] is True

    # 7. delete_path
    del_res = await delete_path(context, {"path": "subdir", "recursive": True})
    assert del_res["deleted"] is True
