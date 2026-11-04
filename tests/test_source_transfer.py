import base64
import pytest

from auto_mindsdb_factory.product_workspace import WorkspaceError, source_digest, source_files, executable_paths
from auto_mindsdb_factory.source_transfer import pack_source, unpack_source, write_source


def entry(name, content=b"x", executable=False):
    return [name, base64.b64encode(content).decode(), executable]


def test_roundtrip_preserves_bytes_modes_deletions_and_excludes_environments(tmp_path):
    original, target = tmp_path / "original", tmp_path / "target"
    original.mkdir()
    (original / "run").write_bytes(b"#!/bin/sh\nexit 0\n")
    (original / "run").chmod(0o755)
    (original / "data").write_bytes(b"\0\xff")
    (original / ".venv").mkdir()
    (original / ".venv" / "secret").symlink_to("/etc/passwd")
    target.mkdir()
    (target / "deleted").write_text("old")
    write_source(target, pack_source(original))
    assert not (target / "deleted").exists()
    assert not (target / ".venv").exists()
    before, after = source_files(original), source_files(target)
    assert source_digest(before, executable_paths(original, before)) == source_digest(after, executable_paths(target, after))


@pytest.mark.parametrize("document", [
    None, {}, [["x"]], [entry("../escape")], [entry("/absolute")],
    [entry("a/../b")], [entry("a//b")], [entry("./a")], [entry("")], [entry(".")],
    [entry("a\\b")], [entry("a\0b")], [entry(".git/config")],
    [entry(".GIT./config")], [entry("x/.venv/payload")],
    [entry("a"), entry("a")], [entry("a"), entry("a/b")],
    [["a", "not base64", False]], [["a", "eA==", 1]],
])
def test_rejects_invalid_transport_before_changing_destination(tmp_path, document):
    (tmp_path / "keep").write_text("original")
    with pytest.raises(WorkspaceError):
        write_source(tmp_path, document)
    assert (tmp_path / "keep").read_text() == "original"


def test_worker_link_cannot_cross_boundary(tmp_path):
    (tmp_path / "escape").symlink_to("/etc/passwd")
    with pytest.raises(WorkspaceError):
        pack_source(tmp_path)


def test_transport_file_limit():
    with pytest.raises(WorkspaceError):
        unpack_source([entry("large", b"x" * (4 * 1024 * 1024 + 1))])


def test_existing_link_destination_is_rejected(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    target = tmp_path / "target"
    target.symlink_to(outside, target_is_directory=True)
    with pytest.raises(WorkspaceError):
        write_source(target, [entry("bad")])
    assert not list(outside.iterdir())
