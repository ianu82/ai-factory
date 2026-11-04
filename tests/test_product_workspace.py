import subprocess

import pytest

from auto_mindsdb_factory.product_workspace import WorkspaceError, export_source, is_git_metadata_path, source_files, source_digest


@pytest.mark.parametrize("name", [".GiT", ".git.", ".git ", ".g\u200cit", ".g\u0301it", ".git:stream"])
def test_metadata_aliases_cannot_enter_a_trusted_checkout(tmp_path, name):
    assert is_git_metadata_path("nested/" + name + "/config")
    (tmp_path / name).write_text("gitdir: /outside")
    with pytest.raises(WorkspaceError, match="Git metadata"):
        source_files(tmp_path)
    assert not is_git_metadata_path(".gitignore")
    assert not is_git_metadata_path(".gitattributes")


def test_export_uses_commit_without_git_metadata_or_uncommitted_changes(tmp_path):
    repo, out = tmp_path / "repo", tmp_path / "out"
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()
    git("init", "-q")
    git("config", "user.name", "test")
    git("config", "user.email", "test@example.com")
    (repo / "app.py").write_text("original")
    git("add", ".")
    git("commit", "-qm", "initial")
    sha = git("rev-parse", "HEAD")
    (repo / "app.py").write_text("uncommitted")
    export_source(repo, sha, out)
    assert source_files(out) == {"app.py": b"original"}
    assert not (out / ".git").exists()
    assert source_digest(source_files(out)) != source_digest({"app.py": b"changed"})


def test_source_import_rejects_links_and_git_metadata(tmp_path):
    (tmp_path / "link").symlink_to("/etc/passwd")
    with pytest.raises(WorkspaceError):
        source_files(tmp_path)
    (tmp_path / "link").unlink()
    (tmp_path / ".git").write_text("gitdir: /somewhere")
    with pytest.raises(WorkspaceError):
        source_files(tmp_path)


def test_export_ignores_archive_attributes_and_rejects_submodules(tmp_path):
    repo, out = tmp_path / "repo", tmp_path / "out"
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()
    git("init", "-q")
    git("config", "user.name", "test")
    git("config", "user.email", "test@example.com")
    (repo / ".gitattributes").write_text("hidden.txt export-ignore\nversion.txt export-subst\n")
    (repo / "hidden.txt").write_text("must be inspected")
    (repo / "version.txt").write_text("$Format:%H$")
    git("add", ".")
    git("commit", "-qm", "base")
    sha = git("rev-parse", "HEAD")
    export_source(repo, sha, out)
    assert (out / "hidden.txt").read_text() == "must be inspected"
    assert (out / "version.txt").read_text() == "$Format:%H$"
    git("update-index", "--add", "--cacheinfo", "160000," + sha + ",vendor")
    git("commit", "-qm", "submodule")
    with pytest.raises(WorkspaceError, match="submodule"):
        export_source(repo, git("rev-parse", "HEAD"), tmp_path / "unsupported")
