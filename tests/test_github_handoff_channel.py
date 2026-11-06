import hashlib
import threading
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.github_handoff import HandoffError, PublicationScope, digest
from auto_mindsdb_factory.github_handoff_channel import GitHubHandoff, read_document, write_exclusive
from auto_mindsdb_factory.run_journal import RunJournal


def channel(tmp_path):
    value = GitHubHandoff(None, None, None, deadline=time.monotonic() + 2, check_cancelled=lambda: None)
    value.scope = PublicationScope("owner/repo", "task", "a" * 40, "b" * 40, "main")
    value.root = tmp_path / "channel"
    value.root.mkdir(mode=0o700)
    value.journal = RunJournal(tmp_path / "store", "task")
    value.journal.create({})
    return value


def test_append_only_roundtrip_preserves_bound_request_and_receipt(tmp_path):
    value = channel(tmp_path)
    result = []
    thread = threading.Thread(target=lambda: result.append(value.api("pulls/1")))
    thread.start()
    try:
        while not (requests := list(value.root.glob("*.request.json"))):
            time.sleep(0.01)
        request = read_document(requests[0])
        value.scope.validate_request(request)
        write_exclusive(value.root / (request["id"] + ".response.json"),
                        {"request_sha256": digest(request), "ok": True, "result": {"number": 1}})
    finally:
        thread.join(timeout=3)
    assert not thread.is_alive() and result == [{"number": 1}]
    assert [e["kind"] for e in value.journal.events()] == ["publication_transport_requested", "publication_transport_completed"]
    with pytest.raises(FileExistsError):
        write_exclusive(requests[0], {})


@pytest.mark.parametrize("response", [
    {"request_sha256": "wrong", "ok": True, "result": {}},
    {"request_sha256": "correct", "ok": False, "result": None},
])
def test_ambiguous_or_mismatched_reply_is_terminal_without_new_request(tmp_path, response, monkeypatch):
    value = channel(tmp_path)
    def reply(_):
        request = read_document(next(value.root.glob("*.request.json")))
        body = {**response, "request_sha256": digest(request) if response["request_sha256"] == "correct" else "wrong"}
        write_exclusive(value.root / (request["id"] + ".response.json"), body)
    monkeypatch.setattr("auto_mindsdb_factory.github_handoff_channel.time.sleep", reply)
    with pytest.raises(HandoffError, match="uncertain"):
        value.api("pulls/1")
    assert len(list(value.root.glob("*.request.json"))) == 1


def test_expired_unbound_or_cancelled_channel_cannot_issue_requests(tmp_path):
    value = channel(tmp_path)
    value.deadline = time.monotonic() - 1
    with pytest.raises(HandoffError, match="deadline"):
        value.api("pulls/1")
    value.deadline = time.monotonic() + 2
    def cancel():
        raise HandoffError("cancelled")
    value.check_cancelled = cancel
    with pytest.raises(HandoffError, match="cancelled"):
        value.api("pulls/1")
    value.scope = None
    with pytest.raises(HandoffError, match="not authorized"):
        value.api("pulls/1")
    assert list(value.root.iterdir()) == []


def test_linked_response_is_not_followed(tmp_path):
    target = tmp_path / "target"
    target.write_text('{}')
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(HandoffError):
        read_document(link)


def test_binding_exports_only_exact_candidate_objects_and_cannot_resume(tmp_path):
    from auto_mindsdb_factory.product_workspace import git
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-b", "main")
    git(root, "config", "user.name", "Test")
    git(root, "config", "user.email", "test@example.com")
    (root / "app").write_text("original")
    git(root, "add", "app")
    git(root, "commit", "-m", "base")
    base = git(root, "rev-parse", "HEAD")
    (root / "app").write_text("fixed")
    git(root, "commit", "-am", "fix")
    head = git(root, "rev-parse", "HEAD")
    git(root, "update-ref", "refs/factory/task/attempt-1", head)
    journal = RunJournal(tmp_path / "store", "task")
    journal.create({})
    value = GitHubHandoff(SimpleNamespace(document={"repository": "owner/repo", "base_branch": "main"}),
        SimpleNamespace(document={"id": "task"}), root, deadline=time.monotonic() + 2, check_cancelled=lambda: None)
    value.bind({"base_sha": base, "candidate_sha": head}, journal)
    bundle = value.root / "candidate.bundle"
    assert read_document(value.root / "scope.json")["bundle_sha256"] == hashlib.sha256(bundle.read_bytes()).hexdigest()
    assert git(root, "bundle", "list-heads", str(bundle)) == head + " refs/factory/task/attempt-1"
    verified = git(root, "bundle", "verify", str(bundle))
    assert head in verified and base in verified
    with pytest.raises(HandoffError, match="rebound"):
        value.bind({"base_sha": base, "candidate_sha": head}, journal)
