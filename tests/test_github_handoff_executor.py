import json
import hashlib
import subprocess
import time
from dataclasses import asdict

import pytest

from auto_mindsdb_factory.github_handoff import HandoffError, PublicationScope, digest
from auto_mindsdb_factory.github_handoff_executor import execute_request, import_candidate
from auto_mindsdb_factory.product_workspace import git


@pytest.fixture
def case(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-b", "main")
    git(root, "config", "user.name", "Test")
    git(root, "config", "user.email", "test@example.com")
    (root / "app").write_text("base")
    git(root, "add", "app")
    git(root, "commit", "-m", "base")
    base = git(root, "rev-parse", "HEAD")
    (root / "app").write_text("fixed")
    git(root, "commit", "-am", "fix")
    sha = git(root, "rev-parse", "HEAD")
    git(root, "remote", "add", "origin", "https://github.com/owner/repo.git")
    scope = PublicationScope("owner/repo", "task", base, sha, "main")
    request = {"version": 1, "id": "c" * 32, "scope": asdict(scope), "expires": time.time() + 60,
               "operation": "api", "arguments": {"path": "pulls/1", "body": None}}
    return scope, request, root, tmp_path / "receipt.json"


def test_executor_uses_only_normal_gh_auth_and_retains_immutable_intent(case):
    scope, request, root, receipt = case
    calls = []
    def runner(command, **options):
        assert receipt.with_name(receipt.name + ".intent").is_file()
        assert command == ["gh", "api", "--hostname", "github.com", "repos/owner/repo/pulls/1"]
        assert options["input"] is None and 0 < options["timeout"] <= 60
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, '{"number":1}', '')
    result = execute_request(scope, request, root, receipt, runner=runner)
    assert result == {"request_sha256": digest(request), "ok": True, "result": {"number": 1}}
    with pytest.raises(FileExistsError):
        execute_request(scope, request, root, receipt, runner=runner)
    assert len(calls) == 1 and git(root, "status", "--porcelain") == ""


def test_unknown_result_is_retained_without_retry_or_secret_stderr(case):
    scope, request, root, receipt = case
    def runner(command, **options):
        raise subprocess.TimeoutExpired(command, 1, stderr="credential-like-private-value")
    with pytest.raises(HandoffError, match="inspect remote") as error:
        execute_request(scope, request, root, receipt, runner=runner)
    assert "private-value" not in str(error.value)
    assert json.loads(receipt.read_text()) == {"request_sha256": digest(request), "ok": False, "result": None}
    with pytest.raises(FileExistsError):
        execute_request(scope, request, root, receipt, runner=runner)


@pytest.mark.parametrize("mutation", ["repo", "pushurl", "base", "expired", "receipt"])
def test_wrong_identity_base_expiry_or_candidate_receipt_cannot_reach_network(case, mutation):
    scope, request, root, receipt = case
    if mutation == "repo":
        git(root, "remote", "set-url", "origin", "https://github.com/other/repo.git")
    elif mutation == "pushurl":
        git(root, "config", "remote.origin.pushurl", "https://github.com/other/repo.git")
    elif mutation == "base":
        scope = PublicationScope(scope.repository, scope.run_id, "a" * 40, scope.candidate_sha, "main")
        request["scope"] = asdict(scope)
    elif mutation == "expired":
        request["expires"] = time.time() - 1
    else:
        receipt = root / "receipt.json"
    def forbidden(*args, **kwargs):
        pytest.fail("Invalid handoff reached network")
    with pytest.raises(HandoffError):
        execute_request(scope, request, root, receipt, runner=forbidden)


def test_exact_incremental_bundle_import_preserves_operator_checkout(case, tmp_path):
    scope, _, root, _ = case
    ref = "refs/factory/task/attempt-1"
    git(root, "update-ref", ref, scope.candidate_sha)
    bundle = tmp_path / "candidate.bundle"
    git(root, "bundle", "create", str(bundle), ref, "^" + scope.base_sha)
    fingerprint = hashlib.sha256(bundle.read_bytes()).hexdigest()
    git(root, "checkout", "--detach", scope.base_sha)
    import_candidate(scope, root, bundle, fingerprint)
    assert git(root, "rev-parse", "HEAD") == scope.base_sha
    assert (root / "app").read_text() == "base"
    assert git(root, "rev-parse", "FETCH_HEAD") == scope.candidate_sha
    with pytest.raises(HandoffError, match="checksum"):
        import_candidate(scope, root, bundle, "0" * 64)
    other = PublicationScope(scope.repository, "other-task", scope.base_sha, scope.candidate_sha, "main")
    with pytest.raises(HandoffError, match="scope"):
        import_candidate(other, root, bundle, fingerprint)
