from dataclasses import asdict

import pytest

from auto_mindsdb_factory.github_handoff import HandoffError, PublicationScope, digest


@pytest.fixture
def scope():
    return PublicationScope("owner/product", "run-1", "a" * 40, "b" * 40, "main")


def test_publication_scope_permits_only_exact_candidate_ref_operations(scope):
    scope.validate("git", ["ls-remote", "--heads", "origin", "refs/heads/main"])
    scope.validate("git", ["push", "--force-with-lease=refs/heads/main:" + "a" * 40,
                           "origin", "b" * 40 + ":refs/heads/main"])
    scope.validate("git", ["push", "--force-with-lease=refs/heads/codex/factory-run-1:",
                           "origin", "b" * 40 + ":refs/heads/codex/factory-run-1"])


@pytest.mark.parametrize("arguments", [
    ["push", "--force", "origin", "HEAD:main"], ["fetch", "origin"],
    ["ls-remote", "--heads", "https://other/repo", "refs/heads/main"],
    ["push", "--force-with-lease=refs/heads/main:", "origin", "b" * 40 + ":refs/heads/main"],
    ["push", "--force-with-lease=refs/heads/main:" + "a" * 40, "origin", "c" * 40 + ":refs/heads/main"],
])
def test_publication_rejects_other_references_credentials_or_commands(scope, arguments):
    with pytest.raises(HandoffError):
        scope.validate("git", arguments)


@pytest.mark.parametrize("path,body", [
    ("pulls/1", None), ("commits/" + "b" * 40 + "/status?per_page=100", None),
    ("pulls", {"title": "Fix", "body": "Verified fix", "base": "main", "head": "codex/factory-run-1"}),
    ("statuses/" + "b" * 40, {"state": "success", "context": "ai-factory/verification",
        "description": "Independent executable checks and review passed", "target_url": "https://github.com/owner/product/pull/1"}),
])
def test_publication_permits_required_reads_pr_and_actual_verification_status(scope, path, body):
    scope.validate("api", {"path": path, "body": body})


@pytest.mark.parametrize("path,body", [
    ("actions/permissions", None), ("pulls/1/merge", {}),
    ("commits/" + "a" * 40 + "/status?per_page=100", None),
    ("pulls", {"title": "Fix", "body": "Fix", "base": "other", "head": "codex/factory-run-1"}),
    ("statuses/" + "b" * 40, {"state": "success", "context": "validate"}),
])
def test_publication_cannot_change_access_merge_via_api_or_forge_ci(scope, path, body):
    with pytest.raises(HandoffError):
        scope.validate("api", {"path": path, "body": body})


def test_request_requires_exact_scope_nonce_expiry_and_canonical_digest(scope):
    request = {"version": 1, "id": "c" * 32, "scope": asdict(scope), "expires": 101,
               "operation": "api", "arguments": {"path": "pulls/1", "body": None}}
    scope.validate_request(request, now=100)
    assert digest(request) == digest(dict(reversed(list(request.items()))))
    for change in ({"expires": 100}, {"expires": float("nan")}, {"expires": 3701},
                   {"scope": {**asdict(scope), "repository": "other/repo"}}, {"id": "../bad"}, {"extra": 1}):
        with pytest.raises(HandoffError):
            scope.validate_request({**request, **change}, now=100)


@pytest.mark.parametrize("field,value", [("repository", "../repo"), ("run_id", "../x"),
    ("base_sha", "main"), ("candidate_sha", "a" * 40), ("base_branch", "main:other")])
def test_invalid_operator_scope_is_rejected(scope, field, value):
    with pytest.raises(HandoffError):
        PublicationScope(**{**asdict(scope), field: value})
