import json
import subprocess
import threading
import time

import pytest

from test_product_promotion import promotion_case
from auto_mindsdb_factory import github_handoff_executor as executor
from auto_mindsdb_factory.github_handoff_channel import GitHubHandoff, read_document, write_exclusive
from auto_mindsdb_factory.product_promotion import GitHubPromotion, PromotionError
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_workspace import git


@pytest.mark.parametrize("ci_success", [False, True])
def test_complete_handoff_keeps_ci_cas_merge_confirmation_and_smoke_gates(promotion_case, tmp_path, monkeypatch, ci_success):
    original, candidate, journal, remote, _, _, smoked = promotion_case
    profile = ProductProfile.from_document({**original.settings, "required_status_checks": ["validate", "sandbox"]})
    candidate["profile_sha"] = profile.digest
    git(original.target, "update-ref", "refs/factory/task/attempt-1", candidate["candidate_sha"])
    transport = GitHubHandoff(profile, original.task, original.target,
                             deadline=time.monotonic() + 10, check_cancelled=lambda: None)
    promotion = GitHubPromotion(profile, original.task, original.target, original.store,
        smoke=original.smoke, deadline=transport.deadline, transport=transport)
    results, errors, requests = [], [], []
    def promote():
        try:
            results.append(promotion(candidate, journal))
        except Exception as exc:
            errors.append(exc)
    def local_git(root, *args):
        if args[:2] == ("remote", "get-url"):
            return "https://github.com/owner/product.git"
        return git(root, *args)
    monkeypatch.setattr(executor, "git", local_git)
    def network(command, **options):
        if command[0] == "git":
            return subprocess.run(command, **options)
        path = command[4].removeprefix("repos/owner/product/")
        body = json.loads(options["input"]) if options["input"] is not None else None
        if "/check-runs?" in path:
            result = {"check_runs": [{"name": n, "status": "completed",
                "conclusion": "success" if ci_success else "failure"} for n in ("validate", "sandbox")]}
        else:
            result = original.api(path, body)
        return subprocess.CompletedProcess(command, 0, json.dumps(result), "")
    thread = threading.Thread(target=promote)
    thread.start()
    try:
        while thread.is_alive():
            root = transport.root
            if root is not None:
                for path in sorted(root.glob("*.request.json")):
                    if path.name in requests:
                        continue
                    requests.append(path.name)
                    request = read_document(path)
                    receipt = tmp_path / (request["id"] + ".response.json")
                    response = executor.execute_request(transport.scope, request, original.target, receipt, runner=network)
                    write_exclusive(root / receipt.name, response)
            time.sleep(0.01)
    finally:
        thread.join(timeout=11)
    assert not thread.is_alive()
    events = [event["kind"] for event in journal.events()]
    assert events.count("publication_transport_requested") == len(requests)
    assert events.count("publication_transport_completed") == len(requests)
    if ci_success:
        assert not errors and results[0]["status"] == "complete"
        assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
        assert events[-1] == "merge_confirmed" and smoked == [candidate["candidate_sha"]]
    else:
        assert not results and len(errors) == 1 and isinstance(errors[0], PromotionError)
        assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
        assert "promotion_requested" not in events and smoked == []
