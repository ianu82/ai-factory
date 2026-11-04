import subprocess
import time

import pytest

from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.product_promotion import GitHubPromotion, PromotionError
from auto_mindsdb_factory.product_workspace import git
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.mark.parametrize("state,required,passes", [("success", ["ci"], True), ("pending", ["ci"], False),
    ("skipped", ["ci"], False), ("skipped", [], True)])
def test_required_checks_need_actual_success(state, required, passes):
    checks = {"check_runs": [{"name": "ci", "status": "completed", "conclusion": state}]}
    assert GitHubPromotion.checks_passed({}, checks, required) == passes


def test_missing_or_duplicate_failed_checks_cannot_pass():
    assert not GitHubPromotion.checks_passed({}, {}, ["ci"])
    with pytest.raises(PromotionError):
        GitHubPromotion.checks_passed({}, {"check_runs": [
            {"name": "ci", "status": "completed", "conclusion": "neutral"},
            {"name": "ci", "status": "completed", "conclusion": "failure"}]}, [])
    with pytest.raises(PromotionError):
        GitHubPromotion.checks_passed({}, {"total_count": 101}, [])


@pytest.fixture
def promotion_case(tmp_path, monkeypatch):
    root, remote = tmp_path / "product", tmp_path / "remote.git"
    root.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(root)], check=True)
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    git(root, "config", "user.name", "test")
    git(root, "config", "user.email", "test@example.com")
    git(root, "remote", "add", "origin", str(remote))
    (root / "app.js").write_text("module.exports=1;\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "base")
    base = git(root, "rev-parse", "HEAD")
    git(root, "push", "origin", "main")
    (root / "app.js").write_text("module.exports=2;\n")
    git(root, "commit", "-qam", "candidate")
    sha = git(root, "rev-parse", "HEAD")
    tree = git(root, "rev-parse", base + "^{tree}")
    other = subprocess.check_output(["git", "-C", str(root), "commit-tree", tree, "-p", base], input="concurrent\n", text=True).strip()
    git(root, "push", "origin", f"{other}:refs/heads/concurrent")
    profile = ProductProfile.from_document({"version": 1, "name": "product", "repository": "owner/product",
        "image": "node@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]], "auto_merge": True})
    task = ProductTask.from_document({"id": "task", "title": "Fix", "goal": "fix", "acceptance_criteria": [
        {"id": "works", "description": "works", "command": ["true"]}]})
    candidate = {"run_id": "task", "candidate_sha": sha, "base_sha": base, "profile_sha": profile.digest,
        "task_sha": task.digest, "changed_paths": ["app.js"], "verification": {"commit_sha": sha,
        "checks": [{"name": n, "command": ["true"], "exit_code": 0} for n in ["unit", "acceptance:works"]],
        "report": {"status": "passed", "findings": [], "criteria": [{"id": "works", "status": "passed", "evidence": "checked"}]}}}
    # Transport tests use a local bare remote; identity validation has separate tests.
    monkeypatch.setattr(ProductProfile, "validate_repository", lambda *args: None)
    pr = {"number": 1, "html_url": "https://github.com/owner/product/pull/1", "head": {"sha": sha}, "base": {"ref": "main"}, "state": "open"}
    def api(path, body=None):
        if path.startswith("pulls?"):
            return []
        if path == "pulls":
            return pr
        if path == "pulls/1":
            merged = git(remote, "rev-parse", "refs/heads/main") == sha
            return {**pr, "merged": merged, "merge_commit_sha": sha if merged else None}
        return {}
    smoked = []
    promotion = GitHubPromotion(profile, task, root, tmp_path / "store", deadline=time.monotonic() + 10,
        smoke=lambda sha, journal: smoked.append(sha), api=api)
    journal = RunJournal(tmp_path / "store", "task")
    journal.create({})
    return promotion, candidate, journal, remote, other, pr, smoked


def test_promotion_confirms_merge_before_smoke(promotion_case):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    result = promotion(candidate, journal)
    assert result["status"] == "complete"
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    assert smoked == [candidate["candidate_sha"]]
    assert journal.events()[-1]["kind"] == "merge_confirmed"


@pytest.mark.parametrize("variant", ["stale-base", "stale-head", "failed-evidence", "wrong-task", "race"])
def test_stale_or_unverified_candidates_never_promote(promotion_case, variant, monkeypatch):
    promotion, candidate, journal, remote, other, pr, smoked = promotion_case
    if variant == "stale-base":
        git(remote, "update-ref", "refs/heads/main", other)
    elif variant == "stale-head":
        pr["head"]["sha"] = other
    elif variant == "failed-evidence":
        candidate["verification"]["checks"][0]["exit_code"] = 1
    elif variant == "wrong-task":
        candidate["task_sha"] = "invalid"
    elif variant == "race":
        def racing_git(root, *args):
            if args[0] == "push" and args[-1].endswith(":refs/heads/main"):
                git(remote, "update-ref", "refs/heads/main", other)
            return git(root, *args)
        monkeypatch.setattr("auto_mindsdb_factory.product_promotion.git", racing_git)
    with pytest.raises(PromotionError):
        promotion(candidate, journal)
    expected = other if variant in {"stale-base", "race"} else candidate["base_sha"]
    assert git(remote, "rev-parse", "refs/heads/main") == expected
    assert not smoked


def test_failed_post_merge_smoke_does_not_report_completion(promotion_case):
    promotion, candidate, journal, remote, _, _, _ = promotion_case
    def failed_smoke(*args):
        raise RuntimeError("smoke failed")
    promotion.smoke = failed_smoke
    with pytest.raises(RuntimeError, match="smoke"):
        promotion(candidate, journal)
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    assert not any(e["kind"] == "run_finished" and e["data"].get("status") == "complete" for e in journal.events())


def test_before_merge_after_ci_rejects_without_touching_main(promotion_case, monkeypatch):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    events = []
    original_api = promotion.api
    def api(path, body=None):
        if path.startswith("commits/"):
            events.append("ci")
            if "check-runs" in path:
                return {"check_runs": [{"name": "ci", "status": "completed", "conclusion": "success"}]}
            return {}
        if path == "pulls/1":
            events.append("recheck")
        return original_api(path, body)
    promotion.api = api
    promotion.settings["required_status_checks"] = ["ci"]
    def reject():
        events.append("guard")
        assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
        raise RuntimeError("canceled")
    promotion.before_merge = reject
    with pytest.raises(RuntimeError, match="canceled"):
        promotion(candidate, journal)
    assert events == ["ci", "ci", "guard"]
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked
    assert not any(e["kind"] == "promotion_requested" for e in journal.events())


def test_before_merge_success_still_rechecks_pr(promotion_case):
    promotion, candidate, journal, remote, other, pr, smoked = promotion_case
    calls = []
    def guard():
        calls.append("guard")
        pr["head"]["sha"] = other
    promotion.before_merge = guard
    with pytest.raises(PromotionError, match="Pull request changed"):
        promotion(candidate, journal)
    assert calls == ["guard"]
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_before_merge_not_called_for_pr_only(promotion_case):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    promotion.settings["auto_merge"] = False
    def guard():
        raise AssertionError("guard should not run")
    promotion.before_merge = guard
    assert promotion(candidate, journal)["status"] == "pr_ready"
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_constructor_guard_runs_inside_lease_after_pending_ci(promotion_case, monkeypatch):
    from contextlib import contextmanager
    from auto_mindsdb_factory.automation import FactoryRunStore

    original, candidate, journal, remote, _, _, smoked = promotion_case
    events = []
    real_lease = FactoryRunStore.run_lease
    @contextmanager
    def lease(store, *args, **kwargs):
        with real_lease(store, *args, **kwargs) as held:
            events.append("lease-enter")
            yield held
            events.append("lease-exit")
    monkeypatch.setattr(FactoryRunStore, "run_lease", lease)
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: None)
    attempts = 0
    def api(path, body=None):
        nonlocal attempts
        if "check-runs" in path:
            attempts += 1
            events.append("ci-pending" if attempts == 1 else "ci-success")
            return {"check_runs": [{"name": "ci", "status": "in_progress" if attempts == 1 else "completed",
                                    "conclusion": None if attempts == 1 else "success"}]}
        if path == "pulls/1":
            events.append("pr-recheck")
        return original.api(path, body)
    def guard():
        events.append("guard")
    promotion = GitHubPromotion(original.profile, original.task, original.target, original.store,
        smoke=original.smoke, deadline=original.deadline, api=api, before_merge=guard)
    promotion.settings["required_status_checks"] = ["ci"]
    assert promotion(candidate, journal)["status"] == "complete"
    assert events[:5] == ["lease-enter", "ci-pending", "ci-success", "guard", "pr-recheck"]
    assert events[-1] == "lease-exit"
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    assert smoked == [candidate["candidate_sha"]]
