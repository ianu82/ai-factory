import subprocess
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.product_promotion import GitHubPromotion, PromotionError
from auto_mindsdb_factory.product_workspace import git
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.fixture(autouse=True)
def isolated_promotion_clock(monkeypatch):
    # Mock retry timing only in the component under test. subprocess uses the
    # real time module while waiting for Git; altering it can expire a promotion
    # or report a retry before any HTTP request has even run.
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time",
                        SimpleNamespace(monotonic=time.monotonic, sleep=time.sleep))


def test_retry_clock_does_not_change_subprocess_deadline_waits(monkeypatch):
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep",
                        lambda _: pytest.fail("unexpected retry"))
    subprocess.run(["git", "-c", "alias.wait=!sleep 0.05", "wait"], check=True, timeout=2,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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
            assert not any(e["kind"] == "github_checks_passed" for e in journal.events())
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


def request_failures(journal):
    # Reopen the journal to assert evidence was persisted, not just kept in memory.
    return [event["data"] for event in RunJournal(journal.root.parent.parent, "task").events()
            if event["kind"] == "github_request_failed"]


@pytest.mark.parametrize("phase", ["pull-list", "status", "check-runs", "pr-recheck", "merge-confirmation"])
def test_two_read_failures_recover_without_repeating_publication(promotion_case, monkeypatch, phase):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: None)
    original_api = promotion.api
    calls = []
    failures = []
    guard_seen = []

    def api(path, body=None):
        calls.append((path, body))
        merged = git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
        match = ((phase == "pull-list" and path.startswith("pulls?"))
                 or (phase == "status" and "/status?" in path)
                 or (phase == "check-runs" and "/check-runs?" in path)
                 or (phase == "pr-recheck" and path == "pulls/1" and not merged)
                 or (phase == "merge-confirmation" and path == "pulls/1" and merged))
        if match and len(failures) < 2:
            failures.append(path)
            raise PromotionError("provider secret: token-in-stdout")
        return original_api(path, body)

    def guard():
        evidence = journal.events()
        guard_seen.append([e for e in evidence if e["kind"] == "github_checks_passed"])
        assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]

    promotion.api = api
    promotion.before_merge = guard
    assert promotion(candidate, journal)["status"] == "complete"
    assert len(failures) == 2 and failures[0] == failures[1]
    assert request_failures(journal) == [
        {"path": failures[0], "read_only": True, "attempt": i, "error_type": "PromotionError"}
        for i in (1, 2)]
    assert guard_seen and len(guard_seen[0]) == 1
    assert guard_seen[0][0]["data"] == {"commit_sha": candidate["candidate_sha"]}
    assert sum(path == "pulls" for path, _ in calls) == 1
    assert sum(path.startswith("statuses/") for path, _ in calls) == 1
    assert sum(event["kind"] == "promotion_requested" for event in journal.events()) == 1
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    assert smoked == [candidate["candidate_sha"]]
    assert "token-in-stdout" not in "".join(p.read_text() for p in journal.root.glob("event-*.json"))


def test_three_read_failures_exhaust_retries_without_promotion(promotion_case, monkeypatch):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: None)
    original_api = promotion.api
    attempts = []
    def api(path, body=None):
        if "/check-runs?" in path:
            attempts.append(path)
            raise PromotionError("sensitive provider response")
        return original_api(path, body)
    promotion.api = api
    promotion.before_merge = lambda: pytest.fail("guard must not run")
    with pytest.raises(PromotionError):
        promotion(candidate, journal)
    assert len(attempts) == 3
    assert request_failures(journal) == [
        {"path": attempts[0], "read_only": True, "attempt": i, "error_type": "PromotionError"}
        for i in (1, 2, 3)]
    assert not any(e["kind"] in {"github_checks_passed", "promotion_requested"} for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


@pytest.mark.parametrize("write_phase", ["pulls", "status"])
def test_ambiguous_publication_write_is_never_retried(promotion_case, monkeypatch, write_phase):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: pytest.fail("write retried"))
    original_api = promotion.api
    calls = []
    def api(path, body=None):
        calls.append((path, body))
        if ((path == "pulls" and write_phase == "pulls")
                or (path.startswith("statuses/") and write_phase == "status")):
            raise PromotionError("write may already have succeeded; secret")
        return original_api(path, body)
    promotion.api = api
    promotion.before_merge = lambda: pytest.fail("guard must not run")
    with pytest.raises(PromotionError):
        promotion(candidate, journal)
    writes = [(path, body) for path, body in calls if body is not None]
    failed_path = "pulls" if write_phase == "pulls" else f"statuses/{candidate['candidate_sha']}"
    assert sum(path == failed_path for path, _ in writes) == 1
    assert request_failures(journal) == [
        {"path": failed_path, "read_only": False, "attempt": 1, "error_type": "PromotionError"}]
    assert not any(e["kind"] == "github_checks_passed" for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_empty_body_is_a_write(promotion_case, monkeypatch):
    promotion, _, journal, _, _, _, _ = promotion_case
    calls = []
    def api(path, body=None):
        calls.append((path, body))
        raise PromotionError("sensitive")
    promotion.api = api
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: pytest.fail("write retried"))
    with pytest.raises(PromotionError):
        promotion._request("empty-write", journal, {})
    assert calls == [("empty-write", {})]
    assert request_failures(journal) == [
        {"path": "empty-write", "read_only": False, "attempt": 1, "error_type": "PromotionError"}]


@pytest.mark.parametrize("failed_kind", ["status", "check-run"])
def test_failed_ci_is_not_a_request_failure_or_retried(promotion_case, monkeypatch, failed_kind):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    original_api = promotion.api
    calls = []
    def api(path, body=None):
        calls.append(path)
        if "/status?" in path:
            return {"statuses": [{"context": "ci", "state": "failure"}]} if failed_kind == "status" else {}
        if "/check-runs?" in path:
            return {"check_runs": [{"name": "ci", "status": "completed", "conclusion": "failure"}]}
        return original_api(path, body)
    promotion.api = api
    promotion.settings["required_status_checks"] = ["ci"]
    promotion.before_merge = lambda: pytest.fail("guard must not run")
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: pytest.fail("failed CI retried"))
    with pytest.raises(PromotionError, match="failed candidate check"):
        promotion(candidate, journal)
    assert sum("/status?" in path for path in calls) == 1
    assert sum("/check-runs?" in path for path in calls) == (failed_kind == "check-run")
    assert not request_failures(journal)
    assert not any(e["kind"] in {"github_checks_passed", "promotion_requested"} for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


@pytest.mark.parametrize("expire", ["during-failure", "during-delay"])
def test_expired_deadline_stops_retry_and_promotion(promotion_case, monkeypatch, expire):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    original_api = promotion.api
    attempts = []
    def api(path, body=None):
        if "/status?" in path:
            attempts.append(path)
            if expire == "during-failure":
                promotion.deadline = time.monotonic() - 1
            raise PromotionError("secret")
        return original_api(path, body)
    def sleep(delay):
        assert 0 < delay <= 0.2
        promotion.deadline = time.monotonic() - 1
    promotion.api = api
    promotion.before_merge = lambda: pytest.fail("guard must not run")
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", sleep)
    with pytest.raises(PromotionError):
        promotion(candidate, journal)
    assert len(attempts) == 1
    assert request_failures(journal) == [
        {"path": attempts[0], "read_only": True, "attempt": 1, "error_type": "PromotionError"}]
    assert not any(e["kind"] in {"github_checks_passed", "promotion_requested"} for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_deadline_expiring_on_successful_checks_stops_guard(promotion_case):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    original_api = promotion.api
    def api(path, body=None):
        if "/check-runs?" in path:
            promotion.deadline = time.monotonic() - 1
            return {}
        return original_api(path, body)
    promotion.api = api
    promotion.before_merge = lambda: pytest.fail("guard must not run")
    with pytest.raises(PromotionError, match="budget"):
        promotion(candidate, journal)
    assert not any(e["kind"] in {"github_checks_passed", "promotion_requested"} for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_default_adapter_failure_evidence_does_not_capture_provider_output(promotion_case, monkeypatch):
    promotion, _, journal, _, _, _, _ = promotion_case
    promotion.api = promotion._api
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.subprocess.run",
                        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 1, "stdout-secret", "stderr-secret"))
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: None)
    with pytest.raises(PromotionError):
        promotion._request("commits/sha/status", journal)
    assert len(request_failures(journal)) == 3
    evidence = "".join(p.read_text() for p in journal.root.glob("event-*.json"))
    assert "stdout-secret" not in evidence and "stderr-secret" not in evidence


def test_attempt_counts_reset_for_distinct_reads(promotion_case, monkeypatch):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    original_api = promotion.api
    attempts = {}
    delays = []
    def api(path, body=None):
        if "/status?" in path or "/check-runs?" in path:
            attempts[path] = attempts.get(path, 0) + 1
            if attempts[path] <= 2:
                raise PromotionError("secret")
        return original_api(path, body)
    promotion.api = api
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", delays.append)
    assert promotion(candidate, journal)["status"] == "complete"
    assert list(attempts.values()) == [3, 3]
    failures = request_failures(journal)
    assert [f["attempt"] for f in failures] == [1, 2, 1, 2]
    assert failures[0]["path"] == failures[1]["path"]
    assert failures[2]["path"] == failures[3]["path"] != failures[0]["path"]
    assert delays == [0.2, 0.4, 0.2, 0.4]
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    assert smoked == [candidate["candidate_sha"]]


def test_non_promotion_error_is_not_retried(promotion_case, monkeypatch):
    promotion, _, journal, _, _, _, _ = promotion_case
    calls = []
    def api(path):
        calls.append(path)
        raise ValueError("unexpected")
    promotion.api = api
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep", lambda _: pytest.fail("unexpected retry"))
    with pytest.raises(ValueError, match="unexpected"):
        promotion._request("read", journal)
    assert calls == ["read"]
    assert not request_failures(journal)


def test_expired_deadline_before_start_performs_no_publication(promotion_case):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    promotion.deadline = time.monotonic() - 1
    promotion.api = lambda *args: pytest.fail("request after deadline")
    with pytest.raises(PromotionError, match="budget"):
        promotion(candidate, journal)
    assert not journal.events()
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_deadline_expiring_on_merge_confirmation_preserves_evidence_without_smoke(promotion_case):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    original_api = promotion.api
    def api(path, body=None):
        result = original_api(path, body)
        if path == "pulls/1" and result["merged"]:
            promotion.deadline = time.monotonic() - 1
        return result
    promotion.api = api
    with pytest.raises(PromotionError, match="budget"):
        promotion(candidate, journal)
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    assert journal.events()[-1]["kind"] == "merge_confirmed"
    assert not smoked


def test_pending_required_check_never_records_pass_when_budget_expires(promotion_case, monkeypatch):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    original_api = promotion.api
    def api(path, body=None):
        if "/check-runs?" in path:
            return {"check_runs": [{"name": "ci", "status": "in_progress", "conclusion": None}]}
        return original_api(path, body)
    promotion.api = api
    promotion.settings["required_status_checks"] = ["ci"]
    promotion.before_merge = lambda: pytest.fail("pending checks must not reach guard")
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.time.sleep",
                        lambda _: setattr(promotion, "deadline", time.monotonic() - 1))
    with pytest.raises(PromotionError, match="pending"):
        promotion(candidate, journal)
    assert not request_failures(journal)
    assert not any(e["kind"] in {"github_checks_passed", "promotion_requested"} for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


def test_deadline_expiring_in_guard_stops_main_push(promotion_case):
    promotion, candidate, journal, remote, _, _, smoked = promotion_case
    def guard():
        promotion.deadline = time.monotonic() - 1
    promotion.before_merge = guard
    with pytest.raises(PromotionError, match="budget"):
        promotion(candidate, journal)
    assert any(e["kind"] == "github_checks_passed" for e in journal.events())
    assert not any(e["kind"] == "promotion_requested" for e in journal.events())
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["base_sha"]
    assert not smoked


@pytest.mark.parametrize("mutated", [False, True])
def test_real_delivery_promotion_third_smoke_runner_failure(promotion_case, tmp_path, monkeypatch, mutated, investigating_planner):
    from contextlib import contextmanager
    from pathlib import Path
    from shutil import copytree, rmtree
    from auto_mindsdb_factory.product_delivery import ProductDelivery
    from auto_mindsdb_factory.railway_runner import RailwayOperationError

    promotion, candidate, old_journal, remote, _, _, _ = promotion_case
    # The fixture's GitHub API is local; the real promotion still pushes to its bare remote.
    controller = tmp_path / "controller"
    subprocess.run(["git", "init", "-q", str(controller)], check=True)
    copytree(Path(__file__).resolve().parents[1] / "schemas", controller / "schemas")
    git(controller, "add", "schemas")
    git(controller, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
        "-c", "commit.gpgsign=false", "commit", "-qm", "controller fixture")
    monkeypatch.setattr("auto_mindsdb_factory.product_delivery.repo_root", lambda: controller)
    monkeypatch.setattr("auto_mindsdb_factory.product_promotion.repo_root", lambda: controller)
    rmtree(old_journal.root)
    calls = []
    class Agent:
        def run(self, *args):
            calls.append("builder")
            return {"status": "passed", "findings": []}
    @contextmanager
    def sandbox(*args, **kwargs):
        yield None
    delivery = ProductDelivery(promotion.profile, promotion.target, promotion.store,
                               agent_factory=lambda *args: Agent(), sandbox_factory=sandbox, planner_factory=investigating_planner)
    delivery.settings["smoke"] = [["first"], ["second"], ["third"]]
    def prepare(sha, workspace, journal, **kwargs):
        if not kwargs: calls.append("prepare")
        workspace.mkdir()
        return {}
    monkeypatch.setattr(delivery, "_prepare", prepare)
    monkeypatch.setattr(delivery, "_commit", lambda *args: (candidate["candidate_sha"], candidate["changed_paths"]))
    def verify(*args, **kwargs):
        calls.append("verifier")
        return candidate["verification"]
    monkeypatch.setattr(delivery, "verify", verify)
    error = RailwayOperationError({"action": "run"}, "ExecInterruptedError")
    if mutated:
        error.phase, error.error_type = "exit", "OSError"
        error.args = ("Railway exit failed (OSError)",)
    def command(sandbox, command, **kwargs):
        if not kwargs: calls.append(command[0])
        if command == ["third"]:
            raise error
        return {"command": command, "exit_code": 0}
    monkeypatch.setattr(delivery, "_command", command)
    promotion.smoke = delivery.smoke
    with pytest.raises(RailwayOperationError) as raised:
        delivery.run(promotion.task, base_sha=candidate["base_sha"], promote=promotion, on_stage=lambda s: None)
    assert raised.value is error
    assert calls == ["prepare", "builder", "verifier", "prepare", "first", "second", "third"]
    assert git(remote, "rev-parse", "refs/heads/main") == candidate["candidate_sha"]
    journal = RunJournal(delivery.store, promotion.task.document["id"])
    events = journal.events()
    assert journal.summary()["status"] == "blocked" and journal.summary()["attempts"] == 1
    assert sum(e["kind"] == "promotion_requested" for e in events) == 1
    assert sum(e["kind"] == "merge_confirmed" for e in events) == 1
    assert [e["data"]["index"] for e in events if e["kind"] == "post_merge_smoke"] == [1, 2]
    assert events[-1]["kind"] == "run_finished"
    data = events[-1]["data"]
    assert data["status"] == "blocked" and data["reason"] == "RailwayOperationError"
    assert (data["runner_operation"], data["runner_category"]) == (
        ("unknown", "UnknownError") if mutated else ("run", "ExecInterruptedError"))
    assert not any(e["kind"] == "run_finished" and e["data"]["status"] == "complete" for e in events)
