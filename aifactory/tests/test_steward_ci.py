"""Deterministic histories: ancestry, coverage, reruns and secret-safe evidence."""

import json

import httpx
import pytest

from aifactory.github import CheckSummary, GitHub, GitHubError
from aifactory.security import redact
from aifactory.steward_ci import diagnose

REPO = {"full_name": "org/repo", "default_branch": "main"}
TOKEN = "workspace-secret-with-no-generic-prefix"


def run(sha, workflow=1, *, identity=None, attempt=1, conclusion="success", status="completed"):
    identity = identity or ord(sha[0]) * 10 + workflow
    return dict(id=identity, workflow_id=workflow, head_sha=sha, run_attempt=attempt,
                run_number=identity, name=f"workflow-{workflow}", status=status, conclusion=conclusion,
                html_url=f"https://github.com/org/repo/actions/runs/{identity}")


def job(r, *, conclusion=None, name="tests", attempt=None, steps=None):
    attempt = attempt or r.get("run_attempt") or 1
    identity = r["id"] * 10 + attempt
    return dict(id=identity, name=name, status=r["status"], conclusion=conclusion or r["conclusion"],
                html_url=f"{r['html_url']}/job/{identity}", steps=steps or [])


class History:
    token = TOKEN

    def __init__(self, runs, chain=("c", "b", "a")):
        self.runs = runs
        self.chain = chain
        self.jobs = {(r["id"], r.get("run_attempt") or 1): [job(r)] for r in runs}
        self.logs = {}
        self.check_runs = {}
        self.statuses = {}
        self.errors = set()
        self.calls = []
        self.page_size = 100
        self.total_override = {}
        self.checked = []
        self.summaries = {}

    async def branch_sha(self, name, branch):
        assert (name, branch) == ("org/repo", "main")
        return self.chain[0]

    async def checks(self, name, sha):
        self.checked.append(sha)
        return self.summaries.get(sha, CheckSummary("success", []))  # deliberately insufficient legacy view

    async def request(self, method, path, *, params=None):
        assert method == "GET"
        path = path.removeprefix("/repos/org/repo")
        self.calls.append((path, params))
        if path in self.errors:
            raise GitHubError(f"denied {TOKEN}")
        if path.startswith("/commits/") and len(path.split("/")) == 3:
            sha = path.split("/")[-1]
            i = self.chain.index(sha)
            return {"sha": sha, "parents": [{"sha": self.chain[i + 1]}] if i + 1 < len(self.chain) else []}
        if path == "/actions/runs":
            rows, key = self.runs, "workflow_runs"
            if params.get("head_sha"):
                rows = [r for r in rows if r["head_sha"] == params["head_sha"]]
        elif path.endswith("/jobs"):
            parts = path.split("/")
            rows, key = self.jobs.get((int(parts[3]), int(parts[5])), []), "jobs"
        elif path.endswith("/check-runs"):
            rows, key = self.check_runs.get(path.split("/")[2], []), "check_runs"
        elif path.endswith("/status"):
            rows, key = self.statuses.get(path.split("/")[2], []), "statuses"
        else:
            raise AssertionError(path)
        page = params["page"]
        size = self.page_size
        return {"total_count": self.total_override.get(path, len(rows)), key: rows[(page - 1) * size:page * size]}

    async def job_log_tail(self, name, check, limit=6000, *, redact_extra=None):
        # Honor the real helper's contract, including its requested limit.
        return redact(self.logs.get(check["id"], "AssertionError: expected 2, got 1"), redact_extra or ())[-limit:]


@pytest.mark.asyncio
async def test_ancestry_not_completion_order_and_history_pages():
    runs = [run("c", conclusion="failure"), run("a"), run("b", conclusion="failure")]
    runs[0]["updated_at"] = "2020-01-01"
    runs[2]["updated_at"] = "2030-01-01"
    gh = History(runs)
    gh.page_size = 1  # short pages must continue until total_count
    result = await diagnose(gh, REPO)
    assert result["state"] == "failure"
    assert result["last_good_sha"] == "a"
    assert result["first_bad_sha"] == "b"
    assert not result["needs_bisection"]
    assert result["classification"] == "code"
    assert result["failures"][0]["evidence"]
    assert result["failures"][0]["url"]
    assert ("/actions/runs", {"branch": "main", "per_page": 100, "page": 3}) in gh.calls
    json.dumps(result)


@pytest.mark.asyncio
async def test_skipped_commits_request_bisection_not_invented_culprit():
    gh = History([run("d", conclusion="failure"), run("a")], chain=("d", "c", "b", "a"))
    result = await diagnose(gh, REPO)
    assert result["last_good_sha"] == "a"
    assert result["first_bad_sha"] is None
    assert result["candidate_interval"] == {"good": "a", "bad": "d", "commits": ["b", "c", "d"]}
    assert result["needs_bisection"]


@pytest.mark.asyncio
async def test_successful_workflow_does_not_erase_failure():
    gh = History([run("c", 1), run("c", 2, conclusion="failure"), run("b", 1), run("b", 2)])
    result = await diagnose(gh, REPO)
    assert result["state"] == "failure"
    assert result["last_good_sha"] == "b"
    assert result["first_bad_sha"] == "c"
    assert result["failures"][0]["workflow_id"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["none", "pending", "api", "missing-workflow", "short-history", "missing-attempt", "no-jobs"])
async def test_incomplete_is_never_recovery(case):
    runs = [run("c")]
    if case == "none":
        runs = []
    elif case == "pending":
        runs = [run("c", conclusion=None, status="in_progress")]
    elif case == "missing-workflow":
        runs.append(run("b", 2))
    elif case == "missing-attempt":
        runs[0].pop("run_attempt")
    gh = History(runs)
    if case == "missing-workflow":
        gh.summaries["c"] = CheckSummary("pending", [dict(id=999, name="workflow-2", kind="workflow")])
    if case == "api":
        gh.errors.add("/commits/c/check-runs")
    if case == "short-history":
        gh.total_override["/actions/runs"] = 2
    if case == "no-jobs":
        gh.jobs.clear()
    result = await diagnose(gh, REPO)
    assert result["state"] in ("incomplete", "pending")
    assert not result["complete"]
    assert result["last_good_sha"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("conclusion", ["skipped", "neutral", "success"])
async def test_completed_conditional_jobs_are_successful(conclusion):
    r = run("c")
    gh = History([r])
    gh.jobs[r["id"], 1].append(job(r, conclusion=conclusion, name="conditional"))
    gh.jobs[r["id"], 1][-1]["id"] += 10000
    result = await diagnose(gh, REPO)
    assert result["state"] == "success"
    assert result["complete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("text,category", [
    ("AssertionError: wrong result", "code"),
    ("SyntaxError: unexpected token", "code"),
    ("Runner lost communication with the server", "infrastructure"),
    ("No matching distribution found for pytest", "dependency"),
    ("ERESOLVE unable to resolve dependency tree", "dependency"),
    ("something failed", "unknown"),
    ("", "unknown"),
])
async def test_failure_classes_have_explicit_job_evidence(text, category):
    r = run("c", conclusion="failure")
    gh = History([r])
    gh.logs[job(r)["id"]] = text
    result = await diagnose(gh, REPO)
    assert result["classification"] == category
    failure = result["failures"][0]
    assert failure["evidence"]
    assert failure["job_name"] == "tests"
    assert failure["attempt"] == 1
    assert failure["run_id"] == r["id"]
    if not text:
        assert "Job log unavailable or empty" in failure["evidence"]


@pytest.mark.asyncio
async def test_same_sha_rerun_test_failure_is_flaky_history_not_active():
    r = run("c", attempt=2)
    gh = History([r])
    gh.jobs[r["id"], 1] = [job(r, conclusion="failure", attempt=1)]
    result = await diagnose(gh, REPO)
    assert result["state"] == "success"
    assert result["classification"] == "flaky"
    assert result["failures"] == []
    assert "Same SHA" in result["attempt_history"][0]["evidence"][-1]
    assert {entry["attempt"] for entry in result["attempt_history"]} == {1, 2}


@pytest.mark.asyncio
async def test_current_attempt_only_classifies_current_failure():
    r = run("c", attempt=2, conclusion="failure")
    gh = History([r])
    gh.jobs[r["id"], 1] = [job(r, attempt=1)]
    gh.logs[job(r)["id"]] = "Runner lost communication with server"
    result = await diagnose(gh, REPO)
    assert result["classification"] == "infrastructure"
    assert len(result["failures"]) == 1
    assert result["failures"][0]["attempt"] == 2
    assert result["attempt_history"][0]["classification"] == "code"


@pytest.mark.asyncio
async def test_install_failure_mentioning_pytest_is_not_flaky():
    r = run("c", attempt=2)
    gh = History([r])
    earlier = job(r, attempt=1, conclusion="failure", steps=[{"name": "Install pytest dependencies", "conclusion": "failure"}])
    gh.jobs[r["id"], 1] = [earlier]
    gh.logs[earlier["id"]] = "pytest AssertionError: No matching distribution found"
    result = await diagnose(gh, REPO)
    assert result["attempt_history"][0]["classification"] == "dependency"
    assert result["classification"] != "flaky"


@pytest.mark.asyncio
@pytest.mark.parametrize("conclusion,attempt", [("success", None), ("success", 1)])
async def test_missing_attempt_or_empty_jobs_never_establish_last_good(conclusion, attempt):
    old = run("b", conclusion=conclusion, attempt=attempt)
    gh = History([run("c", conclusion="failure"), old])
    if attempt:
        gh.jobs[old["id"], attempt] = []
    result = await diagnose(gh, REPO)
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert result["needs_bisection"]
    assert result["incomplete"]


def external_check(identity, conclusion="success", name=None):
    return dict(id=identity, name=name or f"external-{identity}", status="completed", conclusion=conclusion,
                app={"id": 9, "slug": "external"}, html_url=f"https://ci.example/{identity}")


def status(identity, state="success", context=None):
    return dict(id=identity, context=context or f"status-{identity}", state=state, target_url=f"https://ci.example/{identity}")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["check", "status"])
@pytest.mark.parametrize("older", ["failure", "missing", "api"])
async def test_historical_external_failure_or_gap_never_last_good(kind, older):
    gh = History([run("c", conclusion="failure"), run("b")])
    if kind == "check":
        gh.check_runs["c"] = [external_check(1, name="external")]
        if older != "missing":
            gh.check_runs["b"] = [external_check(2, "failure", name="external")]
        if older == "api":
            gh.errors.add("/commits/b/check-runs")
    else:
        gh.statuses["c"] = [status(1, context="external")]
        if older != "missing":
            gh.statuses["b"] = [status(2, "failure", context="external")]
        if older == "api":
            gh.errors.add("/commits/b/status")
    if older == "missing":
        witness = ({**external_check(2, name="external"), "kind": "check_run"} if kind == "check"
                   else dict(name="external", kind="status"))
        gh.summaries["b"] = CheckSummary("pending", [witness])
    result = await diagnose(gh, REPO)
    assert "b" in gh.checked
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    if older != "failure":
        assert result["incomplete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["check", "status"])
@pytest.mark.parametrize("missing_page", [False, True])
async def test_head_second_page_failure_or_unavailable_is_not_green(kind, missing_page):
    gh = History([run("c")])
    if kind == "check":
        gh.check_runs["c"] = [external_check(i) for i in range(1, 101)]
        if not missing_page:
            gh.check_runs["c"].append(external_check(101, "failure"))
        gh.total_override["/commits/c/check-runs"] = 101
    else:
        gh.statuses["c"] = [status(i) for i in range(1, 101)]
        if not missing_page:
            gh.statuses["c"].append(status(101, "failure"))
        gh.total_override["/commits/c/status"] = 101
    result = await diagnose(gh, REPO)
    assert result["state"] == ("incomplete" if missing_page else "failure")
    if missing_page:
        assert result["incomplete"]


@pytest.mark.asyncio
async def test_owned_action_check_does_not_become_unknown_external_failure():
    r = run("c", conclusion="failure")
    gh = History([r, run("b")])
    gh.check_runs["c"] = [{**job(r), "app": {"slug": "github-actions", "id": 1}}]
    result = await diagnose(gh, REPO)
    assert result["classification"] == "code"
    assert len(result["failures"]) == 1
    assert result["failures"][0]["run_id"] == r["id"]
    assert not result["incomplete"]


@pytest.mark.asyncio
async def test_latest_run_and_attempt_selected_not_old_failure():
    old = run("c", identity=10, conclusion="failure")
    current = run("c", identity=20)
    gh = History([current, old])
    gh.check_runs["c"] = [{**job(old), "app": {"slug": "github-actions"}}]
    result = await diagnose(gh, REPO)
    assert result["state"] == "success"
    assert result["failures"] == []
    assert {e["run_id"] for e in result["attempt_history"]} == {old["id"], current["id"]}
    assert result["classification"] == "flaky"


@pytest.mark.asyncio
async def test_log_and_metadata_redaction_before_output_bounds():
    r = run("c", conclusion="failure")
    r["name"] = TOKEN
    gh = History([r])
    secret = "ghp_" + "A" * 300
    gh.logs[job(r)["id"]] = TOKEN + " " + secret + " AssertionError"
    for limit in range(1, 160):
        result = await diagnose(gh, REPO, max_log_chars=limit)
        output = json.dumps(result)
        assert TOKEN not in output
        assert "ghp_" not in output
        assert "AAAA" not in output
        assert "workspace-secret" not in output
        assert len(result["failures"][0]["excerpt"]) <= limit


@pytest.mark.asyncio
async def test_real_log_helper_redacts_full_download_before_tail(monkeypatch):
    secret = "ghp_" + "B" * 300
    body = (TOKEN + " " + secret + " AssertionError").encode()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    original = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: original(transport=transport, **kw))
    gh = GitHub(TOKEN)
    for limit in range(1, 160):
        text = await gh.job_log_tail("org/repo", {"kind": "check_run", "id": 1}, limit=limit, redact_extra=(TOKEN,))
        assert len(text) <= limit
        assert "BBBB" not in text
        assert "workspace-secret" not in text
        assert TOKEN not in text


@pytest.mark.asyncio
async def test_real_log_helper_refuses_oversized_or_unavailable_logs(monkeypatch):
    original = httpx.AsyncClient
    for code, body in [(200, b"x" * 1_000_001), (404, b"missing")]:
        transport = httpx.MockTransport(lambda request: httpx.Response(code, content=body))
        monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: original(transport=transport, **kw))
        text = await GitHub(TOKEN).job_log_tail("org/repo", {"kind": "check_run", "id": 1}, redact_extra=(TOKEN,))
        assert text == ""


@pytest.mark.asyncio
async def test_limits_are_enforced_and_visible():
    gh = History([run("c", attempt=3, conclusion="failure"), run("b")])
    result = await diagnose(gh, REPO, max_commits=1, max_attempts=1, max_jobs=1)
    assert result["last_good_sha"] is None
    assert result["needs_bisection"]
    assert any("window incomplete" in gap for gap in result["incomplete"])
    assert any("Attempt history bounded" in gap for gap in result["incomplete"])
    with pytest.raises(ValueError):
        await diagnose(gh, REPO, max_log_chars=0)


@pytest.mark.asyncio
async def test_mature_repository_needs_only_relevant_ancestry_window():
    gh = History([run("c", conclusion="failure"), run("b")], chain=("c", "b", "a", "older"))
    result = await diagnose(gh, REPO, max_commits=2)
    assert result["last_good_sha"] == "b"
    assert result["first_bad_sha"] == "c"
    assert result["complete"]
    assert not any(path == "/commits/a" for path, _ in gh.calls)


@pytest.mark.asyncio
async def test_unmatched_action_check_is_explicitly_incomplete():
    gh = History([run("c")])
    gh.check_runs["c"] = [{**external_check(99), "app": {"slug": "github-actions"}}]
    result = await diagnose(gh, REPO)
    assert result["state"] == "incomplete"
    assert any("Unmatched Actions" in gap for gap in result["incomplete"])


@pytest.mark.asyncio
async def test_pending_workflow_alongside_failure_is_still_red():
    gh = History([run("c", 1, conclusion="failure"), run("c", 2, status="in_progress", conclusion=None)])
    result = await diagnose(gh, REPO)
    assert result["state"] == "failure"
    assert not result["complete"]


@pytest.mark.asyncio
async def test_merge_culprit_requests_bisection():
    gh = History([run("c", conclusion="failure"), run("b")])
    original = gh.request

    async def request(method, path, **kwargs):
        data = await original(method, path, **kwargs)
        if path.endswith("/commits/c"):
            data["parents"].append({"sha": "side-branch"})
        return data

    gh.request = request
    result = await diagnose(gh, REPO)
    assert result["first_bad_sha"] is None
    assert result["candidate_interval"]["commits"] == ["c"]
    assert result["needs_bisection"]


@pytest.mark.asyncio
async def test_incomplete_bad_snapshot_never_proves_exact_culprit():
    gh = History([run("c", conclusion="failure"), run("b")])
    gh.errors.add("/commits/c/status")
    result = await diagnose(gh, REPO)
    assert result["state"] == "failure"
    assert result["first_bad_sha"] is None
    assert result["needs_bisection"]


@pytest.mark.asyncio
async def test_install_step_with_assertion_is_not_flaky():
    r = run("c", attempt=2)
    gh = History([r])
    gh.jobs[r["id"], 1] = [job(r, conclusion="failure", attempt=1,
        steps=[{"name": "Install dependencies", "conclusion": "failure"}])]
    gh.logs[job(r, attempt=1)["id"]] = "pytest AssertionError while installing plugin"
    result = await diagnose(gh, REPO)
    assert result["attempt_history"][0]["classification"] == "dependency"
    assert result["classification"] != "flaky"


@pytest.mark.asyncio
async def test_pending_job_in_red_workflow_is_incomplete():
    r = run("c", conclusion="failure")
    gh = History([r])
    gh.jobs[r["id"], 1].append(dict(id=99, name="still running", status="in_progress", conclusion=None))
    result = await diagnose(gh, REPO)
    assert result["state"] == "failure"
    assert not result["complete"]
    assert any("Pending run/jobs" in gap for gap in result["incomplete"])


@pytest.mark.asyncio
async def test_missing_workflow_coverage_cannot_establish_historical_good():
    gh = History([run("c", 1, conclusion="failure"), run("c", 2), run("b", 1)])
    # The historical summary independently witnesses workflow-2 on b. Its
    # presence on c alone would not establish that it also applied to b.
    gh.summaries["b"] = CheckSummary("pending", [dict(id=999, name="workflow-2", kind="workflow")])
    result = await diagnose(gh, REPO)
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert any("Missing CI coverage for b" in gap for gap in result["incomplete"])


@pytest.mark.asyncio
async def test_missing_historical_external_page_prevents_last_good():
    gh = History([run("c", conclusion="failure"), run("b")])
    gh.total_override["/commits/b/status"] = 1
    result = await diagnose(gh, REPO)
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert any("Incomplete pagination: /commits/b/status" in gap for gap in result["incomplete"])


@pytest.mark.asyncio
async def test_owned_earlier_attempt_check_is_history_not_active():
    r = run("c", attempt=2, conclusion="failure")
    gh = History([r])
    earlier = job(r, attempt=1)
    gh.jobs[r["id"], 1] = [earlier]
    gh.logs[job(r)["id"]] = "Runner disconnected"
    gh.check_runs["c"] = [{**earlier, "app": {"slug": "github-actions"}}]
    result = await diagnose(gh, REPO)
    assert result["classification"] == "infrastructure"
    assert [e["attempt"] for e in result["failures"]] == [2]


@pytest.mark.asyncio
async def test_unavailable_log_error_stays_unknown_and_secret_safe():
    gh = History([run("c", conclusion="failure")])

    async def unavailable(*args, **kwargs):
        raise GitHubError(TOKEN)

    gh.job_log_tail = unavailable
    result = await diagnose(gh, REPO)
    assert result["classification"] == "unknown"
    assert result["failures"][0]["excerpt"] == ""
    assert "Job log unavailable or empty" in result["failures"][0]["evidence"]
    assert TOKEN not in json.dumps(result)


@pytest.mark.asyncio
async def test_egress_scrubs_literal_credentials_with_json_escapes():
    gh = History([run("c", conclusion="failure")])
    gh.token = 'unusual-secret-with-"quotes"\nand-newline'
    r = gh.runs[0]
    r["name"] = gh.token
    gh.jobs[r["id"], 1][0]["name"] = gh.token
    result = await diagnose(gh, REPO)
    assert result["attempt_history"][0]["job_name"] == "[redacted]"
    assert result["attempt_history"][0]["workflow_name"] == "[redacted]"


@pytest.mark.asyncio
async def test_proven_green_head_is_last_good():
    result = await diagnose(History([run("c")]), REPO)
    assert result["state"] == "success"
    assert result["last_good_sha"] == "c"
    assert result["first_bad_sha"] is None


@pytest.mark.asyncio
async def test_missing_history_names_missing_evidence():
    result = await diagnose(History([]), REPO)
    assert result["state"] == "incomplete"
    assert "No default-branch workflow history available" in result["incomplete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("sha", ["c", "b"])
async def test_stale_external_check_never_certifies_head_or_last_good(sha):
    gh = History([run("c", conclusion="failure" if sha == "b" else "success"), run("b")])
    gh.check_runs["c"] = [external_check(1, name="external")]
    gh.check_runs["b"] = [external_check(2, name="external")]
    gh.check_runs[sha][0]["conclusion"] = "stale"
    result = await diagnose(gh, REPO)
    assert result["states"][sha] == "incomplete"
    assert result["state"] != "success"
    assert not result["complete"]
    assert result["last_good_sha"] != sha
    assert result["first_bad_sha"] is None
    assert f"Stale check evidence: {sha}/" in " ".join(result["incomplete"])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["run", "job"])
async def test_stale_actions_evidence_never_establishes_recovery(kind):
    r = run("c")
    gh = History([r])
    if kind == "run":
        r["conclusion"] = "stale"
    else:
        gh.jobs[r["id"], 1][0]["conclusion"] = "stale"
    result = await diagnose(gh, REPO)
    assert result["state"] == "incomplete"
    assert not result["complete"]
    assert result["last_good_sha"] is None
    assert result["incomplete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field,text,category", [
    ("summary", "AssertionError: incorrect answer", "code"),
    ("text", "Runner disconnected", "infrastructure"),
    ("summary", "No matching distribution found for pytest", "dependency"),
])
async def test_external_failure_uses_check_output_not_actions_logs(field, text, category):
    gh = History([run("c")])
    check = external_check(900, "failure", name="third-party tests")
    check["output"] = {field: text}
    gh.check_runs["c"] = [check]

    async def no_actions_logs(*args, **kwargs):
        pytest.fail("Third-party check IDs must not be sent to the Actions log endpoint")

    gh.job_log_tail = no_actions_logs
    result = await diagnose(gh, REPO)
    assert result["state"] == "failure"
    assert result["classification"] == category
    failure = result["failures"][0]
    assert failure["excerpt"] == text
    assert failure["url"] == check["html_url"]
    assert failure["job_name"] == check["name"]
    assert failure["run_id"] is None
    assert failure["attempt"] is None
    assert any("check-run output" in evidence for evidence in failure["evidence"])
    assert any("Log contains:" in evidence for evidence in failure["evidence"])


@pytest.mark.asyncio
async def test_external_output_is_redacted_before_truncation_and_handles_missing_output():
    gh = History([run("c")])
    check = external_check(900, "failure")
    gh.check_runs["c"] = [check]
    result = await diagnose(gh, REPO)
    assert result["classification"] == "unknown"
    assert "Check output unavailable or empty" in result["failures"][0]["evidence"]
    check["output"] = {"summary": "AssertionError: " + TOKEN,
                       "text": "ghp_" + "Z" * 300 + " tail"}
    for limit in range(1, 160):
        result = await diagnose(gh, REPO, max_log_chars=limit)
        encoded = json.dumps(result)
        assert TOKEN not in encoded
        assert "workspace-secret" not in encoded
        assert "ghp_" not in encoded
        assert "ZZZZ" not in encoded
        assert len(result["failures"][0]["excerpt"]) <= limit

@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["workflow", "check", "status"])
@pytest.mark.parametrize("change", ["rename", "remove"])
@pytest.mark.parametrize("head_failure", [False, True])
async def test_revision_local_coverage_allows_renamed_or_removed_ci(kind, change, head_failure):
    runs = [run("c", conclusion="failure" if head_failure else "success"), run("b")]
    if kind == "workflow":
        runs.append(run("b", 2))
        if change == "rename":
            runs.append(run("c", 3))
    gh = History(runs)
    if kind == "check":
        gh.check_runs["b"] = [external_check(1, name="old-name")]
        if change == "rename":
            gh.check_runs["c"] = [external_check(2, name="new-name")]
    if kind == "status":
        gh.statuses["b"] = [status(1, context="old-name")]
        if change == "rename":
            gh.statuses["c"] = [status(2, context="new-name")]
    result = await diagnose(gh, REPO)
    if head_failure:
        assert result["states"]["b"] == "success"
    else:
        # Proven head recovery does not require inspecting older revisions.
        assert gh.checked == ["c"]
    assert result["state"] == ("failure" if head_failure else "success")
    assert result["last_good_sha"] == ("b" if head_failure else "c")
    assert result["first_bad_sha"] == ("c" if head_failure else None)
    assert result["complete"]
    assert not result["incomplete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["run", "job"])
async def test_stale_historical_actions_never_establishes_last_good(kind):
    old = run("b")
    gh = History([run("c", conclusion="failure"), old])
    if kind == "run":
        old["conclusion"] = "stale"
    else:
        gh.jobs[old["id"], 1][0]["conclusion"] = "stale"
    result = await diagnose(gh, REPO)
    assert result["states"]["b"] == "incomplete"
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert result["needs_bisection"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["workflow", "check_run", "status"])
@pytest.mark.parametrize("sha", ["c", "b"])
async def test_revision_local_missing_check_is_incomplete(kind, sha):
    gh = History([run("c", conclusion="failure" if sha == "b" else "success"), run("b")])
    gh.summaries[sha] = CheckSummary("pending", [dict(id=999, name="expected-here", kind=kind)])
    result = await diagnose(gh, REPO)
    assert result["states"][sha] == "incomplete"
    assert result["last_good_sha"] != sha
    assert result["first_bad_sha"] is None
    assert not result["complete"]
    assert f"Missing CI coverage for {sha}: expected-here" in result["incomplete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["workflow", "check_run"])
async def test_revision_local_actions_fallback_matches_inspected_run_or_job(kind):
    r = run("c")
    gh = History([r])
    identity = r["id"] if kind == "workflow" else job(r)["id"]
    gh.summaries["c"] = CheckSummary("success", [dict(id=identity, name="tests", kind=kind)])
    result = await diagnose(gh, REPO)
    assert result["state"] == "success"
    assert result["complete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("head_failure", [False, True])
@pytest.mark.parametrize("max_pages", [1, 10])
async def test_large_history_proves_local_boundary_within_page_budget(head_failure, max_pages):
    runs = [run("c", conclusion="failure" if head_failure else "success"), run("b")]
    runs.extend(run(f"old-{i}", identity=10000 + i) for i in range(max_pages * 100 + 99))
    gh = History(runs)
    result = await diagnose(gh, REPO, max_pages=max_pages, max_commits=2)
    assert result["state"] == ("failure" if head_failure else "success")
    assert result["last_good_sha"] == ("b" if head_failure else "c")
    assert result["first_bad_sha"] == ("c" if head_failure else None)
    assert result["complete"]
    assert not result["incomplete"]
    assert gh.checked == (["c", "b"] if head_failure else ["c"])
    assert not any(path.endswith("/jobs") and "10000" in path for path, _ in gh.calls)
    branch_pages = [params["page"] for path, params in gh.calls
                    if path == "/actions/runs" and not params.get("head_sha")]
    assert branch_pages == list(range(1, max_pages + 1))
    assert any(params.get("head_sha") == "c" for _, params in gh.calls if params)


@pytest.mark.asyncio
@pytest.mark.parametrize("extra_sha", ["old-100", "force-pushed"])
async def test_unrelated_or_distant_run_does_not_extend_boundary_walk(extra_sha):
    chain = ("c", "b", *(f"old-{i}" for i in range(101)))
    gh = History([run("c", conclusion="failure"), run("b"), run(extra_sha)], chain=chain)
    result = await diagnose(gh, REPO, max_commits=2)
    assert result["last_good_sha"] == "b"
    assert result["first_bad_sha"] == "c"
    assert result["complete"]
    assert gh.checked == ["c", "b"]
    assert [path for path, _ in gh.calls if path.count("/") == 2 and path.startswith("/commits/")] == ["/commits/c"]
    assert not any(str(run(extra_sha)["id"]) in path for path, _ in gh.calls if path.endswith("/jobs"))


@pytest.mark.asyncio
async def test_bounded_history_still_requires_complete_revision_workflows():
    # A second workflow on b is beyond the global discovery window. The scoped
    # query must fetch it, rather than certifying b from the one visible pass.
    gh = History([run("c", conclusion="failure"), run("b"),
                  *(run(f"old-{i}", identity=10000 + i) for i in range(99)),
                  run("b", 2, conclusion="failure")])
    result = await diagnose(gh, REPO, max_pages=1, max_commits=2)
    assert result["states"]["b"] == "failure"
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert result["needs_bisection"]
    assert not result["complete"]
    assert any("window incomplete" in gap for gap in result["incomplete"])


@pytest.mark.asyncio
async def test_bounded_history_missing_boundary_evidence_remains_incomplete():
    gh = History([run("c", conclusion="failure"), run("b"),
                  *(run(f"old-{i}", identity=10000 + i) for i in range(99))])
    gh.errors.add("/commits/b/status")
    result = await diagnose(gh, REPO, max_pages=1, max_commits=2)
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert result["needs_bisection"]
    assert not result["complete"]
    assert "Unavailable GitHub evidence: /commits/b/status" in result["incomplete"]


@pytest.mark.asyncio
async def test_boundary_outside_ancestry_budget_is_not_invented():
    gh = History([run("d", conclusion="failure"), run("a")], chain=("d", "c", "b", "a"))
    result = await diagnose(gh, REPO, max_commits=2)
    assert result["last_good_sha"] is None
    assert result["first_bad_sha"] is None
    assert result["candidate_interval"] is None
    assert result["needs_bisection"]
    assert not result["complete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("sha", ["c", "b"])
async def test_owned_stale_check_cannot_hide_behind_successful_actions_job(sha):
    current = run("c", conclusion="failure" if sha == "b" else "success")
    old = run("b")
    gh = History([current, old])
    owner = current if sha == "c" else old
    gh.check_runs[sha] = [{**job(owner, conclusion="stale"), "app": {"slug": "github-actions"}}]
    result = await diagnose(gh, REPO)
    assert result["state"] != "success"
    assert result["last_good_sha"] != sha
    assert not result["complete"]
    assert any("Stale check evidence" in gap for gap in result["incomplete"])


@pytest.mark.asyncio
@pytest.mark.parametrize("sha", ["c", "b"])
async def test_bounded_history_requires_complete_scoped_pagination(sha):
    gh = History([run("c", conclusion="failure" if sha == "b" else "success"), run("b"),
                  *(run(f"old-{i}", identity=10000 + i) for i in range(99))])
    original = gh.request

    async def request(method, path, **kwargs):
        data = await original(method, path, **kwargs)
        if path.endswith("/actions/runs") and kwargs["params"].get("head_sha") == sha:
            data["total_count"] += 1  # a workflow is missing from this revision
        return data

    gh.request = request
    result = await diagnose(gh, REPO, max_pages=1, max_commits=2)
    assert result["state"] != "success"
    assert result["last_good_sha"] != sha
    assert result["first_bad_sha"] is None
    assert not result["complete"]
    assert "Incomplete pagination: /actions/runs" in result["incomplete"]
