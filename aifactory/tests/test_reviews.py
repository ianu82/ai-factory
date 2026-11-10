from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from aifactory.db import session_scope
from aifactory.github import GitHub, GitHubError
from aifactory.models import Repository
from aifactory.pipeline.runtime import merged_settings
from aifactory.review import (DEFAULT_REVIEW, FACTORY_REPLY_MARKER, new_review_evidence,
                              normalize_feedback, review_policy)


def user(login="reviewer", kind="User"):
    return {"login": login, "type": kind}


def review(id=1, state="COMMENTED", login="reviewer", body="review prose", kind="User"):
    return {"id": id, "state": state, "user": user(login, kind), "body": body,
            "submitted_at": f"2026-11-09T11:00:{id:02d}Z", "html_url": f"https://github.test/review/{id}"}


def inline(id=10, rid=1, login="reviewer", body="fix this", kind="User"):
    return {"id": f"PRRC_{id}", "databaseId": id, "body": body,
            "author": {"login": login, "__typename": kind},
            "pullRequestReview": {"databaseId": rid}, "path": "file.py", "line": 7,
            "originalLine": 6, "diffHunk": "@@ -6 +7 @@", "url": "https://github.test/comment",
            "createdAt": "2026-11-09T11:00:00Z", "updatedAt": "2026-11-09T11:01:00Z"}


def thread(*comments, resolved=False, id="PRRT_1"):
    return {"id": id, "isResolved": resolved, "comments": {"nodes": list(comments)}}


def normalized(reviews=None, threads=None, comments=None, **policy):
    return normalize_feedback(reviews=reviews or [], threads=threads or [], comments=comments or [],
                              policy=policy, identity=user("FactoryAccount"), pull_author=user("PRAuthor"))


def test_defaults_inheritance_and_evidence_contract():
    assert merged_settings({})["review"] == DEFAULT_REVIEW
    assert review_policy() == DEFAULT_REVIEW
    workspace = {"review": {"address": "changes_requested", "ignore": ["noise"], "wait_seconds": 35}}
    assert review_policy(workspace, {"review": {"max_rounds": 0}}) == {
        **DEFAULT_REVIEW, **workspace["review"], "max_rounds": 0}
    copy = review_policy()
    copy["ignore"].append("other")
    assert review_policy()["ignore"] == []
    evidence = new_review_evidence()
    assert evidence == {"rounds": 0, "pending": [], "wait_deadlines": {}, "active_round": None,
                        "active_head": None, "findings": {}}
    evidence["findings"]["x"] = {}
    assert new_review_evidence()["findings"] == {}


def test_commented_inline_is_actionable_but_prose_and_resolved_are_not():
    findings = normalized(reviews=[review()], threads=[thread(inline(), inline()),
                                                      thread(inline(11), resolved=True)])
    assert len(findings) == 1
    f = findings[0]
    assert f == {"id": "comment:10", "kind": "inline", "comment_id": "10", "review_id": "1",
                 "thread_id": "PRRT_1", "reply_to_id": "10", "author": "reviewer", "author_type": "User",
                 "body": "fix this", "url": "https://github.test/comment", "review_state": "COMMENTED",
                 "resolved": False, "path": "file.py", "line": 7, "original_line": 6,
                 "diff_hunk": "@@ -6 +7 @@", "created_at": "2026-11-09T11:00:00Z",
                 "updated_at": "2026-11-09T11:01:00Z"}
    assert normalized(reviews=[review()], threads=[thread(inline())], address="changes_requested") == []


def test_change_requests_current_dismissed_superseded_and_deduplicated():
    reviews = [review(1, "CHANGES_REQUESTED"), review(2, "APPROVED"),
               review(3, "DISMISSED", "dismissed"), review(4, "CHANGES_REQUESTED", "current"),
               review(5, "COMMENTED", "current"), review(6, "CHANGES_REQUESTED", "same", "fix this")]
    threads = [thread(inline(10, 1)), thread(inline(11, 3, "dismissed")),
               thread(inline(12, 4, "current")), thread(inline(13, 5, "current")),
               thread(inline(14, 6, "same"))]
    assert {f["id"] for f in normalized(reviews=reviews[::-1], threads=threads)} == {
        "review:4", "comment:12", "comment:13", "comment:14"}
    assert {f["id"] for f in normalized(reviews=reviews, threads=threads, address="changes_requested")} == {
        "review:4", "comment:12", "comment:14"}
    # A new change request supersedes the old request, even from differently-cased login.
    assert {f["id"] for f in normalized(reviews=[review(1, "CHANGES_REQUESTED"),
                                                               review(2, "CHANGES_REQUESTED", "REVIEWER")],
                                       threads=[thread(inline())])} == {"review:2"}


@pytest.mark.parametrize("mode,expected", [("all", {"comment:10", "comment:11", "comment:12", "issue_comment:31"}),
                                          ("people", {"comment:10", "issue_comment:31"}),
                                          ("bots", {"comment:11", "comment:12"})])
def test_reviewer_modes_and_case_insensitive_identity_ignore_markers(mode, expected):
    threads = [thread(inline()), thread(inline(11, login="typebot", kind="Bot")),
               thread(inline(12, login="connector[bot]")), thread(inline(13, login="FACTORYaccount")),
               thread(inline(14, login="prauthor")), thread(inline(15, login="IGNORED")),
               thread(inline(16, body=FACTORY_REPLY_MARKER))]
    comments = [{"id": i, "user": user(login, kind), "body": "plain PR comment"}
                for i, login, kind in [(31, "human", "User"), (32, "commentbot", "Bot"),
                                      (33, "factoryaccount", "User"), (34, "ignored", "User")]]
    assert {f["id"] for f in normalized(threads=threads, comments=comments, reviewers=mode,
                                       ignore=["ignored"])} == expected
    assert normalized(threads=threads, comments=comments, address="off") == []
    assert normalized(comments=comments, address="changes_requested") == []


@pytest.mark.parametrize("endpoint", ["reviews", "issue_comments"])
def test_rest_pagination(endpoint):
    gh = GitHub("secret")
    gh.request = AsyncMock(side_effect=[[{"id": i} for i in range(100)], [{"id": 100}]])
    result = asyncio.run(getattr(gh, endpoint)("owner/repo", 7))
    assert len(result) == 101
    path = "/repos/owner/repo/pulls/7/reviews" if endpoint == "reviews" else "/repos/owner/repo/issues/7/comments"
    assert [c.args for c in gh.request.call_args_list] == [("GET", path), ("GET", path)]
    assert [c.kwargs["params"]["page"] for c in gh.request.call_args_list] == [1, 2]


def connection(nodes, cursor=None):
    return {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}


def test_graphql_nested_and_thread_pagination():
    gh = GitHub("secret")
    first = {**thread(), "comments": connection([inline()], "comment-cursor")}
    gh.request = AsyncMock(side_effect=[
        {"data": {"repository": {"pullRequest": {"reviewThreads": connection([first], "thread-cursor")}}}},
        {"data": {"node": {"comments": connection([inline(11)], "comment-cursor-2")}}},
        {"data": {"node": {"comments": connection([inline(12)])}}},
        {"data": {"repository": {"pullRequest": {"reviewThreads": connection([
            {**thread(id="PRRT_2"), "comments": connection([inline(13)])}])}}}},
    ])
    threads = asyncio.run(gh.review_threads("owner/repo", 7))
    assert len(threads) == 2
    assert [c["databaseId"] for c in threads[0]["comments"]["nodes"]] == [10, 11, 12]
    variables = [c.kwargs["json"]["variables"] for c in gh.request.call_args_list]
    assert variables == [{"owner": "owner", "name": "repo", "number": 7, "after": None},
                         {"id": "PRRT_1", "after": "comment-cursor"},
                         {"id": "PRRT_1", "after": "comment-cursor-2"},
                         {"owner": "owner", "name": "repo", "number": 7, "after": "thread-cursor"}]


@pytest.mark.parametrize("response", [{"errors": [{"message": "denied"}]},
                                      {"data": {"repository": {}}, "errors": [{"message": "partial"}]},
                                      {"data": None}, {"data": {"repository": None}},
                                      {"data": {"repository": {"pullRequest": {"reviewThreads": {
                                          "nodes": [], "pageInfo": {"hasNextPage": True}}}}}}])
def test_graphql_errors_and_incomplete_data_are_not_clean_feedback(response):
    gh = GitHub("secret")
    gh.request = AsyncMock(return_value=response)
    with pytest.raises(GitHubError):
        asyncio.run(gh.review_threads("owner/repo", 7))


def test_replies_use_root_rest_comment_and_resolution_uses_node_id():
    findings = normalized(threads=[thread(inline(), inline(11))])
    assert findings[1]["reply_to_id"] == "10"
    gh = GitHub("secret")
    gh.request = AsyncMock(side_effect=[{"id": 99}, {"data": {"resolveReviewThread": {
        "thread": {"id": "PRRT_1", "isResolved": True}}}}])
    assert asyncio.run(gh.reply_review_comment("owner/repo", 7, findings[1]["reply_to_id"], "Fixed in abc")) == {"id": 99}
    call = gh.request.call_args_list[0]
    assert call.args == ("POST", "/repos/owner/repo/pulls/7/comments/10/replies")
    assert call.kwargs["json"]["body"] == f"Fixed in abc\n\n{FACTORY_REPLY_MARKER}"
    assert asyncio.run(gh.resolve_review_thread(findings[0]["thread_id"]))["isResolved"] is True
    call = gh.request.call_args_list[1]
    assert call.kwargs["json"]["variables"] == {"id": "PRRT_1"}
    assert "resolveReviewThread(input:{threadId:$id})" in call.kwargs["json"]["query"]
    gh.request = AsyncMock(return_value={"data": {}, "errors": [{"message": "denied"}]})
    with pytest.raises(GitHubError):
        asyncio.run(gh.resolve_review_thread("PRRT_1"))


def test_authenticated_feedback_snapshot_and_off_mode():
    gh = GitHub("secret")
    gh.request = AsyncMock(side_effect=[user("ServiceAccount"), {"user": user("PRAuthor")},
        [review()], {"data": {"repository": {"pullRequest": {"reviewThreads": connection([
            {**thread(), "comments": connection([inline(), inline(11, login="SERVICEACCOUNT")])}])}}}}, []])
    findings = asyncio.run(gh.review_feedback("owner/repo", 7, {}))
    assert [f["id"] for f in findings] == ["comment:10"]
    gh.request.reset_mock()
    assert asyncio.run(gh.review_feedback("owner/repo", 7, {"address": "off"})) == []
    gh.request.assert_not_called()


def test_configuration_round_trips(client, signed_in, monkeypatch):
    from aifactory import api

    ws = signed_in["workspaces"][0]
    base = f"/api/workspaces/{ws['id']}"
    assert ws["settings"]["review"] == DEFAULT_REVIEW
    r = client.patch(base, json={"settings": {"review": {"wait_seconds": 0, "ignore": ["Noise"]}}})
    assert r.status_code == 200, r.text
    r = client.patch(base, json={"settings": {"review": {"address": "changes_requested"}}})
    assert r.json()["settings"]["review"] == {**DEFAULT_REVIEW, "wait_seconds": 0, "ignore": ["Noise"],
                                             "address": "changes_requested"}
    monkeypatch.setattr(api, "_detect_repository", AsyncMock(return_value=(
        {"full_name": "owner/repo", "default_branch": "main"}, {"checks": ["pytest"]})))
    r = client.post(base + "/repositories", json={"full_name": "owner/repo", "config": {"review": {"max_rounds": 0}}})
    assert r.status_code == 200, r.text
    repo_id = r.json()["id"]
    assert r.json()["config"]["review"] == {"max_rounds": 0}
    r = client.patch(base + f"/repositories/{repo_id}", json={"config": {"review": {"reviewers": "bots"}}})
    assert r.status_code == 200, r.text
    assert r.json()["config"]["review"] == {"max_rounds": 0, "reviewers": "bots"}
    me = client.get("/api/me").json()
    repo = client.get(base + "/repositories").json()[0]
    assert review_policy(me["workspaces"][0]["settings"], repo["config"]) == {
        "address": "changes_requested", "reviewers": "bots", "wait_seconds": 0, "ignore": ["Noise"], "max_rounds": 0}


INVALID = [None, [], "off", {"address": "invalid"}, {"reviewers": "human"}, {"ignore": "name"},
           {"ignore": [1]}, {"ignore": [""]}, {"ignore": ["  "]}, {"ignore": [None]}, {"ignore": {}},
           {"unknown": True}] + [{key: value} for key in ("wait_seconds", "max_rounds")
                                for value in (True, False, -1, 1.5, "2", None)]


@pytest.mark.parametrize("invalid", INVALID)
def test_api_rejects_invalid_review_policies(client, signed_in, invalid):
    ws_id = signed_in["workspaces"][0]["id"]
    base = f"/api/workspaces/{ws_id}"
    with session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="owner/repo", config={})
        s.add(repo)
        s.flush()
        repo_id = repo.id
    r = client.patch(base, json={"settings": {"review": invalid}})
    assert r.status_code == 422, r.text
    r = client.patch(base + f"/repositories/{repo_id}", json={"config": {"review": invalid}})
    assert r.status_code == 422, r.text
    # Rejected writes must not change either persisted config.
    assert client.get("/api/me").json()["workspaces"][0]["settings"]["review"] == DEFAULT_REVIEW
    assert client.get(base + "/repositories").json()[0]["config"] == {
        "steward": {"mode": "propose", "fix_minutes": 60}}
    with session_scope() as s:
        assert s.get(Repository, repo_id).config == {}


@pytest.mark.parametrize("invalid", INVALID)
def test_connect_validates_review_before_github_access(client, signed_in, invalid, monkeypatch):
    from aifactory import api

    detect = AsyncMock(side_effect=AssertionError("Invalid policies must not contact GitHub"))
    monkeypatch.setattr(api, "_detect_repository", detect)
    base = f"/api/workspaces/{signed_in['workspaces'][0]['id']}"
    r = client.post(base + "/repositories", json={"full_name": "owner/repo", "config": {"review": invalid}})
    assert r.status_code == 422, r.text
    detect.assert_not_called()


@pytest.mark.parametrize("legacy_id", [None, 42])
def test_full_database_ids_match_change_requests_and_target_replies(legacy_id):
    """BigInt strings take precedence over null or obsolete legacy GraphQL Ints."""
    comment_id, review_id = 2**31 + 123, 2**31 + 456
    root = {**inline(), "fullDatabaseId": str(comment_id), "databaseId": legacy_id,
            "pullRequestReview": {"fullDatabaseId": str(review_id), "databaseId": legacy_id}}
    reply = {**inline(11), "fullDatabaseId": str(comment_id + 1), "databaseId": legacy_id,
             "pullRequestReview": {"fullDatabaseId": str(review_id), "databaseId": legacy_id}}
    gh = GitHub("secret")
    gh.request = AsyncMock(side_effect=[
        user("FactoryAccount"), {"user": user("PRAuthor")},
        [{**review(state="CHANGES_REQUESTED"), "id": review_id}],
        {"data": {"repository": {"pullRequest": {"reviewThreads": connection([
            {**thread(), "comments": connection([root], "more-comments")}])}}}},
        {"data": {"node": {"comments": connection([reply])}}},
        [], {"id": 99},
    ])
    findings = asyncio.run(gh.review_feedback("owner/repo", 7, {"address": "changes_requested"}))
    inline_findings = [f for f in findings if f["kind"] == "inline"]
    assert [f["id"] for f in inline_findings] == [f"comment:{comment_id}", f"comment:{comment_id + 1}"]
    assert all(f["review_id"] == str(review_id) for f in findings)
    assert all(f["review_state"] == "CHANGES_REQUESTED" for f in findings)
    assert inline_findings[1]["comment_id"] == str(comment_id + 1)
    assert inline_findings[1]["reply_to_id"] == str(comment_id)
    asyncio.run(gh.reply_review_comment("owner/repo", 7, inline_findings[1]["reply_to_id"], "Fixed"))
    assert gh.request.call_args.args == (
        "POST", f"/repos/owner/repo/pulls/7/comments/{comment_id}/replies")
    # Both the first thread page and nested comment pages request the modern IDs.
    for call in gh.request.call_args_list[3:5]:
        query = call.kwargs["json"]["query"]
        assert "id fullDatabaseId databaseId body" in query
        assert "pullRequestReview { fullDatabaseId databaseId state }" in query


def test_review_patch_preserves_operator_settings_protection(client, signed_in):
    from aifactory.models import Workspace

    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        ws = s.get(Workspace, ws_id)
        ws.settings = {**ws.settings, "_operator_marker": "keep"}
    response = client.patch(f"/api/workspaces/{ws_id}", json={"settings": {
        "_operator_marker": "replace", "_forged": True, "review": {"wait_seconds": 0}}})
    assert response.status_code == 200, response.text
    with session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
        assert settings["_operator_marker"] == "keep"
        assert "_forged" not in settings
        assert settings["review"] == {**DEFAULT_REVIEW, "wait_seconds": 0}
