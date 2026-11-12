"""Review policy, normalized GitHub findings and the durable review evidence contract.

The delivery pipeline's review stage keeps its state under ``task.evidence['review']``
(``ReviewEvidence``; assign a new evidence dict when updating JSON).
GitHub collection errors must propagate; an unavailable snapshot is not an empty one.
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any, TypedDict

# ``request`` is the comment that asks for a review of a head after the pull request's first ("" never asks).
DEFAULT_REVIEW = {"address": "unresolved", "reviewers": "all", "ignore": [],
                  "wait_seconds": 600, "max_rounds": 3, "request": "@codex review",
                  "trust": "collaborators", "block_on": "p1", "on_timeout": "proceed"}
FACTORY_REPLY_MARKER = "<!-- ai-factory:review-reply -->"
TRUSTED_ASSOCIATIONS = ("OWNER", "MEMBER", "COLLABORATOR")
SEVERITIES = ("P0", "P1", "unrated", "P2", "P3")  # most severe first; an unrated finding ranks with a P1
ACTIONS = ("fixed", "disputed", "deferred")  # a builder's answer to a finding
BODY_CHARS, MAX_FINDINGS = 4000, 20  # what one review round carries: per finding body, and findings
_BADGE = re.compile(r"!\[(P[0-3]) Badge\]\(")
_CODEX_STATE = re.compile(r"<!--\s*codex-review-state:v1\s+(\{.*?\})\s*-->", re.S)


def validate_review(raw: Any, *, repository: bool = False) -> dict[str, Any]:
    """Validate a partial policy without coercion; intake opt-ins are repository-only."""
    if not isinstance(raw, dict):
        raise ValueError("review must be an object")
    for key in ("mode", "allow_untrusted_local"):
        if key in raw and not repository:
            raise ValueError(f"review.{key} is only allowed in repository config")
    if raw.keys() - (DEFAULT_REVIEW.keys() | {"mode", "allow_untrusted_local"}):
        raise ValueError("Unknown review policy field")
    for key, choices in (("address", ("unresolved", "changes_requested", "off")),
                         ("reviewers", ("people", "bots", "all")), ("trust", ("collaborators", "anyone")),
                         ("block_on", ("p1", "all", "none")), ("on_timeout", ("proceed", "ask"))):
        if key in raw and raw[key] not in choices:
            raise ValueError(f"review.{key} must be one of {', '.join(choices)}")
    if "request" in raw and (not isinstance(raw["request"], str) or len(raw["request"]) > 500):
        raise ValueError("review.request must be a string of at most 500 characters")
    if "ignore" in raw:
        ignored = raw["ignore"]
        if not isinstance(ignored, list) or any(not isinstance(x, str) or not x.strip() for x in ignored):
            raise ValueError("review.ignore must be a list of nonempty logins")
    for key in ("wait_seconds", "max_rounds"):
        if key in raw and (type(raw[key]) is not int or raw[key] < 0):
            raise ValueError(f"review.{key} must be a nonnegative integer")
    if "mode" in raw and raw["mode"] not in ("off", "all", "labelled"):
        raise ValueError("review.mode must be one of off, all, labelled")
    if "allow_untrusted_local" in raw and type(raw["allow_untrusted_local"]) is not bool:
        raise ValueError("review.allow_untrusted_local must be a boolean")
    result = deepcopy(raw)
    if "ignore" in result:
        result["ignore"] = [login.strip() for login in result["ignore"]]
    if "request" in result:
        result["request"] = result["request"].strip()
    return result


def review_policy(workspace: dict | None = None, repository: dict | None = None) -> dict[str, Any]:
    """Resolve settings/config objects, overriding individual fields, not sections."""
    policy = {**deepcopy(DEFAULT_REVIEW), **validate_review((workspace or {}).get("review", {})),
              **validate_review((repository or {}).get("review", {}), repository=True)}
    # PR intake opt-ins are repository-local, not delivery feedback behavior.
    return {key: policy[key] for key in DEFAULT_REVIEW}


class Finding(TypedDict):
    id: str  # review:<REST id>, comment:<REST id>, or issue_comment:<REST id>
    kind: str  # review | inline | issue_comment
    comment_id: str | None  # REST review comment id, usable for replies
    review_id: str | None  # REST review id (GraphQL fullDatabaseId, or legacy databaseId)
    thread_id: str | None  # GraphQL node id, usable for resolution
    reply_to_id: str | None  # root REST review comment id, never a reply id
    author: str
    author_type: str
    body: str
    url: str
    created_at: str | None
    updated_at: str | None
    review_state: str | None
    resolved: bool | None
    path: str | None
    line: int | None
    original_line: int | None
    diff_hunk: str | None
    severity: str  # P0..P3 from the reviewer's badge, otherwise unrated


class ReviewEvidence(TypedDict, total=False):
    """``task.evidence['review']``: the review stage's durable state (pipeline/delivery.py)."""
    rounds: int  # revisions made because of review: automatic rounds and a person's "request changes"
    answered: dict[str, dict[str, Any]]  # finding id -> {action, note, updated_at}: the builder's latest answer
    head: str  # the head whose review is awaited; the three times below belong to it and survive restarts
    started_at: str
    deadline: str
    next_poll_at: str
    # {head, state: first | intent | posted, at}: the request comment, once per head after the pull request's first
    requested: dict[str, Any]
    # {head, patch_id, state: reviewed | unavailable, reason: timeout | unreadable, waited_seconds, reviewers,
    #  open: [open_findings()], more, counts: {severity: n}, ask: limit | open | unreviewed, of, at}. ``ask``: a
    # person decides the merge; ``of``: the reviewed head this one carries the same change as.
    result: dict[str, Any]
    # {n, head, findings: [ids], replies: {id: {replied, resolved}}, tries}: the revision in progress, kept until
    # its answers are posted on the threads
    round: dict[str, Any] | None
    unanswered: list[str]  # threads the Factory gave up replying on


def new_review_evidence() -> ReviewEvidence:
    return {"rounds": 0, "answered": {}}


def _author(record: dict) -> tuple[str, str]:
    author = record.get("user") or record.get("author") or {}
    return author.get("login") or "", author.get("type") or author.get("__typename") or ""


def _bot(login: str, kind: str) -> bool:
    return kind.casefold() == "bot" or login.casefold().endswith("[bot]")


def _ignored(policy: dict, identity: dict, pull_author: dict) -> set[str]:
    ignored = {s.casefold() for s in policy["ignore"]}
    ignored.update(a.get("login", "").casefold() for a in (identity, pull_author))
    return ignored


def _counted(record: dict, policy: dict, ignored: set[str], *, human_only: bool = False) -> bool:
    """Whether a review or comment counts: not the Factory's own, from an allowed kind of reviewer, and trusted (an
    installed bot or a collaborator; the repository may be public, where anyone can comment)."""
    login, kind = _author(record)
    if not login or login.casefold() in ignored or FACTORY_REPLY_MARKER in (record.get("body") or ""):
        return False
    bot = _bot(login, kind)
    association = str(record.get("author_association") or record.get("authorAssociation") or "").upper()
    if policy["trust"] != "anyone" and not bot and association not in TRUSTED_ASSOCIATIONS:
        return False
    return (not human_only or not bot) and (policy["reviewers"] == "all"
            or (policy["reviewers"] == "bots") == bot)


def severity(body: str) -> str:
    """Codex marks a finding with a badge image in its body (``![P1 Badge](…)``); anything else is unrated."""
    found = _BADGE.search(body or "")
    return found.group(1) if found else "unrated"


def blocking(level: str, block_on: str) -> bool:
    return block_on == "all" or (block_on == "p1" and level not in ("P2", "P3"))


def codex_review_state(body: str) -> dict[str, Any] | None:
    """The commit a completed Codex review covered, from the state comment Codex leaves on a pull request (it posts
    no review when it finds nothing). None for anything that is not exactly that."""
    found = _CODEX_STATE.search(body or "")
    if not found:
        return None
    try:
        state = json.loads(found.group(1))
    except ValueError:
        return None
    review = state.get("code_review") if isinstance(state, dict) else None
    if not isinstance(review, dict) or review.get("status") != "completed":
        return None
    sha = review.get("commit_sha")
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        return None
    return {"commit_sha": sha, "has_any_findings": bool(review.get("has_any_findings"))}


def _head_reviews(head: str, reviews: list[dict], comments: list[dict], policy: dict, identity: dict,
                  pull_author: dict) -> tuple[list[str], list[str]]:
    """Who submitted a review of exactly this commit, and whose Codex state comment says its review of it is
    complete: (login, whether it reports findings). Trusted reviewers only."""
    policy = {**deepcopy(DEFAULT_REVIEW), **validate_review(policy)}
    ignored = _ignored(policy, identity, pull_author)
    submitted = [_author(r)[0] for r in reviews if r.get("commit_id") == head
                 and str(r.get("state") or "").upper() not in ("PENDING", "DISMISSED") and _counted(r, policy, ignored)]
    stated = []
    for c in comments:
        state = codex_review_state(c.get("body") or "") or {}
        if state.get("commit_sha") == head and _counted(c, policy, ignored):
            stated.append((_author(c)[0], state["has_any_findings"]))
    return submitted, stated


def reviewed_by(head: str, *, reviews: list[dict], comments: list[dict], policy: dict, identity: dict,
                pull_author: dict) -> list[str]:
    """Logins of the trusted reviewers who reviewed exactly this head: a submitted review of that commit, or a
    Codex state comment naming it that reports no findings (Codex submits no review then). A state comment that
    reports findings is not a review by itself: the findings are in the review Codex submitted for the commit,
    and until that review is read nothing is known about them (see findings_unseen)."""
    submitted, stated = _head_reviews(head, reviews, comments, policy, identity, pull_author)
    return list(dict.fromkeys(submitted + [login for login, findings in stated if not findings]))


def findings_unseen(head: str, *, reviews: list[dict], comments: list[dict], policy: dict, identity: dict,
                    pull_author: dict) -> bool:
    """Whether a reviewer reports findings on this head whose review is not among ``reviews``: what was read of the
    pull request is then not the whole review, and a head must never count as clean on it."""
    submitted, stated = _head_reviews(head, reviews, comments, policy, identity, pull_author)
    return any(findings and login not in submitted for login, findings in stated)


def open_findings(findings: list[Finding], policy: dict, answered: dict[str, dict]) -> list[dict[str, Any]]:
    """What the review stage keeps of each open finding, blocking ones first and then by severity. A finding the
    builder answered keeps that answer and no longer blocks until its reviewer writes again: a new comment is a new
    finding, and an edited one no longer matches the answer."""
    found = []
    for f in findings:
        answer = (answered or {}).get(f["id"]) or {}
        answer = answer if answer.get("updated_at") == f["updated_at"] else None
        found.append({
            "id": f["id"], "severity": f["severity"], "author": f["author"], "path": f["path"],
            "line": f["line"] or f["original_line"], "url": f["url"],
            "body": f["body"] if len(f["body"]) <= BODY_CHARS else f["body"][:BODY_CHARS] + " … [cut here]",
            "thread_id": f["thread_id"], "reply_to_id": f["reply_to_id"], "updated_at": f["updated_at"],
            "blocking": not answer and blocking(f["severity"], policy["block_on"]),
            "answer": {"action": answer.get("action"), "note": answer.get("note") or ""} if answer else None})
    return sorted(found, key=lambda f: (not f["blocking"], SEVERITIES.index(f["severity"])))


def headline(body: str, limit: int = 120) -> str:
    """A finding's first words for a one-line listing, without the badge markup Codex opens with."""
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)|</?sub>", "", body or "").replace("**", "")
    return " ".join(text.split())[:limit]


def reply_text(action: str, note: str, head: str) -> str:
    """What the Factory writes on a finding's thread once the builder's answer to it is published."""
    note = " ".join(str(note or "").split())[:1000]
    return f"Fixed in {head[:10]}: {note}" if action == "fixed" else f"Not changed: {note}"


def requested_round(evidence: dict | None, head: str) -> dict:
    """A person's "request changes" is a review round too: it counts, and the builder answers whatever the review
    of that head left open. A task that never reached the review stage is left as it is."""
    review = (evidence or {}).get("review")
    if not review:
        return evidence or {}
    result = review.get("result") or {}
    rounds = int(review.get("rounds", 0)) + 1
    ids = [f["id"] for f in result.get("open") or []] if result.get("head") == head else []
    return {**evidence, "review": {**review, "rounds": rounds, "round": {
        "n": rounds, "head": head, "findings": ids, "replies": {}, "tries": 0} if ids else None}}


def _database_id(record: dict) -> int | None:
    """GitHub's legacy GraphQL Int is null for IDs above 2**31.

    fullDatabaseId is a BigInt scalar and may be encoded as a decimal string.
    Never fall back to a GraphQL node ID for a REST reply or review match.
    """
    value = record.get("fullDatabaseId")
    if value is None:
        value = record.get("databaseId")
    return int(value) if value is not None else None


def normalize_feedback(*, reviews: list[dict], threads: list[dict], comments: list[dict],
                       policy: dict, identity: dict, pull_author: dict) -> list[Finding]:
    """Normalize a complete snapshot and apply policy in one place.

    COMMENTED prose is never actionable. Only the latest decisive submitted review
    per author can request changes; COMMENTED does not withdraw a change request.
    An unresolved thread is actionable independently of COMMENTED prose.
    """
    policy = {**deepcopy(DEFAULT_REVIEW), **validate_review(policy)}
    if policy["address"] == "off":
        return []
    ignored = _ignored(policy, identity, pull_author)

    def counted(record: dict, *, human_only: bool = False) -> bool:
        return _counted(record, policy, ignored, human_only=human_only)

    submitted = [r for r in reviews if r.get("state", "").upper() != "PENDING"]
    submitted.sort(key=lambda r: (r.get("submitted_at") or "", int(r["id"])))
    by_id = {str(r["id"]): r for r in submitted}
    latest = {}
    for r in submitted:
        if r.get("state", "").upper() in ("CHANGES_REQUESTED", "APPROVED", "DISMISSED"):
            latest[_author(r)[0].casefold()] = str(r["id"])
    current = {rid for rid, r in by_id.items() if r.get("state", "").upper() == "CHANGES_REQUESTED"
               and latest.get(_author(r)[0].casefold()) == rid}

    def finding(record: dict, kind: str, rid: str | None = None, thread: dict | None = None,
                root: str | None = None) -> Finding:
        login, author_type = _author(record)
        cid = str(_database_id(record) or record["id"])
        return {"id": f"{'comment' if kind == 'inline' else kind}:{cid}", "kind": kind,
                "comment_id": cid if kind == "inline" else None, "review_id": rid,
                "thread_id": thread["id"] if thread else None, "reply_to_id": root,
                "author": login, "author_type": author_type, "body": record.get("body") or "",
                "url": record.get("url") or record.get("html_url") or "",
                "created_at": record.get("createdAt") or record.get("created_at") or record.get("submitted_at"),
                "updated_at": record.get("updatedAt") or record.get("updated_at"),
                "review_state": by_id[rid]["state"].upper() if rid in by_id else None,
                "resolved": thread["isResolved"] if thread else None,
                "path": record.get("path") or (thread or {}).get("path"),
                "line": record.get("line", (thread or {}).get("line")),
                "original_line": record.get("originalLine", record.get("original_line", (thread or {}).get("originalLine"))),
                "diff_hunk": record.get("diffHunk") or record.get("diff_hunk"),
                "severity": severity(record.get("body") or "")}

    found: dict[str, Finding] = {}
    for thread in threads:
        if thread["isResolved"]:
            continue
        records = thread["comments"]["nodes"]
        if not records:
            continue
        root = str(_database_id(records[0]) or records[0]["id"])
        for record in records:
            review = record.get("pullRequestReview") or {}
            rid_value = _database_id(review) or record.get("pull_request_review_id")
            rid = str(rid_value) if rid_value is not None else None
            state = by_id.get(rid, {}).get("state", "").upper()
            if state == "DISMISSED" or (state == "CHANGES_REQUESTED" and rid not in current):
                continue
            if policy["address"] == "changes_requested" and rid not in current:
                continue
            if counted(record) and (record.get("body") or "").strip():
                item = finding(record, "inline", rid, thread, root)
                found[item["id"]] = item
    # Prefer the precise inline location if a review repeats the same prose.
    inline_bodies = {(f["review_id"], f["author"].casefold(), f["body"].strip()) for f in found.values()}
    for rid, review in by_id.items():
        body = (review.get("body") or "").strip()
        if rid in current and counted(review) and body and (rid, _author(review)[0].casefold(), body) not in inline_bodies:
            item = finding(review, "review", rid)
            found[item["id"]] = item
    if policy["address"] == "unresolved":
        for comment in comments:
            if (comment.get("body") or "").strip().casefold() == policy["request"].casefold():
                continue  # someone asking for a review, as the Factory does, is not a finding
            if counted(comment, human_only=True) and (comment.get("body") or "").strip():
                item = finding(comment, "issue_comment")
                found[item["id"]] = item
    return list(found.values())
