"""Review policy, normalized GitHub findings and the durable review evidence contract.

Delivery feedback callers persist ``new_review_evidence()`` under
``task.evidence['review']`` (assign a new evidence dict when updating JSON).
GitHub collection errors must propagate; an unavailable snapshot is not an empty one.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, TypedDict

DEFAULT_REVIEW = {"address": "unresolved", "reviewers": "all", "ignore": [],
                  "wait_seconds": 120, "max_rounds": 2}
FACTORY_REPLY_MARKER = "<!-- ai-factory:review-reply -->"


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
                         ("reviewers", ("people", "bots", "all"))):
        if key in raw and raw[key] not in choices:
            raise ValueError(f"review.{key} must be one of {', '.join(choices)}")
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


class FindingProgress(TypedDict):
    """Keyed by stable finding id; retain entries across heads and polling cycles."""
    finding: Finding  # retain location/thread IDs even after it leaves pending
    head: str
    round: int
    status: str  # pending | addressed | disputed
    commit: str | None
    reply_id: str | None
    replied_at: str | None
    resolved_at: str | None  # only set after successful GitHub resolution


class ReviewEvidence(TypedDict):
    rounds: int  # completed review-driven revisions, independent of build attempts
    pending: list[Finding]
    wait_deadlines: dict[str, str]  # head SHA -> UTC ISO deadline; never reset on polling
    active_round: int | None
    active_head: str | None
    findings: dict[str, FindingProgress]  # durable reply/resolution idempotency ledger


def new_review_evidence() -> ReviewEvidence:
    return {"rounds": 0, "pending": [], "wait_deadlines": {}, "active_round": None,
            "active_head": None, "findings": {}}


def _author(record: dict) -> tuple[str, str]:
    author = record.get("user") or record.get("author") or {}
    return author.get("login") or "", author.get("type") or author.get("__typename") or ""


def _bot(login: str, kind: str) -> bool:
    return kind.casefold() == "bot" or login.casefold().endswith("[bot]")


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
    ignored = {s.casefold() for s in policy["ignore"]}
    ignored.update(a.get("login", "").casefold() for a in (identity, pull_author))

    def counted(record: dict, *, human_only: bool = False) -> bool:
        login, kind = _author(record)
        if not login or login.casefold() in ignored or FACTORY_REPLY_MARKER in (record.get("body") or ""):
            return False
        bot = _bot(login, kind)
        return (not human_only or not bot) and (policy["reviewers"] == "all"
                or (policy["reviewers"] == "bots") == bot)

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
                "diff_hunk": record.get("diffHunk") or record.get("diff_hunk")}

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
            if counted(comment, human_only=True) and (comment.get("body") or "").strip():
                item = finding(comment, "issue_comment")
                found[item["id"]] = item
    return list(found.values())
