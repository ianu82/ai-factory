"""Report schemas each role must return, regardless of harness."""

from __future__ import annotations

import fnmatch
import re
from typing import Any

PLANNER_REPORT: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string", "description": "What will be delivered and how, in 2-5 sentences."},
        "tasks": {
            "type": "array",
            "description": "Independent, reviewable units of work. Prefer the fewest tasks that can run in parallel.",
            "items": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "Short id like T1, T2 (at most 20 characters)."},
                    "title": {"type": "string"},
                    "repo": {"type": "string", "description": "owner/name of the repository this task changes."},
                    "spec": {"type": "string", "description": "Precise implementation guidance: files, approach, edge cases, tests to add."},
                    "acceptance": {"type": "array", "items": {"type": "string"}, "description": "Verifiable acceptance criteria."},
                    "constraints": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Security and robustness rules this change must keep, each concrete and checkable "
                                       "by a test or probe: untrusted input, secrets, isolation (fail closed), bounds, "
                                       "concurrency and restarts, failure behaviour, and prior decisions that settle the "
                                       "design.",
                    },
                    "files": {"type": "array", "items": {"type": "string"},
                              "description": "Paths (or globs) of the files this task will change."},
                    "depends_on": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["key", "title", "repo", "spec", "acceptance", "constraints", "files", "depends_on"],
            },
        },
        "questions": {
            "type": "array",
            "description": "Only genuine product decisions that block planning. Leave empty when you can proceed safely.",
            "items": {
                "type": "object",
                "properties": {"question": {"type": "string"}, "why": {"type": "string"},
                               "options": {"type": "array", "items": {"type": "string"}}},
                "required": ["question", "why"],
            },
        },
        "risks": {"type": "array", "items": {"type": "string"},
                  "description": "Assumptions you made and the residual risks of this plan."},
    },
    "required": ["summary", "tasks", "questions"],
}

BUILDER_REPORT: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["done", "blocked"]},
        "summary": {"type": "string", "description": "What changed and why, for the pull request description."},
        "tests": {"type": "array", "items": {"type": "string"}, "description": "Commands you ran and their results."},
        "criteria": {
            "type": "array",
            "items": {"type": "object", "properties": {"criterion": {"type": "string"}, "how": {"type": "string"}},
                      "required": ["criterion", "how"]},
        },
        "blocked_reason": {"type": "string"},
        "runtime_checks": {
            "type": "array",
            "description": "When the app is booted for runtime QA: up to 5 HTTP requests against the running app "
                           "that demonstrate this change works, with the status each should return.",
            "items": {"type": "object", "properties": {
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]},
                "path": {"type": "string", "description": "Path on the app, starting with /"},
                "body": {"type": "string", "description": "JSON request body as text, or empty"},
                "expect_status": {"type": "integer"},
                "why": {"type": "string", "description": "What this request proves about the change"}},
                "required": ["method", "path", "expect_status", "why"]},
        },
        "runtime_pages": {"type": "array", "items": {"type": "string"},
                          "description": "Up to 4 page paths worth a screenshot because this change affects them."},
    },
    "required": ["status", "summary"],
}

VERIFIER_REPORT: dict[str, Any] = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["pass", "fail"]},
        "summary": {"type": "string"},
        "criteria": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "criterion": {"type": "string"},
                    "status": {"type": "string", "enum": ["pass", "fail", "unverified"]},
                    "evidence": {"type": "string", "description": "Concrete evidence: commands run, output, file:line."},
                },
                "required": ["criterion", "status", "evidence"],
            },
        },
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"severity": {"type": "string", "enum": ["blocker", "major", "minor"]},
                               "description": {"type": "string"}, "location": {"type": "string"}},
                "required": ["severity", "description"],
            },
        },
    },
    "required": ["verdict", "summary", "criteria"],
}

SCHEMAS = {"planner": PLANNER_REPORT, "builder": BUILDER_REPORT, "verifier": VERIFIER_REPORT}


TASK_KEY_LIMIT = 20  # Task.key column width


def validate_report(role: str, report: Any) -> tuple[dict[str, Any] | None, str]:
    """Light, forgiving validation: returns (normalized_report, error)."""
    if not isinstance(report, dict):
        return None, "The report must be a JSON object."
    schema = SCHEMAS[role]
    missing = [k for k in schema["required"] if k not in report]
    if missing:
        return None, f"The report is missing required fields: {', '.join(missing)}."
    if role == "planner":
        tasks = report.get("tasks")
        if not isinstance(tasks, list):
            return None, "tasks must be a list."
        if not tasks and not report.get("questions"):
            return None, "Provide at least one task, or questions explaining what blocks planning."
        keys = set()
        for i, task in enumerate(tasks):
            if not isinstance(task, dict):
                return None, f"tasks[{i}] must be an object."
            for field in ("key", "title", "repo", "spec", "acceptance"):
                if not task.get(field):
                    return None, f"tasks[{i}] is missing {field}."
            for field in ("constraints", "files"):
                values = task.get(field)
                if not isinstance(values, list) or not values or not all(isinstance(v, str) and v.strip() for v in values):
                    return None, (f"tasks[{i}] needs {field}: a non-empty list of strings. "
                                  + ("Write the concrete security and robustness rules this change must keep."
                                     if field == "constraints" else "List the files this task will change."))
            task.setdefault("depends_on", [])
            if len(str(task["key"])) > TASK_KEY_LIMIT:
                return None, f"tasks[{i}] key {task['key']!r} is longer than {TASK_KEY_LIMIT} characters; use a short key like T1."
            if task["key"] in keys:
                return None, f"Duplicate task key {task['key']}."
            keys.add(task["key"])
        for task in tasks:
            unknown = [d for d in task["depends_on"] if d not in keys]
            if unknown:
                return None, f"Task {task['key']} depends on unknown tasks {unknown}."
        if _has_cycle(tasks):
            return None, "Task dependencies contain a cycle."
        report.setdefault("risks", [])
        sequence_shared_files(tasks)
    if role == "verifier":
        if report.get("verdict") not in ("pass", "fail"):
            return None, "verdict must be pass or fail."
        criteria = report.get("criteria") or []
        if report["verdict"] == "pass" and any(c.get("status") != "pass" for c in criteria if isinstance(c, dict)):
            return None, "A pass verdict requires every criterion to pass. Use fail (or verify further)."
        if report["verdict"] == "pass" and any(
            isinstance(f, dict) and f.get("severity") == "blocker" for f in report.get("findings") or []
        ):
            return None, "A pass verdict cannot include blocker findings."
    if role == "builder" and report.get("status") not in ("done", "blocked"):
        return None, "status must be done or blocked."
    return report, ""


def _has_cycle(tasks: list[dict[str, Any]]) -> bool:
    graph = {t["key"]: list(t.get("depends_on") or []) for t in tasks}
    state: dict[str, int] = {}

    def visit(node: str) -> bool:
        if state.get(node) == 1:
            return True
        if state.get(node) == 2:
            return False
        state[node] = 1
        if any(visit(n) for n in graph.get(node, [])):
            return True
        state[node] = 2
        return False

    return any(visit(k) for k in graph)


def _ancestors(tasks: list[dict[str, Any]], key: str) -> set[str]:
    graph = {t["key"]: list(t.get("depends_on") or []) for t in tasks}
    seen: set[str] = set()
    stack = list(graph.get(key, []))
    while stack:
        node = stack.pop()
        if node not in seen:
            seen.add(node)
            stack.extend(graph.get(node, []))
    return seen


def sequence_shared_files(tasks: list[dict[str, Any]]) -> None:
    """Tasks in one repository that change the same file run one after the other.

    Parallel edits to one file conflict when the first merges, and the rebuild repeats the work. A later task
    (in plan order) gains a dependency on an earlier one it overlaps, unless one already depends on the other.
    Safe to run again (planning does after it normalizes repository names): it never adds a cycle or a repeat.
    """
    for i, later in enumerate(tasks):
        mine = {f.strip().lstrip("./") for f in later.get("files") or []}
        for earlier in tasks[:i]:
            if earlier.get("repo", "").lower() != later.get("repo", "").lower():
                continue
            theirs = {f.strip().lstrip("./") for f in earlier.get("files") or []}
            if not _overlap(mine, theirs):
                continue
            if earlier["key"] in _ancestors(tasks, later["key"]) or later["key"] in _ancestors(tasks, earlier["key"]):
                continue
            later["depends_on"] = [*later.get("depends_on", []), earlier["key"]]


_WILDCARD = re.compile(r"[*?\[]")


def _literal_prefix(pattern: str) -> str:
    match = _WILDCARD.search(pattern)
    return pattern if match is None else pattern[:match.start()]


def _overlap(a: set[str], b: set[str]) -> bool:
    """Paths or globs (either side) that could name the same file. Two globs overlap conservatively when their
    literal prefixes nest (src/*.py and src/model* could both be src/models.py)."""
    for x in a:
        for y in b:
            if x == y or fnmatch.fnmatchcase(x, y) or fnmatch.fnmatchcase(y, x):
                return True
            if _WILDCARD.search(x) and _WILDCARD.search(y):
                px, py = _literal_prefix(x), _literal_prefix(y)
                if px.startswith(py) or py.startswith(px):
                    return True
    return False


_CONSTRAINT_LABEL = re.compile(r"^\W*C(\d+)\b", re.IGNORECASE)
_NON_WORD = re.compile(r"[\W_]+")  # Unicode-aware: letters and digits in any script are kept


def _words(text: str) -> str:
    return " " + _NON_WORD.sub(" ", text.casefold()).strip() + " "


def _judges(entry: str, requirement: str) -> bool:
    """Whether a report entry (normalized) repeats a requirement's full text, as the verifier is told to."""
    want = _words(requirement)
    return bool(want.strip()) and want in entry


def missing_coverage(acceptance: list[str], constraints: list[str], criteria: list[Any]) -> list[str]:
    """What a passing verifier report left unjudged. Every constraint and every acceptance criterion needs its own
    report entry: a constraint by its label (C1, C2, … up to the number of constraints) or its full text, a
    criterion by its full text (the verifier is told to repeat it verbatim). One entry never counts for two
    requirements. A pass that skips any of them is not evidence."""
    reported = [str(c.get("criterion", "")).strip() for c in criteria if isinstance(c, dict)]
    entries = [_words(r) for r in reported]
    labels = [int(m.group(1)) if (m := _CONSTRAINT_LABEL.match(r)) else 0 for r in reported]
    required: list[tuple[str, list[int]]] = []
    for j, text in enumerate(constraints):
        required.append((f"C{j + 1}", [i for i, e in enumerate(entries) if labels[i] == j + 1 or _judges(e, text)]))
    for k, text in enumerate(acceptance):
        required.append((f"acceptance criterion {k + 1}", [i for i, e in enumerate(entries) if _judges(e, text)]))
    owner: dict[int, int] = {}  # report entry -> requirement it counts for (maximum bipartite matching)

    def assign(req: int, seen: set[int]) -> bool:
        for i in required[req][1]:
            if i not in seen:
                seen.add(i)
                if i not in owner or assign(owner[i], seen):
                    owner[i] = req
                    return True
        return False

    return [name for n, (name, _) in enumerate(required) if not assign(n, set())]
