"""Report schemas each role must return, regardless of harness."""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Sequence

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
                    "key": {"type": "string", "description": "Short id like T1, T2."},
                    "title": {"type": "string"},
                    "repo": {"type": "string", "description": "owner/name of the repository this task changes."},
                    "spec": {"type": "string", "description": "Precise implementation guidance: files, approach, edge cases, tests to add."},
                    "acceptance": {"type": "array", "items": {"type": "string"}, "description": "Verifiable acceptance criteria."},
                    "depends_on": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["key", "title", "repo", "spec", "acceptance", "depends_on"],
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
        "risks": {"type": "array", "items": {"type": "string"}},
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
        "review_responses": {
            "type": "array",
            "description": "Only when the task lists review findings: one answer for every finding id listed.",
            "items": {"type": "object", "properties": {
                "id": {"type": "string", "description": "The finding's id, copied exactly"},
                "action": {"type": "string", "enum": ["fixed", "disputed", "deferred"]},
                "note": {"type": "string", "description": "One or two sentences for the reviewer: what you changed, "
                                                          "or why you did not"}},
                "required": ["id", "action", "note"]},
        },
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

PLAN_REVIEW_REPORT: dict[str, Any] = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["approve", "revise", "escalate"],
                    "description": "approve: building can start. revise: the planner must resolve the findings. "
                                   "escalate: only a person can decide (say what in the summary)."},
        "summary": {"type": "string", "description": "Your assessment in 2-5 sentences, for the planner and the team."},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"severity": {"type": "string", "enum": ["blocker", "major", "minor"]},
                               "task": {"type": "string", "description": "Task key, or empty for the whole plan."},
                               "description": {"type": "string",
                                               "description": "What is wrong and what to change."}},
                "required": ["severity", "description"],
            },
        },
    },
    "required": ["verdict", "summary", "findings"],
}

SCHEMAS = {"planner": PLANNER_REPORT, "builder": BUILDER_REPORT, "verifier": VERIFIER_REPORT,
           "plan_reviewer": PLAN_REVIEW_REPORT}


CRITERION_MATCH = 0.8  # how alike a reported criterion and one of the task's must be to count as the same one


def _criterion_key(text: Any) -> str:
    """A criterion's text reduced to what identifies it: case, spacing, a list number and quoting are ignored."""
    key = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    key = re.sub(r"^(?:criterion )?\(?#?\d{1,3}[.):] ", "", key)
    return key.strip(" \"'`*.…")


def _criterion_number(text: Any) -> int:
    """The list number when a reported criterion only refers to it ("2", "Criterion 2"); otherwise 0."""
    plain = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    found = re.fullmatch(r"(?:(?:criterion|ac) ?)?#?(\d{1,3})[.):]?", plain)
    return int(found.group(1)) if found else 0


def _likeness(a: str, b: str) -> float:
    shorter, longer = sorted((a, b), key=len)
    if len(shorter) >= 24 and shorter in longer:
        return 1.0  # a criterion cut short, or quoted inside a longer sentence
    return SequenceMatcher(None, a, b).ratio()


def uncovered_criteria(expected: Sequence[str], criteria: list[dict[str, Any]]) -> list[tuple[int, str]]:
    """The task's acceptance criteria (list number, text) that a verifier's report has no result for. Each reported
    result counts for one criterion: the one whose text it gives exactly, names by its list number, or rewords
    recognisably. A repeated result, or one that gives another criterion's text, does not cover a second one."""
    wanted = {n: key for n, text in enumerate(expected, 1) if (key := _criterion_key(text))}
    texts = [c.get("criterion") for c in criteria]
    keys = [_criterion_key(text) for text in texts]
    free = list(range(len(criteria)))
    missing = dict(wanted)
    for n, key in wanted.items():  # the criterion's own text
        hit = next((i for i in free if keys[i] == key), None)
        if hit is not None:
            free.remove(hit)
            del missing[n]
    for i in list(free):  # its list number alone
        n = _criterion_number(texts[i])
        if n in missing:
            free.remove(i)
            del missing[n]
    known = set(wanted.values())
    loose = [i for i in free if keys[i] and keys[i] not in known and not _criterion_number(texts[i])]
    for n, key in list(missing.items()):  # a recognisable rewording
        best = max(loose, key=lambda i: _likeness(key, keys[i]), default=None)
        if best is not None and _likeness(key, keys[best]) >= CRITERION_MATCH:
            loose.remove(best)
            del missing[n]
    return [(n, " ".join(str(expected[n - 1]).split())) for n in missing]


def validate_report(role: str, report: Any, expected_criteria: Sequence[str] | None = None,
                    expected_findings: Sequence[str] | None = None) -> tuple[dict[str, Any] | None, str]:
    """Light, forgiving validation: returns (normalized_report, error). ``expected_criteria`` are the task's
    acceptance criteria, when the caller knows them: a verifier's report needs a result of its own for each.
    ``expected_findings`` are the ids of the review findings a builder was given: its report answers each."""
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
            task.setdefault("depends_on", [])
            if task["key"] in keys:
                return None, f"Duplicate task key {task['key']}."
            keys.add(task["key"])
        for task in tasks:
            unknown = [d for d in task["depends_on"] if d not in keys]
            if unknown:
                return None, f"Task {task['key']} depends on unknown tasks {unknown}."
        if _has_cycle(tasks):
            return None, "Task dependencies contain a cycle."
    if role == "verifier":
        if report.get("verdict") not in ("pass", "fail"):
            return None, "verdict must be pass or fail."
        criteria = report.get("criteria") or []
        if not isinstance(criteria, list) or any(not isinstance(c, dict) for c in criteria):
            return None, "criteria must be a list of objects, each with criterion, status and evidence."
        if not isinstance(report.get("findings") or [], list) or any(
                not isinstance(f, dict) for f in report.get("findings") or []):
            return None, "findings must be a list of objects."
        if uncovered := uncovered_criteria(expected_criteria or [], criteria):
            total = sum(1 for text in expected_criteria or [] if _criterion_key(text))
            listed = "; ".join(f"{n}. {text[:200]}" for n, text in uncovered[:5])
            more = f" (and {len(uncovered) - 5} more)" if len(uncovered) > 5 else ""
            return None, (f"The report has results for {total - len(uncovered)} of the task's {total} acceptance "
                          f"criteria ({len(uncovered)} missing). Not covered: {listed}{more}. Report on every "
                          "criterion once, with its text copied exactly, a status and evidence; a result that "
                          "repeats or replaces a criterion does not count for another one. Use unverified for one "
                          "you could not check.")
        unmet = any(c.get("status") != "pass" for c in criteria)
        blocker = any(f.get("severity") == "blocker" for f in report.get("findings") or [])
        if report["verdict"] == "pass" and unmet:
            return None, "A pass verdict requires every criterion to pass. Use fail (or verify further)."
        if report["verdict"] == "pass" and blocker:
            return None, "A pass verdict cannot include blocker findings."
        # Only an unmet criterion or a blocker fails a change; major and minor findings are advice on a pass.
        if report["verdict"] == "fail" and not unmet and not blocker:
            return None, ("A fail verdict needs a criterion that is not met or a blocker finding. Mark the criterion "
                          "that is not met as fail; or raise the finding to blocker if the change must not merge as "
                          "it is; or use pass and keep the findings as advice.")
    if role == "builder" and report.get("status") not in ("done", "blocked"):
        return None, "status must be done or blocked."
    if role == "builder" and report["status"] == "done" and expected_findings:
        answers = report.get("review_responses")
        answered = {a.get("id") for a in (answers if isinstance(answers, list) else []) if isinstance(a, dict)
                    and a.get("action") in ("fixed", "disputed", "deferred") and str(a.get("note") or "").strip()}
        if unanswered := [f for f in expected_findings if f not in answered]:
            more = f" (and {len(unanswered) - 10} more)" if len(unanswered) > 10 else ""
            return None, (f"review_responses must answer every review finding you were given. Not answered: "
                          f"{', '.join(unanswered[:10])}{more}. Give each its id copied exactly, an action (fixed, "
                          "disputed or deferred) and a short note.")
    if role == "plan_reviewer":
        if report.get("verdict") not in ("approve", "revise", "escalate"):
            return None, "verdict must be approve, revise or escalate."
        findings = report.get("findings")
        if not isinstance(findings, list) or any(not isinstance(f, dict) for f in findings):
            return None, "findings must be a list of objects."
        serious = [f for f in findings if f.get("severity") in ("blocker", "major")]
        if report["verdict"] == "approve" and serious:
            return None, "An approve verdict cannot include blocker or major findings. Use revise."
        if report["verdict"] == "revise" and not serious:
            return None, "A revise verdict needs at least one blocker or major finding for the planner to fix."
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
