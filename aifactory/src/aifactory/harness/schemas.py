"""Report schemas each role must return, regardless of harness."""

from __future__ import annotations

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
