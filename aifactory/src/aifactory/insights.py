"""Improvement proposals from the Factory's own history.

The Factory measures how delivery actually goes in a workspace (attributed human decisions,
verifier findings, failed checks, runtime QA, budgets, harness outcomes) and proposes
concrete changes to how it works there. A proposal is a Signal with source ``improvement``:
it carries its evidence and one applicable action, and nothing changes until a person with
the right role applies it. Each rule needs a minimum sample before it speaks, and a proposal
someone ignored stays quiet until the evidence behind it changes.
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy.orm import Session

from .db import utcnow
from .events import notify, record
from .models import Decision, Event, Outcome, Repository, Run, Signal, Task, User, Workspace
from .pipeline.runtime import merged_settings
from .signals import fingerprint

log = logging.getLogger(__name__)

WINDOW_DAYS = 30
ACTION_TYPES = ("settings", "repo_config", "team_note", "review")


@dataclass
class Proposal:
    rule: str
    key: str  # stable identity within the rule (repo id, setting path…)
    title: str
    body: str
    action: dict[str, Any]
    repo_id: str | None = None
    evidence: list[dict[str, Any]] = field(default_factory=list)
    basis: str = ""  # changes when the evidence materially changes

    @property
    def fingerprint(self) -> str:
        return fingerprint("improvement", self.rule, self.key)


# --- rules ---------------------------------------------------------------------------------
def _since():
    return utcnow() - timedelta(days=WINDOW_DAYS)


def _label(db: Session, task: Task) -> str:
    outcome = db.get(Outcome, task.outcome_id)
    return f"#{outcome.number} {task.key}" if outcome else task.key


def _outcome_title(db: Session, outcome_id: str | None) -> str:
    outcome = db.get(Outcome, outcome_id) if outcome_id else None
    return outcome.title if outcome else ""


def _names(db: Session, user_ids: set[str]) -> list[str]:
    users = db.query(User).filter(User.id.in_(user_ids)).all() if user_ids else []
    return sorted({u.name or u.email for u in users})


def _merge_streaks(db: Session, ws: Workspace, repos: dict[str, Repository]) -> list[Proposal]:
    """A repository whose merges people always approve, and that never needed recovery, can merge on its own."""
    settings = merged_settings(ws.settings)
    out = []
    decisions = (db.query(Decision).filter(Decision.workspace_id == ws.id, Decision.kind == "approve_merge",
                                           Decision.status == "resolved")
                 .order_by(Decision.resolved_at.desc()).limit(200).all())
    by_repo: dict[str, list[tuple[Decision, Task]]] = defaultdict(list)
    for d in decisions:
        task = db.get(Task, d.task_id) if d.task_id else None
        if task and task.repo_id in repos:
            by_repo[task.repo_id].append((d, task))
    for repo_id, rows in by_repo.items():
        repo = repos[repo_id]
        mode = ((repo.config or {}).get("merge") or {}).get("mode") or settings["autonomy"].get("merge", "auto")
        if mode != "approval":
            continue
        streak = []
        for d, task in rows:  # newest first
            if d.resolution != "approve" or task.status in ("smoke_failed", "reverted"):
                break
            streak.append((d, task))
        if len(streak) < 5:
            continue
        approvers = _names(db, {d.resolved_by for d, _ in streak if d.resolved_by})
        evidence = [{"label": f"{_label(db, task)} · PR #{task.pr_number}" if task.pr_number else _label(db, task),
                     "detail": f"approved {d.resolved_at:%b %d}", "outcome_id": task.outcome_id, "task_id": task.id,
                     "url": task.pr_url} for d, task in streak[:8]]
        out.append(Proposal(
            rule="merge_autonomy", key=repo_id, repo_id=repo_id,
            title=f"Let {repo.full_name} merge verified changes without approval",
            body=(f"The last {len(streak)} merges the Factory asked about in {repo.full_name} were all approved"
                  f" without changes (by {', '.join(approvers) or 'your team'}), and none of them needed a revert or"
                  " failed post-merge smoke. Verification, CI and smoke still gate every merge; this only removes"
                  " the wait for a click. You can switch back at any time in repository settings."),
            action={"type": "repo_config", "repo_id": repo_id, "set": {"merge": {"mode": "auto"}},
                    "label": "Merge automatically"},
            evidence=evidence, basis=str(len(streak) // 5)))
    return out


def _plan_streak(db: Session, ws: Workspace) -> list[Proposal]:
    settings = merged_settings(ws.settings)
    if settings["autonomy"].get("plan") != "approval":
        return []
    rows = (db.query(Decision).filter(Decision.workspace_id == ws.id, Decision.kind == "approve_plan",
                                      Decision.status == "resolved")
            .order_by(Decision.resolved_at.desc()).limit(50).all())
    streak = []
    for d in rows:
        if d.resolution != "approve" or (d.comment or "").strip():
            break
        streak.append(d)
    if len(streak) < 5:
        return []
    approvers = _names(db, {d.resolved_by for d in streak if d.resolved_by})
    return [Proposal(
        rule="plan_autonomy", key=ws.id,
        title="Start delivery as soon as a plan is ready",
        body=(f"The last {len(streak)} plans were approved exactly as proposed (by {', '.join(approvers) or 'your team'})."
              " Planner questions that need a person still stop and ask; only the routine approval goes away."),
        action={"type": "settings", "set": {"autonomy.plan": "auto"}, "label": "Approve plans automatically"},
        evidence=[{"label": d.title, "detail": f"approved {d.resolved_at:%b %d}", "outcome_id": d.outcome_id}
                  for d in streak[:8]],
        basis=str(len(streak) // 5))]


_WORD = re.compile(r"[a-z0-9_./-]+")


def _gist(text: str) -> str:
    words = _WORD.findall((text or "").lower())
    return " ".join(words[:12])


def _review_themes(db: Session, ws: Workspace, repos: dict[str, Repository]) -> list[Proposal]:
    """Blocking findings that reviewers raise again and again become standing guidance for builders."""
    runs = (db.query(Run).filter(Run.workspace_id == ws.id, Run.role == "verifier", Run.started_at >= _since())
            .order_by(Run.started_at.desc()).limit(300).all())
    out = []
    per_repo: dict[str, list[tuple[Run, Task, dict[str, Any]]]] = defaultdict(list)
    for run in runs:
        report = run.result or {}
        if report.get("verdict") != "fail":
            continue
        task = db.get(Task, run.task_id) if run.task_id else None
        if not task or task.repo_id not in repos:
            continue
        for finding in report.get("findings") or []:
            if isinstance(finding, dict) and finding.get("severity") in ("blocker", "major") and finding.get("description"):
                per_repo[task.repo_id].append((run, task, finding))
    for repo_id, rows in per_repo.items():
        tasks = {task.id for _, task, _ in rows}
        if len(tasks) < 3:
            continue
        seen: set[str] = set()
        bullets, evidence = [], []
        for run, task, finding in rows:
            gist = _gist(finding["description"])
            if not gist or gist in seen:
                continue
            seen.add(gist)
            text = " ".join(str(finding["description"]).split())
            bullets.append(f"- {text[:280]}")
            evidence.append({"label": f"{_label(db, task)} · attempt {run.attempt}", "detail": text[:160],
                             "outcome_id": task.outcome_id, "task_id": task.id})
            if len(bullets) >= 6:
                break
        repo = repos[repo_id]
        note = ("Independent review has repeatedly rejected changes in this repository for the following;"
                " check for each before reporting done:\n" + "\n".join(bullets))
        out.append(Proposal(
            rule="review_themes", key=repo_id, repo_id=repo_id,
            title=f"Teach builders what reviewers keep rejecting in {repo.full_name}",
            body=(f"Verifiers rejected {len(tasks)} tasks in {repo.full_name} over the last {WINDOW_DAYS} days with blocking"
                  " findings. Adding them to the repository's team notes puts them in front of every builder (and"
                  " verifier) from the first attempt. Edit the note before applying it."),
            action={"type": "team_note", "repo_id": repo_id, "note": note, "label": "Add to team notes"},
            evidence=evidence, basis=str(len(tasks) // 3)))
    return out


def _check_failures(db: Session, ws: Workspace, repos: dict[str, Repository]) -> list[Proposal]:
    """A required check that builders keep failing means they are not running it before they finish."""
    events = (db.query(Event).filter(Event.workspace_id == ws.id, Event.kind == "check_result", Event.level == "warn",
                                     Event.created_at >= _since()).limit(2000).all())
    counts: dict[tuple[str, str], set[str]] = defaultdict(set)
    samples: dict[tuple[str, str], list[Event]] = defaultdict(list)
    tasks: dict[str, Task] = {}
    for e in events:
        command = (e.data or {}).get("command")
        task = tasks.get(e.task_id) or (db.get(Task, e.task_id) if e.task_id else None)
        if not command or not task or task.repo_id not in repos:
            continue
        if command not in ((repos[task.repo_id].config or {}).get("checks") or []):
            continue  # the repository no longer runs this check: old failures say nothing about today
        tasks[task.id] = task
        counts[(task.repo_id, command)].add(task.id)
        samples[(task.repo_id, command)].append(e)
    out = []
    for (repo_id, command), task_ids in counts.items():
        failures = len(samples[(repo_id, command)])
        if len(task_ids) < 2 or failures < 3:
            continue
        repo = repos[repo_id]
        note = f"Run `{command}` yourself and make it pass before you report a change as done."
        if note in ((repo.config or {}).get("context") or ""):
            continue
        out.append(Proposal(
            rule="check_failures", key=f"{repo_id}:{command}", repo_id=repo_id,
            title=f"Builders keep failing `{command}` in {repo.full_name}",
            body=(f"`{command}` failed {failures} times across {len(task_ids)} tasks in the last {WINDOW_DAYS} days, each"
                  " costing a full extra attempt. A standing note asks builders to run it before finishing."),
            action={"type": "team_note", "repo_id": repo_id, "note": note, "label": "Add to team notes"},
            evidence=[{"label": _label(db, tasks[e.task_id]),
                       "detail": f"failed {e.created_at:%b %d} · {_outcome_title(db, e.outcome_id)}"[:160],
                       "outcome_id": e.outcome_id, "task_id": e.task_id} for e in samples[(repo_id, command)][:6]],
            basis=str(failures // 3)))
    return out


def _qa_boot(db: Session, ws: Workspace, repos: dict[str, Repository]) -> list[Proposal]:
    """Runtime QA that cannot start the app gives reviewers no evidence; the configuration needs a person."""
    events = (db.query(Event).filter(Event.workspace_id == ws.id, Event.kind.in_(("qa_app_failed", "qa_app_ready")),
                                     Event.created_at >= _since()).order_by(Event.created_at.desc()).limit(400).all())
    per_repo: dict[str, list[Event]] = defaultdict(list)
    for e in events:
        task = db.get(Task, e.task_id) if e.task_id else None
        if task and task.repo_id in repos:
            per_repo[task.repo_id].append(e)
    out = []
    for repo_id, rows in per_repo.items():
        recent = rows[:6]
        failed = [e for e in recent if e.kind == "qa_app_failed"]
        if len(failed) < 2 or len(failed) * 2 < len(recent):
            continue
        repo = repos[repo_id]
        start = ((repo.config or {}).get("qa") or {}).get("start", "")
        out.append(Proposal(
            rule="qa_boot", key=repo_id, repo_id=repo_id,
            title=f"Runtime QA cannot start {repo.full_name}",
            body=(f"In {len(failed)} of the last {len(recent)} verifications the app did not become ready"
                  + (f" (start command: `{start}`)" if start else "")
                  + ", so reviewers had no screenshots or probes to look at. The latest"
                  " boot output is in the evidence. Usually the start command, a missing service (database), or"
                  " the ready path needs adjusting in the repository's QA configuration."),
            action={"type": "review", "repo_id": repo_id, "label": "Open repository settings",
                    "path": "settings"},
            evidence=[{"label": e.created_at.strftime("%b %d %H:%M"), "detail": e.message[:240],
                       "outcome_id": e.outcome_id, "task_id": e.task_id} for e in failed[:5]],
            basis=str(len(failed) // 2)))
    return out


def _budget_hits(db: Session, ws: Workspace) -> list[Proposal]:
    settings = merged_settings(ws.settings)
    runs = (db.query(Run).filter(Run.workspace_id == ws.id, Run.started_at >= _since(),
                                 Run.error.like("%budget of%reached%")).limit(100).all())
    cost_hits = [r for r in runs if "Cost budget" in r.error]
    if len(cost_hits) < 2:
        return []
    current = float(settings["budgets"].get("run_usd") or 0)
    proposed = round(max(current * 1.5, current + 2), 2)
    return [Proposal(
        rule="run_budget", key=ws.id,
        title=f"Raise the per-session budget from ${current:.2f} to ${proposed:.2f}",
        body=(f"{len(cost_hits)} agent sessions stopped at the ${current:.2f} cost budget in the last {WINDOW_DAYS} days;"
              " each restart costs more than finishing would have. The outcome budget still caps total spend."),
        action={"type": "settings", "set": {"budgets.run_usd": proposed}, "label": f"Raise to ${proposed:.2f}"},
        evidence=[{"label": f"{r.role} · {r.harness}", "detail": f"${r.cost_usd:.2f} · {r.error[:120]}",
                   "outcome_id": r.outcome_id, "task_id": r.task_id} for r in cost_hits[:6]],
        basis=f"{current}:{len(cost_hits) // 2}")]


def _harness_results(db: Session, ws: Workspace) -> list[Proposal]:
    """Compare builders on finished work: first-attempt verification rate and cost per delivered task."""
    settings = merged_settings(ws.settings)
    default = settings["harness"].get("builder") or "native"
    tasks = (db.query(Task).filter(Task.workspace_id == ws.id, Task.finished_at.isnot(None), Task.finished_at >= _since(),
                                   Task.status.in_(("completed", "failed", "smoke_failed", "reverted", "blocked")))
             .limit(500).all())
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"n": 0, "delivered": 0, "first": 0, "cost": 0.0})
    for t in tasks:
        s = stats[t.harness]
        s["n"] += 1
        attempts = [a for a in (t.evidence or {}).get("attempts", []) if isinstance(a, dict)]
        verified = [a for a in attempts if a.get("result") == "verified"]
        if t.status == "completed":
            s["delivered"] += 1
        if verified and verified[0].get("n") == 1:
            s["first"] += 1
        s["cost"] += sum(r.cost_usd or 0 for r in db.query(Run).filter(Run.task_id == t.id).all())
    base = stats.get(default)
    if not base or base["n"] < 4:
        return []
    out = []
    for name, s in stats.items():
        if name == default or s["n"] < 3:
            continue
        rate, base_rate = s["first"] / s["n"], base["first"] / base["n"]
        per = s["cost"] / max(1, s["delivered"])
        base_per = base["cost"] / max(1, base["delivered"])
        if rate - base_rate < 0.25 or (base_per and per > 2 * base_per):
            continue
        label = {"native": "Native", "codex": "Codex", "anton": "Anton"}.get(name, name)
        out.append(Proposal(
            rule="builder_harness", key=f"{ws.id}:{name}",
            title=f"Use {label} as the default builder",
            body=(f"Over the last {WINDOW_DAYS} days {label} passed independent verification on the first attempt in"
                  f" {s['first']} of {s['n']} tasks ({rate:.0%}), against {base['first']} of {base['n']}"
                  f" ({base_rate:.0%}) for the current default, at ${per:.2f} per delivered task versus"
                  f" ${base_per:.2f}. Outcomes can still choose a worker explicitly."),
            action={"type": "settings", "set": {"harness.builder": name}, "label": f"Make {label} the default"},
            evidence=[{"label": n, "detail": f"{v['first']}/{v['n']} first-attempt · {v['delivered']} delivered ·"
                                             f" ${v['cost']:.2f}"} for n, v in sorted(stats.items())],
            basis=f"{s['n']}:{base['n']}"))
    return out


def _merge_feedback(db: Session, ws: Workspace, repos: dict[str, Repository]) -> list[Proposal]:
    """What people said when they rejected a merge is guidance the next builder should have."""
    rows = (db.query(Decision).filter(Decision.workspace_id == ws.id, Decision.kind == "approve_merge",
                                      Decision.status == "resolved", Decision.resolution == "reject",
                                      Decision.resolved_at >= _since()).order_by(Decision.resolved_at.desc())
            .limit(100).all())
    per_repo: dict[str, list[tuple[Decision, Task]]] = defaultdict(list)
    for d in rows:
        task = db.get(Task, d.task_id) if d.task_id else None
        if task and task.repo_id in repos and (d.comment or "").strip():
            per_repo[task.repo_id].append((d, task))
    out = []
    for repo_id, items in per_repo.items():
        if len(items) < 2:
            continue
        repo = repos[repo_id]
        names = _names(db, {d.resolved_by for d, _ in items if d.resolved_by})
        note = "Reviewers asked for these when rejecting earlier changes:\n" + "\n".join(
            f"- {' '.join(d.comment.split())[:280]}" for d, _ in items[:6])
        out.append(Proposal(
            rule="merge_feedback", key=repo_id, repo_id=repo_id,
            title=f"Keep reviewers' merge feedback for {repo.full_name}",
            body=(f"{len(items)} merges were rejected with comments by {', '.join(names) or 'your team'}. Saving that"
                  " feedback as team notes means builders see it before they start, not after a rejection."),
            action={"type": "team_note", "repo_id": repo_id, "note": note, "label": "Add to team notes"},
            evidence=[{"label": f"{_label(db, task)} · PR #{task.pr_number}" if task.pr_number else _label(db, task),
                       "detail": d.comment[:200], "outcome_id": task.outcome_id, "task_id": task.id, "url": task.pr_url}
                      for d, task in items[:6]],
            basis=str(len(items) // 2)))
    return out


RULES = (_merge_streaks, _plan_streak, _review_themes, _check_failures, _qa_boot, _budget_hits, _harness_results,
         _merge_feedback)


def proposals(db: Session, workspace_id: str) -> list[Proposal]:
    ws = db.get(Workspace, workspace_id)
    if ws is None:
        return []
    repos = {r.id: r for r in db.query(Repository).filter(Repository.workspace_id == workspace_id).all()}
    out: list[Proposal] = []
    for rule in RULES:
        try:
            out.extend(rule(db, ws) if rule in (_plan_streak, _budget_hits, _harness_results) else rule(db, ws, repos))
        except Exception:  # noqa: BLE001 - one broken rule must not hide the others
            log.exception("improvement rule %s failed", rule.__name__)
    return out


def analyze(db: Session, workspace_id: str) -> dict[str, int]:
    """Refresh improvement proposals for a workspace. Returns counts of new/updated/retired proposals."""
    found = {p.fingerprint: p for p in proposals(db, workspace_id)}
    existing = {s.fingerprint: s for s in db.query(Signal).filter(Signal.workspace_id == workspace_id,
                                                                  Signal.source == "improvement").all()}
    created = updated = retired = 0
    for fp, p in found.items():
        data = {"rule": p.rule, "basis": p.basis, "action": p.action, "evidence": p.evidence}
        row = existing.get(fp)
        if row is None:
            db.add(Signal(workspace_id=workspace_id, repo_id=p.repo_id, source="improvement", external_id=p.rule,
                          fingerprint=fp, title=p.title[:300], body=p.body[:8000], data=data))
            created += 1
            continue
        changed = (row.data or {}).get("basis") != p.basis
        if row.status in ("new",) or (changed and row.status in ("ignored", "resolved", "accepted")):
            # Re-open an ignored or applied proposal only when the evidence materially changed.
            if row.status != "new":
                row.status = "new"
                row.count += 1
            row.title, row.body, row.data = p.title[:300], p.body[:8000], data
            row.last_seen_at = utcnow()
            updated += 1
    for fp, row in existing.items():
        if fp not in found and row.status == "new":
            row.status = "resolved"  # the evidence no longer supports it
            retired += 1
    if created or updated or retired:
        db.flush()
        notify(workspace_id, "signal", "")
    return {"created": created, "updated": updated, "retired": retired}


# --- applying a proposal ----------------------------------------------------------------------
def apply(db: Session, sig: Signal, user: User, *, note: str | None = None) -> str:
    """Apply an improvement's action as ``user``. Returns a one-line description of what changed."""
    action = (sig.data or {}).get("action") or {}
    kind = action.get("type")
    if kind not in ACTION_TYPES:
        raise ValueError("This proposal has nothing to apply")
    ws = db.get(Workspace, sig.workspace_id)
    summary = ""
    if kind == "settings":
        settings = dict(ws.settings or {})
        for path, value in (action.get("set") or {}).items():
            section, _, leaf = path.partition(".")
            settings[section] = {**(settings.get(section) or {}), leaf: value} if leaf else value
        ws.settings = settings
        summary = ", ".join(f"{k} → {v}" for k, v in (action.get("set") or {}).items())
    elif kind in ("repo_config", "team_note", "review"):
        repo = db.get(Repository, action.get("repo_id"))
        if repo is None or repo.workspace_id != sig.workspace_id:
            raise ValueError("The repository for this proposal is no longer connected")
        cfg = dict(repo.config or {})
        if kind == "repo_config":
            for key, value in (action.get("set") or {}).items():
                cfg[key] = {**(cfg.get(key) or {}), **value} if isinstance(value, dict) else value
            summary = f"{repo.full_name}: " + ", ".join(f"{k} → {v}" for k, v in (action.get("set") or {}).items())
        elif kind == "team_note":
            text = (note if note is not None else action.get("note") or "").strip()
            if not text:
                raise ValueError("The team note is empty")
            context = (cfg.get("context") or "").rstrip()
            cfg["context"] = ((context + "\n\n") if context else "") + text
            cfg["context"] = cfg["context"][-8000:]
            summary = f"{repo.full_name}: added a team note"
        else:
            summary = f"{repo.full_name}: marked for review"
        repo.config = cfg
    sig.status = "accepted"
    sig.last_seen_at = utcnow()
    record(sig.workspace_id, "improvement_applied", f"Applied “{sig.title}” ({summary})", actor=user.id,
           data={"signal_id": sig.id, "rule": (sig.data or {}).get("rule"), "action": action}, session=db)
    return summary
