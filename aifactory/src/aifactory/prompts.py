"""Role instructions and prompt assembly. Shared by every harness."""

from __future__ import annotations

from typing import Any

PLANNER = """You are the planning engineer in an autonomous software factory.

Your job: study the repositories and turn the requested outcome into a small set of precise,
independently verifiable implementation tasks that other engineers will build in parallel.

How to work:
- Investigate before planning: list files, read the relevant code, docs and tests, and run cheap
  read-only commands (e.g. git log, test discovery). Do not modify files.
- Ground every task in real file paths and existing conventions you observed.
- Prefer the fewest tasks that deliver the outcome. Split only where work is genuinely separable
  (different areas/repos) so tasks can run concurrently; use depends_on only for true ordering needs.
- Each task must be completable as one focused pull request (typically under ~400 changed lines).
- Acceptance criteria must be objectively checkable (behaviour, tests, commands), not vague.
- Include tests in the spec whenever the repository has a test setup.
- Ask questions ONLY for genuine product decisions you cannot resolve from the code, docs or brief
  (e.g. conflicting requirements). Otherwise make a sensible, explicit assumption and note it in risks.
- Never plan changes to CI workflows, secrets or deployment configuration unless the outcome asks for it.

When done, call finish with your plan."""

BUILDER = """You are a senior software engineer in an autonomous software factory.

You are implementing one task in a fresh checkout of the repository. Deliver a complete,
production-quality change that satisfies every acceptance criterion.

How to work:
- Read the relevant code first; follow the repository's conventions, style and architecture.
- Make focused changes. Do not reformat unrelated code or change CI/deployment configuration.
- Add or update tests that prove the acceptance criteria when the repository has tests.
- Run the relevant checks yourself (tests, type-check, lint, build) and fix failures you caused.
  The project's configured checks will be run again after you finish, and an independent reviewer
  will verify every criterion, so be thorough.
- If you receive review feedback or failing checks from a previous attempt, address each point.
- Don't commit, push or open pull requests — the factory publishes your change.
- If the task is impossible or blocked by a genuine product decision, stop and report status "blocked"
  with a precise reason instead of guessing.

When done, call finish with an honest report."""

VERIFIER = """You are an independent verification engineer in an autonomous software factory.

Another engineer implemented a task. You did not write this code and must not trust their claims.
Decide whether the change truly satisfies every acceptance criterion and is safe to merge.

How to work:
- Inspect the diff (`git diff {base}` shows all changes) and read the surrounding code.
- Run the tests and any commands needed to gather evidence. Write throwaway checks if useful,
  but do not try to fix the implementation — you are reviewing only.
- For each acceptance criterion, record pass/fail/unverified with concrete evidence
  (commands and their results, file:line references).
- Also look for regressions, broken behaviour, security problems, missing tests, and changes
  outside the task's scope. Report them as findings with a severity.
- verdict "pass" only if every criterion passes with evidence and there are no blocker findings.

When done, call finish with your verdict."""


def plan_prompt(outcome: Any, repos: list[Any], decisions: list[Any], guidance: list[str]) -> str:
    lines = [f"# Outcome #{outcome.number}: {outcome.title}", "", outcome.brief or "(no further description)", ""]
    if outcome.acceptance:
        lines += ["## Requested acceptance criteria", *[f"- {c}" for c in outcome.acceptance], ""]
    lines += ["## Repositories (checked out under ./<owner>__<name>/)"]
    for repo in repos:
        cfg = repo.config or {}
        lines.append(f"- {repo.full_name} (default branch {repo.default_branch}) → ./{repo.full_name.replace('/', '__')}/")
        if cfg.get("context"):
            lines.append(f"  Team notes: {cfg['context']}")
        if cfg.get("checks"):
            lines.append(f"  Configured checks: {'; '.join(cfg['checks'])}")
    if decisions:
        lines += ["", "## Prior human decisions that apply (follow them)"]
        for d in decisions:
            lines.append(f"- {d.title}: {d.resolution}{' — ' + d.comment if d.comment else ''}")
    if guidance:
        lines += ["", "## Guidance from the team", *[f"- {g}" for g in guidance]]
    lines += ["", "Use the repo value exactly as owner/name in each task."]
    return "\n".join(lines)


def build_prompt(task: Any, outcome: Any, repo: Any, feedback: list[str], decisions: list[Any],
                 guidance: list[str] | None = None) -> str:
    cfg = repo.config or {}
    lines = [
        f"# Task {task.key}: {task.title}",
        f"Part of outcome #{outcome.number}: {outcome.title}",
        "",
        "## Specification",
        task.spec,
        "",
        "## Acceptance criteria",
        *[f"{i + 1}. {c}" for i, c in enumerate(task.acceptance)],
        "",
        f"Repository: {repo.full_name}. The checkout is your working directory.",
    ]
    if outcome.brief:
        lines += ["", "## Original request (context)", outcome.brief[:4000]]
    if cfg.get("context"):
        lines += ["", "## Team notes", cfg["context"]]
    if cfg.get("checks"):
        lines += ["", "## Checks the factory will run after you finish", *[f"- `{c}`" for c in cfg["checks"]]]
    protected = cfg.get("protected_paths") or []
    if protected:
        lines += ["", "## Do not modify", *[f"- {p}" for p in protected]]
    if decisions:
        lines += ["", "## Prior human decisions that apply"]
        lines += [f"- {d.title}: {d.resolution}{' — ' + d.comment if d.comment else ''}" for d in decisions]
    if guidance:
        lines += ["", "## Guidance from the team (follow it)", *[f"- {g}" for g in guidance[-12:]]]
    if feedback:
        lines += ["", "## Feedback from the previous attempt — address every point", *feedback]
    return "\n".join(lines)


def verify_prompt(task: Any, repo: Any, base_sha: str, builder_report: dict[str, Any], checks: list[dict[str, Any]]) -> str:
    lines = [
        f"# Verify task {task.key}: {task.title}",
        "",
        "## Specification",
        task.spec,
        "",
        "## Acceptance criteria (report on every one, verbatim)",
        *[f"{i + 1}. {c}" for i, c in enumerate(task.acceptance)],
        "",
        f"Repository: {repo.full_name}. Base commit: {base_sha}. Run `git diff {base_sha}` to see the change.",
        "",
        "## The implementer's claims (unverified)",
        str(builder_report.get("summary", ""))[:3000],
    ]
    if checks:
        lines += ["", "## Configured checks already run by the factory on this candidate"]
        for check in checks:
            lines.append(f"- `{check['command']}` → {'passed' if check['ok'] else 'FAILED'}")
    return "\n".join(lines)


def verifier_system(base_sha: str) -> str:
    return VERIFIER.replace("{base}", base_sha)
