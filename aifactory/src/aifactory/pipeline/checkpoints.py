"""Private, bounded delivery checkpoints and explicit park/resume hooks.

No provider-stop policy lives here. T4 can ``await park(runner)`` before releasing
its lease; normal delivery calls ``resume`` after the scheduler admits the task.
Only the Git-visible worktree is saved (not ignored caches, processes or .git).
All Git operations on a job's checkout run through the sandbox abstraction.
"""
from __future__ import annotations

import hashlib
import re
from copy import deepcopy

from sqlalchemy import exists

from ..db import session_scope
from ..gitops import Patch, apply_in_sandbox, checkout, extract_patch, protected_violations, DEFAULT_PROTECTED
from ..models import DeliveryCheckpoint, Plan, Task, new_id
from ..sandbox import LocalSandbox
from .lifecycle import transition

MAX_PATCH_BYTES = 5_000_000
MAX_METADATA_BYTES = 1_000_000
STAGES = ("build", "checks", "verify")


class CheckpointError(RuntimeError):
    """Recovery cannot establish exactness; never silently rebuild a checked candidate."""


def _scope(task) -> dict:
    ev = task.evidence or {}
    return {"epoch": int(ev.get("budget_epoch", 0)), "attempt_base": int(ev.get("attempt_base", 0)),
            "fix_round": ev.get("fix_round") or "", "base_sha": task.base_sha, "branch": task.branch,
            "repo_id": task.repo_id, "plan_id": task.plan_id, "outcome_id": task.outcome_id}


def _fence(runner, session, *, checkpoint_id: str | None = None) -> Task:
    from .delivery import Stop

    task = session.get(Task, runner.task_id)
    if (runner.cancelled.is_set() or task is None or task.workspace_id != runner.ws
            or (task.status, task.stage, task.attempt, _scope(task)) !=
               (runner.task.status, runner.task.stage, runner.task.attempt, _scope(runner.task))
            or task.status not in ("running", "queued")
            or (checkpoint_id is not None and (task.evidence or {}).get("delivery_checkpoint") != checkpoint_id)):
        raise Stop()
    # updated_at fences evidence/epoch changes as well as status/stage. Comparing
    # JSON on UPDATE would not work on PostgreSQL's JSON columns.
    if not transition(session, task, owner=runner.owner, expected={"updated_at": task.updated_at,
                      "attempt": task.attempt, "base_sha": task.base_sha},
                      guards=(exists().where(Plan.id == Task.plan_id, Plan.status == "approved"),),
                      updated_at=task.updated_at):
        raise Stop()
    return task


async def park(runner) -> bool:
    """Save live build work or the exact checked candidate, then refund its reservation.

    The task's status/lease are deliberately unchanged: the caller owns the stop
    policy. Returns False outside an unfinished delivery attempt. The reservation
    number is stored separately; prior failures and retry budgets are untouched.
    Refusal raises Stop; validation, storage and extraction errors propagate and
    must not be treated as a successful park by the caller.
    """
    if runner.task.stage not in STAGES or runner.builder is None:
        return False
    with session_scope() as session:
        task = _fence(runner, session)
        existing = session.get(DeliveryCheckpoint, task.id)
        if (existing and (task.evidence or {}).get("delivery_checkpoint") == existing.id
                and task.attempt == existing.data.get("consumed_before")
                and all(existing.data.get(key) == value for key, value in _scope(task).items())):
            return True  # already parked: never refund twice
        epoch = int((task.evidence or {}).get("budget_epoch", 0))
        if any(a.get("n") == task.attempt and int(a.get("epoch", 0)) == epoch
               and a.get("result") not in ("infra", "checks_preexisting")
               for a in (task.evidence or {}).get("attempts", [])):
            return False  # an attempt that ended is not an interruption
    # Never snapshot the verifier's mutable checkout, nor a builder tree changed
    # by checks. The candidate is exactly what the checks/verification were given.
    stage = runner.task.stage
    patch = runner.patch if stage in ("checks", "verify") else None
    if patch is None:
        patch = await extract_patch(runner.builder, runner.task.base_sha, limit=MAX_PATCH_BYTES)
    if len(patch.diff) > MAX_PATCH_BYTES or not re.fullmatch(r"[0-9a-f]{40,64}", runner.task.base_sha):
        raise CheckpointError("Delivery checkpoint exceeds its bound or has no exact base SHA")
    if protected_violations(patch.files, DEFAULT_PROTECTED + list(runner.cfg.get("protected_paths") or [])):
        raise CheckpointError("Delivery checkpoint changes protected paths")
    reserved = runner.task.attempt
    if reserved < 1:
        raise CheckpointError("Delivery checkpoint has no reserved attempt")
    data = {**_scope(runner.task), "reserved_attempt": reserved, "consumed_before": reserved - 1,
            "stage": stage, "sha256": hashlib.sha256(patch.diff).hexdigest(),
            "files": patch.files, "additions": patch.additions, "deletions": patch.deletions,
            "builder_report": deepcopy(runner.builder_report),
            "checks": deepcopy(runner.checks) if stage == "verify" else None,
            "candidate": deepcopy((runner.task.evidence or {}).get("candidate")),
            "tree": deepcopy(runner._tree)}
    # Check the size before serializing potentially untrusted metadata.
    if _metadata_size(data) > MAX_METADATA_BYTES:
        raise CheckpointError("Delivery checkpoint metadata exceeds its bound")
    checkpoint_id = new_id()
    with session_scope() as session:
        task = _fence(runner, session)
        ev = {**(task.evidence or {}), "delivery_checkpoint": checkpoint_id}
        # The no-op fence above holds the row lock until this atomic refund + save.
        if not transition(session, task, owner=runner.owner, evidence=ev, attempt=reserved - 1):
            from .delivery import Stop
            raise Stop()
        old = session.get(DeliveryCheckpoint, task.id)
        if old:
            session.delete(old)
            session.flush()
        session.add(DeliveryCheckpoint(task_id=task.id, workspace_id=runner.ws, id=checkpoint_id,
                                       patch=patch.diff, data=data))
    runner.task.attempt, runner.task.evidence = reserved - 1, ev
    return True


def _metadata_size(value) -> int:
    """Bound traversal and strings before JSON allocates a second copy."""
    if isinstance(value, str):
        return len(value) * 6 + 2  # worst-case JSON escaping
    if isinstance(value, dict):
        total = 2
        for key, item in value.items():
            total += _metadata_size(str(key)) + _metadata_size(item) + 2
            if total > MAX_METADATA_BYTES:
                return total
        return total
    if isinstance(value, (list, tuple)):
        total = 2
        for item in value:
            total += _metadata_size(item) + 1
            if total > MAX_METADATA_BYTES:
                return total
        return total
    return 32


async def resume(runner, token) -> dict | None:
    """Restore into a fresh credential-free sandbox, then reclaim one reservation.

    Does not invoke a model or publish anything. The returned stage tells delivery
    whether to continue the builder or independently verify the checked candidate.
    The saved row remains until the attempt ends, making repeated interruptions safe.
    """
    with session_scope() as session:
        task = session.get(Task, runner.task_id)
        if task is None or task.workspace_id != runner.ws:
            from .delivery import Stop
            raise Stop()
        checkpoint_id = (task.evidence or {}).get("delivery_checkpoint")
        row = session.get(DeliveryCheckpoint, task.id)
        if not checkpoint_id:
            # A fresh budget invalidated an older checkpoint. Delete private bytes
            # lazily on admission, never attach them to the new epoch.
            if row:
                _fence(runner, session)
                session.delete(row)
            return None
        task = _fence(runner, session)
        if row is None or row.id != checkpoint_id or row.workspace_id != runner.ws:
            raise CheckpointError("Missing delivery checkpoint")
        data, raw = deepcopy(row.data), row.patch
        if any(data.get(key) != value for key, value in _scope(task).items()):
            raise CheckpointError("Stale delivery checkpoint epoch, round or base")
        if (task.attempt not in (data["consumed_before"], data["reserved_attempt"])
                or len(raw) > MAX_PATCH_BYTES or hashlib.sha256(raw).hexdigest() != data.get("sha256")
                or data.get("stage") not in STAGES):
            raise CheckpointError("Invalid delivery checkpoint")
    patch = Patch(raw, data["files"], data["additions"], data["deletions"])
    bad = protected_violations(patch.files, DEFAULT_PROTECTED + list(runner.cfg.get("protected_paths") or []))
    if bad:
        raise CheckpointError("Delivery checkpoint changes protected paths")
    # Checked candidates must pass today's full policy too. Partial builder work
    # may be empty/incomplete, but never gains permission to publish from recovery.
    if data["stage"] != "build":
        problem = await runner._policy_problem(patch)
        if problem:
            raise CheckpointError("Delivery checkpoint rejected by current candidate policy: " + problem)
    local = LocalSandbox(f"task-{runner.task_id[:12]}-resume-{new_id()[:8]}").create()
    try:
        actual = await checkout(local, runner.repo.full_name, token, data["base_sha"], data["branch"])
        if actual != data["base_sha"]:
            raise CheckpointError("Delivery checkpoint base did not match checkout")
        await apply_in_sandbox(local, patch)
        # Recheck after network/VM boundaries, before setup or any further work.
        with session_scope() as session:
            _fence(runner, session, checkpoint_id=checkpoint_id)
        runner.builder = local
        await runner._set_up_builder()
        with session_scope() as session:
            task = _fence(runner, session, checkpoint_id=checkpoint_id)
            if not transition(session, task, owner=runner.owner, attempt=data["reserved_attempt"]):
                from .delivery import Stop
                raise Stop()
        runner.task.attempt = data["reserved_attempt"]
        runner.patch, runner.builder_report = patch, data["builder_report"]
        runner.checks = data.get("checks") or []
        runner._tree = {**data.get("tree", {}), "changes": "restored", "attempt": data["reserved_attempt"]}
        if runner._tree.get("from") == "branch":
            runner._tree["pushed"] = "uncommitted"  # restored patch, never copied commit objects or Git config
        if data["stage"] == "build":
            runner.event("changes_restored", f"Attempt {data['reserved_attempt']}: restored interrupted work "
                         "from the private delivery checkpoint")
        runner.event("candidate_resumed", f"Attempt {data['reserved_attempt']}: resuming the built candidate at "
                     f"{'verification' if data['stage'] == 'verify' else data['stage']} after an interruption")
        return data
    except BaseException:
        await (runner.builder or local).close()
        runner.builder = None
        raise


def discard(session, task_id: str) -> None:
    """Called in the transaction recording a genuine result; private bytes do not linger."""
    session.query(DeliveryCheckpoint).filter(DeliveryCheckpoint.task_id == task_id).delete(synchronize_session=False)
