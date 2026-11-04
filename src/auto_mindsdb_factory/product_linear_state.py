"""Durable Linear handoff evidence, separate from product delivery metrics.

Callers must hold lease(issue_id) while claiming, reading or updating an issue.
Pending is a discovery snapshot: acquire the issue lease and recheck before acting.
A claim without an outcome is not permission to start another delivery: inspect
the existing product delivery journal and recover it instead.
Lease renewal errors must be observed by the caller (including on context exit).
The external controller store has trusted host writers; candidate code cannot access it.
"""
from __future__ import annotations

import json
import re
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from .automation import FactoryRunStore
from .product_linear_intake import _issue_id, eligibility, issue_fingerprint, task_from_issue
from .reliability import write_json_atomic
from .product_task import ProductTask
from .run_journal import JournalError, RunJournal


class LinearStateError(JournalError):
    pass


def _uuid(value):
    try:
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value):
            raise ValueError()
        return str(UUID(value))
    except ValueError as exc:
        raise LinearStateError("Invalid issue UUID") from exc


def _result(value):
    if not isinstance(value, dict) or set(value) - {"status", "merge_sha", "pull_request_url", "reason"}:
        raise LinearStateError("Invalid outcome")
    if value.get("status") not in ("complete", "blocked"):
        raise LinearStateError("Invalid outcome status")
    for key in ("merge_sha", "pull_request_url", "reason"):
        if key in value and (not isinstance(value[key], str) or not value[key].strip()):
            raise LinearStateError("Invalid outcome field")
    if "merge_sha" in value and not re.fullmatch(r"[0-9a-fA-F]{40}", value["merge_sha"]):
        raise LinearStateError("Invalid merge SHA")
    if "pull_request_url" in value and not re.fullmatch(
        r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[1-9][0-9]*", value["pull_request_url"]
    ):
        raise LinearStateError("Invalid GitHub pull request URL")
    if "pull_request_url" in value and any(p in {".", ".."} for p in value["pull_request_url"].split("/")[3:5]):
        raise LinearStateError("Invalid GitHub repository URL")
    if value["status"] == "complete" and not {"merge_sha", "pull_request_url"} <= value.keys():
        raise LinearStateError("Complete requires merge SHA and pull request URL")
    return value


class LinearDeliveryState:
    def __init__(self, store: Path, target_root: Path):
        self.store = Path(store).resolve()
        if (self.store.is_relative_to(Path(target_root).resolve())
                or Path(target_root).resolve().is_relative_to(self.store)):
            raise LinearStateError("State store must be outside candidate target")
        self.base = self.store / "linear-intake"
        self.delivery = self.base / "delivery"
        self._safe(self.base, self.delivery)
        self.locks = FactoryRunStore(self.store)

    def _safe(self, *paths):
        for path in paths:
            if path.is_symlink() or not path.resolve().is_relative_to(self.store):
                raise LinearStateError("Unsafe state path")

    def lease(self, issue_id: str, *, ttl_seconds=60, renew_interval_seconds=15):
        """Renewable per-issue source lease; independent of the delivery task lease."""
        key = "linear-source-" + _uuid(issue_id)
        run_dir = self.store / "runs" / key
        self._safe(self.store / "runs", run_dir, run_dir / ".automation.lock",
                   run_dir / ".automation.lock.guard")
        @contextmanager
        def acquire():
            # Context creation and entry may be separated by arbitrary caller work.
            self._safe(self.store / "runs", run_dir, run_dir / ".automation.lock",
                       run_dir / ".automation.lock.guard")
            with self.locks.run_lease(key, ttl_seconds=ttl_seconds,
                                      renew_interval_seconds=renew_interval_seconds) as held:
                yield held
        return acquire()

    def _journal(self, task_id):
        if not isinstance(task_id, str) or not re.fullmatch(r"linear-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", task_id):
            raise LinearStateError("Invalid Linear task identity")
        self._safe(self.base, self.delivery, self.delivery / task_id)
        return RunJournal(self.base, task_id)

    def _read(self, journal):
        if not isinstance(journal, RunJournal) or journal.root.parent != self.delivery:
            raise LinearStateError("Journal is not in this state store")
        self._safe(self.base, self.delivery, journal.root)
        if not re.fullmatch(r"linear-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", journal.root.name):
            raise LinearStateError("Invalid Linear task identity")
        try:
            for path in journal.root.iterdir():
                if path.is_symlink() or (path.name.startswith("event-") and not re.fullmatch(r"event-[0-9]{6}\.json", path.name)):
                    raise LinearStateError("Unsafe or invalid evidence path")
            manifest = json.loads((journal.root / "manifest.json").read_text(encoding="utf-8"))
            if not isinstance(manifest, dict) or set(manifest) != {"version", "created_at", "binding"} or type(manifest["version"]) is not int or manifest["version"] != 1 or not isinstance(manifest["created_at"], str) or not manifest["created_at"].strip():
                raise LinearStateError("Invalid manifest")
            binding = manifest["binding"]
            if not isinstance(binding, dict) or set(binding) != {"issue_id", "identifier", "url", "fingerprint", "task", "task_digest", "route_digest", "profile_digest"}:
                raise LinearStateError("Invalid binding")
            if _uuid(binding["issue_id"]) != binding["issue_id"] or journal.root.name != "linear-" + binding["issue_id"]:
                raise LinearStateError("Mismatched issue identity")
            if any(not isinstance(binding[k], str) or not binding[k].strip() for k in ("identifier", "url")):
                raise LinearStateError("Invalid issue metadata")
            url = urlsplit(binding["url"])
            if (url.scheme not in ("http", "https") or not url.hostname or url.username is not None
                    or url.password is not None or any(c.isspace() for c in binding["url"])
                    or (url.port is not None and not 1 <= url.port <= 65535)):
                raise LinearStateError("Invalid issue URL")
            if any(not isinstance(binding[k], str) or not re.fullmatch(r"[0-9a-f]{64}", binding[k]) for k in ("fingerprint", "task_digest", "route_digest", "profile_digest")):
                raise LinearStateError("Invalid digest")
            task = ProductTask.from_document(binding["task"])
            if task.digest != binding["task_digest"] or task.document["id"] != journal.root.name:
                raise LinearStateError("Invalid task evidence")
            events = journal.events()
            result, synced = None, False
            for index, event in enumerate(events, 1):
                if type(event["sequence"]) is not int or not (journal.root / f"event-{index:06d}.json").is_file() or set(event) != {"sequence", "at", "kind", "data"} or not isinstance(event["at"], str) or not event["at"].strip():
                    raise LinearStateError("Invalid event")
                if event["kind"] == "outcome" and result is None and not synced:
                    result = _result(event["data"])
                elif event["kind"] == "synced" and result is not None and not synced and event["data"] == {}:
                    synced = True
                else:
                    raise LinearStateError("Invalid handoff event sequence")
            terminal = journal.root / "outcome.json"
            checkpoint = _result(json.loads(terminal.read_text())) if terminal.exists() else None
            if checkpoint is not None and result is not None and checkpoint != result:
                raise LinearStateError("Terminal outcome evidence is inconsistent")
            # Either durable witness preserves the same immutable outcome. A missing
            # mirror can be repaired under the source lease without another build.
            return binding, result if result is not None else checkpoint, synced
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise LinearStateError("Corrupt Linear handoff evidence") from exc

    def claim(self, task, snapshot, route, profile):
        issue_id = _issue_id(snapshot)
        if not isinstance(snapshot.identifier, str) or not snapshot.identifier.strip():
            raise LinearStateError("Invalid issue identifier")
        # Workflow state can advance after the original claim; it is not source content.
        source = replace(snapshot, state={"id": route.document["queued_state_id"]})
        expected = task_from_issue(source, route)
        if not isinstance(task, ProductTask) or task != expected:
            raise LinearStateError("Task does not match source issue and route")
        binding = {"issue_id": issue_id, "identifier": snapshot.identifier, "url": snapshot.url,
                   "fingerprint": issue_fingerprint(snapshot), "task": task.document, "task_digest": task.digest,
                   "route_digest": route.digest, "profile_digest": profile.digest}
        task_id = task.document["id"]
        self._safe(self.base, self.delivery, self.delivery / task_id)
        existed = (self.delivery / task_id).exists()
        if not existed and not eligibility(snapshot, route):
            raise LinearStateError("Issue is not eligible for a new claim")
        journal = self._journal(task_id)
        if existed:
            if self._read(journal)[0] != binding:
                raise LinearStateError("Conflicting Linear claim")
        else:
            journal.create({"binding": binding})
        return journal

    def get_binding(self, journal):
        return self._read(journal)[0]

    def outcome(self, journal):
        return self._read(journal)[1]

    def record_outcome(self, journal, outcome):
        _result(outcome)
        _, old, _ = self._read(journal)
        if old is not None and old != outcome:
            raise LinearStateError("Outcome is immutable")
        if not (journal.root / "outcome.json").exists():
            write_json_atomic(journal.root / "outcome.json", outcome)
        if not any(e["kind"] == "outcome" for e in journal.events()):
            journal.append("outcome", **outcome)

    def mark_synced(self, journal):
        _, outcome, synced = self._read(journal)
        if outcome is None:
            raise LinearStateError("Cannot sync without an outcome")
        self.record_outcome(journal, outcome)
        if not synced:
            journal.append("synced")

    def pending(self):
        self._safe(self.base, self.delivery)
        if not self.delivery.exists():
            return []
        result = []
        for path in sorted(self.delivery.iterdir()):
            self._safe(path)
            if not path.is_dir():
                raise LinearStateError("Invalid delivery evidence directory")
            journal = self._journal(path.name)
            if not self._read(journal)[2]:
                result.append(journal)
        return result
