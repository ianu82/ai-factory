"""Append-only run evidence. One factory run may contain many failed attempts."""

from __future__ import annotations

import fcntl
import json
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .intake import utc_now
from .reliability import write_json_atomic


class JournalError(RuntimeError):
    pass


class RunJournal:
    def __init__(self, store: Path, run_id: str, *, read_only: bool = False) -> None:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
            raise JournalError("Invalid run id")
        self.root = store.resolve() / "delivery" / run_id
        self._read_only = read_only
        if read_only:
            if not self.root.is_dir():
                raise JournalError("Run does not exist")
            # A manifest is optional (empty run directories are inspectable), but
            # if present it must be readable evidence rather than broken JSON.
            manifest = self.root / "manifest.json"
            try:
                if manifest.is_symlink() and not manifest.exists():
                    raise JournalError("Run manifest could not be read")
                if manifest.exists() and not isinstance(json.loads(manifest.read_text(encoding="utf-8")), dict):
                    raise JournalError("Run manifest is invalid")
            except (OSError, ValueError) as exc:
                raise JournalError("Run manifest could not be read") from exc
        else:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    @contextmanager
    def _lock(self) -> Iterator[None]:
        if self._read_only:
            raise JournalError("Journal is read-only")
        with (self.root / ".journal.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def create(self, manifest: dict[str, Any]) -> None:
        with self._lock():
            path = self.root / "manifest.json"
            if path.exists():
                raise JournalError("Run already exists; inspect its evidence instead of overwriting it")
            write_json_atomic(path, {"version": 1, "created_at": utc_now(), **manifest})

    def append(self, kind: str, **data: Any) -> dict[str, Any]:
        with self._lock():
            events = self.events()
            sequence = len(events) + 1
            event = {"sequence": sequence, "at": utc_now(), "kind": kind, "data": data}
            write_json_atomic(self.root / f"event-{sequence:06d}.json", event)
            return event

    def events(self) -> list[dict[str, Any]]:
        result = []
        if self._read_only and not self.root.is_dir():
            raise JournalError("Run does not exist")
        try:
            for sequence, path in enumerate(sorted(p for p in self.root.iterdir() if p.match("event-*.json")), start=1):
                event = json.loads(path.read_text(encoding="utf-8"))
                if (path.name != f"event-{sequence:06d}.json" or event.get("sequence") != sequence
                        or not isinstance(event.get("data"), dict)):
                    raise JournalError("Run evidence has a missing or invalid event")
                if self._read_only and (type(event["sequence"]) is not int
                        or not isinstance(event.get("kind"), str)
                        or (event["kind"] == "run_finished" and not isinstance(event["data"].get("status"), str))):
                    raise JournalError("Run evidence has a missing or invalid event")
                result.append(event)
        except (OSError, json.JSONDecodeError, AttributeError) as exc:
            raise JournalError("Run evidence could not be read") from exc
        return result

    def summary(self) -> dict[str, Any]:
        events = self.events()
        usage = [event["data"].get("usage") for event in events if event["kind"] == "model_response"]
        total = sum(item.get("total_tokens", 0) for item in usage if isinstance(item, dict))
        terminal = [event for event in events if event["kind"] == "run_finished"]
        responses = [event["data"] for event in events if event["kind"] == "model_response"]
        usage_complete = (sum(event["kind"] == "model_requested" for event in events) <= len(responses)
            and not any(event["kind"] == "model_failed" for event in events)
            and all(isinstance(item, dict) and "total_tokens" in item for item in usage))
        known_cost = round(sum(item.get("cost_estimate_usd") or 0 for item in responses), 8)
        cost_complete = usage_complete and all(item.get("cost_estimate_usd") is not None for item in responses)
        return {
            "status": terminal[-1]["data"]["status"] if terminal else "incomplete",
            "attempts": sum(event["kind"] == "attempt_started" for event in events),
            "total_tokens": total,
            "usage_complete": usage_complete,
            "duration_seconds": terminal[-1]["data"].get("duration_seconds") if terminal else None,
            "cost_usd": known_cost if cost_complete else None,
            "known_inference_cost_usd": known_cost,
            "cost_complete": cost_complete,
            "cost_basis": "inference estimate from recorded usage and explicit rates; excludes execution costs and unobserved requests",
            "events": len(events),
        }
