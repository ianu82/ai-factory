"""Bounded failure investigation proposals; no recursive execution or adoption."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from .automation import FactoryRunStore
from .intake import repo_root
from .product_outcomes import delivery_runs
from .reliability import write_json_atomic


def propose_improvements(store: Path, *, min_failures: int = 3, limit: int = 3,
                         cooldown_hours: float = 24, now: datetime | None = None) -> list[dict]:
    if not 2 <= min_failures <= 100 or not 1 <= limit <= 10 or not math.isfinite(cooldown_hours) or cooldown_hours < 1:
        raise ValueError("Invalid improvement threshold, limit or cooldown")
    groups = {}
    for manifest, _, events in delivery_runs(store):
        for event in events:
            data = event["data"]
            if (event["kind"] == "verification_check" and type(data.get("exit_code")) is int
                    and data["exit_code"] not in {0, 124, 125, 126, 127, 137}):
                failure = {"kind": "verification", "name": data["name"], "command": data["command"]}
            elif event["kind"] == "model_failed":
                failure = {"kind": "model", "role": data["role"], "model": data.get("model")}
            else:
                continue
            key = {"profile_sha": manifest["profile_sha"], "repository": manifest["profile"]["repository"], "failure": failure}
            fingerprint = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()
            group = groups.setdefault(fingerprint, {**key, "source_runs": set()})
            group["source_runs"].add(manifest["task"]["id"])
    directory = store / "improvement-proposals"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    now = now or datetime.now(timezone.utc)
    locks = FactoryRunStore(store, repo_root_override=repo_root())
    proposed = []
    with locks.state_lease(ttl_seconds=60, renew_interval_seconds=15):
        index_path = directory / "index.json"
        index = json.loads(index_path.read_text()) if index_path.exists() else {}
        for fingerprint, group in sorted(groups.items()):
            sources = sorted(group["source_runs"])
            previous = index.get(fingerprint)
            if len(sources) < min_failures or (previous and (
                set(sources) <= set(previous["source_runs"]) or
                now - datetime.fromisoformat(previous["created_at"]) < timedelta(hours=cooldown_hours)
            )):
                continue
            proposal = {"version": 1, "id": "improvement-" + uuid4().hex, "origin": "improvement",
                "status": "proposed", "fingerprint": fingerprint, "created_at": now.isoformat(),
                **group, "source_runs": sources,
                "goal": "Investigate the repeated failure, reproduce it and propose the smallest general fix with a regression check.",
                "promotion_requirements": ["Independent executable acceptance checks", "Repeated replay evidence and a general product justification",
                    "Existing policy, judge criteria, hold-outs and merge authority remain independently controlled"],
            }
            write_json_atomic(directory / (proposal["id"] + ".json"), proposal)
            index[fingerprint] = {"created_at": now.isoformat(), "source_runs": sources, "proposal_id": proposal["id"]}
            proposed.append(proposal)
            if len(proposed) >= limit:
                break
        write_json_atomic(index_path, index)
    return proposed
