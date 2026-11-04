"""Measure delivered work, including unsuccessful attempts in its cost."""

from __future__ import annotations

import json
import math
from pathlib import Path

from .run_journal import RunJournal


def delivery_runs(store: Path):
    for path in sorted((store / "delivery").glob("*/manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("purpose") != "delivery":
            continue
        journal = RunJournal(store, path.parent.name)
        yield manifest, journal.summary(), journal.events()


def outcome_metrics(store: Path) -> dict:
    runs = list(delivery_runs(store))
    completed = [summary for _, summary, _ in runs if summary["status"] == "complete"]
    durations = sorted(s["duration_seconds"] for s in completed if s["duration_seconds"] is not None)
    known = round(sum(summary["known_inference_cost_usd"] for _, summary, _ in runs), 8)
    unknown = sum(not summary["cost_complete"] for _, summary, _ in runs)
    return {
        "work_items": len(runs), "accepted_work_items": len(completed),
        "blocked_work_items": sum(summary["status"] == "blocked" for _, summary, _ in runs),
        "known_inference_cost_usd": known, "unpriced_work_items": unknown,
        "inference_cost_per_accepted_item_usd": round(known / len(completed), 8) if completed and not unknown else None,
        "cost_definition": "All delivery attempts, including failed work; excludes replay, improvement runs and execution costs",
        "completion_latency_samples": len(durations),
        "completion_latency_p50_seconds": durations[math.ceil(len(durations) * 0.5) - 1] if durations else None,
        "completion_latency_p90_seconds": durations[math.ceil(len(durations) * 0.9) - 1] if durations else None,
        "recorded_human_interventions": sum(e["kind"] == "human_intervention" for _, _, events in runs for e in events),
        "recorded_corrective_events": sum(e["kind"] in {"revert", "corrective_work"} for _, _, events in runs for e in events),
        "observation_scope": "Only recorded factory events; external supervision, repairs and invoices are not inferred",
    }
