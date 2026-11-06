"""Bounded native planning session; callers supply a read-only job command boundary."""

import json
import time

from .product_agent import AgentSessionError, ProductAgent, RoleSnapshotError, TOOLS
from .product_plan import PLAN_SCHEMA, ProductPlan
from .product_roles import select_role


class ProductPlanner(ProductAgent):
    report_schema = PLAN_SCHEMA
    report_event = "planner_report"
    tools = [TOOLS[0], {**TOOLS[1], "parameters": PLAN_SCHEMA,
                      "description": "Finish with bounded planning guidance; unresolved questions block implementation."}]

    def __init__(self, connector, journal, *, task, base_sha, profile, deadline):
        settings = profile.document
        usage = journal.summary()
        if not usage["usage_complete"] or usage["total_tokens"] >= settings["max_total_tokens"]:
            raise AgentSessionError("Planning cannot start with unknown or exhausted usage")
        if time.monotonic() >= deadline:
            raise AgentSessionError("Run time budget exhausted before planning")
        if connector.config.model != settings["builder_model"] or getattr(connector.config, "fallback_model", None):
            raise AgentSessionError("Planning requires the pinned builder model without fallback")
        super().__init__(connector, journal, max_turns=min(8, settings["max_agent_turns"]),
            max_total_tokens=min(settings["max_total_tokens"], usage["total_tokens"] + 100000),
            deadline=min(deadline, time.monotonic() + 120), pricing=settings["pricing"])
        self.task, self.base_sha, self.profile = task, base_sha, profile

    def _instructions(self, role):
        if role != "planner":
            raise AgentSessionError("Planning session cannot execute another role")
        try:
            manifest = json.loads((self.journal.root / "manifest.json").read_text())
            retained = manifest["planner_role_snapshot"]
            if retained != select_role("planner", "2"):
                raise ValueError("untrusted planner")
        except (OSError, ValueError, KeyError, TypeError):
            raise RoleSnapshotError("Missing or untrusted executable planner snapshot") from None
        return retained["instructions"]

    def _accept_report(self, arguments, context, ran_command):
        if not ran_command:
            raise AgentSessionError("Planner must investigate before reporting")
        ProductPlan.create(arguments, task=self.task, base_sha=self.base_sha,
                           profile_sha=self.profile.digest, planner_role=select_role("planner", "2"))
