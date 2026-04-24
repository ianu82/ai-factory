from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml


def repo_root(default: Path | None = None) -> Path:
    if default is not None:
        return default.resolve()
    return Path(__file__).resolve().parents[2]


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


class PolicyEngine:
    """Loads repo policies and evaluates lane/approval decisions."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = repo_root(root)
        self.lane_policy = self._load_yaml(self.root / "factory" / "policies" / "lanes.yaml")
        self.eval_policy = self._load_yaml(
            self.root / "factory" / "policies" / "eval-tiers.yaml"
        )

        self._weighted_factors: dict[str, int] = self.lane_policy["risk_score"]["weighted_factors"]
        self._hard_overrides: dict[str, list[str]] = self.lane_policy["hard_overrides"]
        self._lanes: dict[str, dict[str, Any]] = self.lane_policy["lanes"]
        self._score_bands: dict[str, dict[str, int]] = self.lane_policy["risk_score"]["bands"]
        self._known_flags: set[str] = set(self._weighted_factors)
        for override_values in self._hard_overrides.values():
            self._known_flags.update(override_values)

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    @property
    def known_flags(self) -> set[str]:
        return set(self._known_flags)

    def score_flags(self, flags: Iterable[str]) -> int:
        flag_list = list(flags)
        unknown = sorted(set(flag_list) - self._known_flags)
        if unknown:
            unknown_list = ", ".join(unknown)
            raise ValueError(f"Unknown policy flags: {unknown_list}")

        raw_score = sum(self._weighted_factors.get(flag, 0) for flag in flag_list)
        minimum = self.lane_policy["risk_score"]["minimum"]
        maximum = self.lane_policy["risk_score"]["maximum"]
        return max(minimum, min(maximum, raw_score))

    def determine_lane(self, flags: Iterable[str], risk_score: int) -> tuple[str, str | None]:
        flag_set = set(flags)
        for lane_name, override_flags in self._hard_overrides.items():
            for override_flag in override_flags:
                if override_flag in flag_set:
                    return lane_name, override_flag

        for lane_name, band in self._score_bands.items():
            if band["min"] <= risk_score <= band["max"]:
                return lane_name, None

        raise ValueError(f"Risk score {risk_score} did not map to a lane.")

    @staticmethod
    def risk_tier(risk_score: int) -> str:
        if risk_score < 30:
            return "low"
        if risk_score < 60:
            return "medium"
        if risk_score < 80:
            return "high"
        return "critical"

    def budget_class(self, lane: str) -> str:
        max_budget = self._lanes[lane]["build_limits"]["max_token_budget_usd"]
        if max_budget <= 25:
            return "small"
        if max_budget <= 75:
            return "medium"
        if max_budget <= 150:
            return "large"
        return "custom"

    def build_policy_decision_artifact(
        self,
        *,
        artifact_id: str,
        owner_agent: str,
        status: str,
        next_stage: str,
        risk_tier: str | None = None,
        lane: str | None = None,
        policy_decision_id: str | None = None,
        budget_class: str | None = None,
        rollback_class: str | None = None,
        approval_requirements: list[str] | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        now = timestamp or utc_now()
        artifact: dict[str, Any] = {
            "id": artifact_id,
            "version": 1,
            "source_stage": "policy_engine",
            "next_stage": next_stage,
            "status": status,
            "owner_agent": owner_agent,
            "created_at": now,
            "updated_at": now,
        }
        if risk_tier is not None:
            artifact["risk_tier"] = risk_tier
        if lane is not None:
            artifact["execution_lane"] = lane
        if policy_decision_id is not None:
            artifact["policy_decision_id"] = policy_decision_id
        if budget_class is not None:
            artifact["budget_class"] = budget_class
        if rollback_class is not None:
            artifact["rollback_class"] = rollback_class
        if approval_requirements is not None:
            artifact["approval_requirements"] = approval_requirements
        return artifact

    def evaluate_change(
        self,
        *,
        spec_packet_id: str,
        decision: str,
        flags: Iterable[str],
        reasoning: Iterable[str],
        artifact_id: str,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        risk_score = self.score_flags(flags)
        risk_tier = self.risk_tier(risk_score)
        reasoning_list = list(reasoning)
        if not reasoning_list:
            raise ValueError("reasoning must contain at least one item")

        if decision in {"ignore", "watchlist"}:
            status = "rejected" if decision == "ignore" else "approved"
            artifact = self.build_policy_decision_artifact(
                artifact_id=artifact_id,
                owner_agent="Policy Engine",
                status=status,
                next_stage="completed",
                risk_tier=risk_tier,
                policy_decision_id=artifact_id,
                rollback_class="none",
                approval_requirements=[],
                timestamp=timestamp,
            )
            return {
                "artifact": artifact,
                "spec_packet_id": spec_packet_id,
                "decision": decision,
                "risk_score": risk_score,
                "required_approvals": [],
                "reasoning": reasoning_list,
            }

        lane, override_reason = self.determine_lane(flags, risk_score)
        lane_policy = self._lanes[lane]
        approvals = list(lane_policy["default_required_approvals"])
        artifact = self.build_policy_decision_artifact(
            artifact_id=artifact_id,
            owner_agent="Policy Engine",
            status="ready",
            next_stage="ticketing",
            risk_tier=risk_tier,
            lane=lane,
            policy_decision_id=artifact_id,
            budget_class=self.budget_class(lane),
            rollback_class=lane_policy["rollback_class"],
            approval_requirements=approvals,
            timestamp=timestamp,
        )
        return {
            "artifact": artifact,
            "spec_packet_id": spec_packet_id,
            "decision": decision,
            "risk_score": risk_score,
            "lane_assignment": {
                "lane": lane,
                "locked": override_reason is not None,
                "hard_override_reason": override_reason,
            },
            "required_eval_tiers": list(lane_policy["required_eval_tiers"]),
            "autonomy_policy": dict(lane_policy["autonomy"]),
            "budget_policy": dict(lane_policy["build_limits"]),
            "deployment_policy": dict(lane_policy["release_policy"]),
            "required_approvals": approvals,
            "reasoning": reasoning_list,
        }

