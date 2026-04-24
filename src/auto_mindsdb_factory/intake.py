from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import yaml

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, FactoryController, WorkItem
from .policy import PolicyEngine


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


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def slugify(text: str) -> str:
    collapsed = normalize_whitespace(text).lower()
    slug = re.sub(r"[^a-z0-9]+", "-", collapsed).strip("-")
    return re.sub(r"-{2,}", "-", slug)


def build_identifier(prefix: str, seed: str, max_length: int = 48) -> str:
    slug = slugify(seed)
    available = max(8, max_length - len(prefix) - 1)
    if not slug:
        slug = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:available]
    trimmed = slug[:available].strip("-")
    if not trimmed:
        trimmed = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:available]
    return f"{prefix}-{trimmed}"


def summarize_title(text: str, max_length: int = 96) -> str:
    normalized = normalize_whitespace(text).rstrip(".")
    if len(normalized) <= max_length:
        return normalized
    clipped = normalized[: max_length - 3].rsplit(" ", 1)[0].rstrip(",;:")
    return f"{clipped}..."


def extract_date_label(text: str) -> str | None:
    match = re.search(r"[A-Z][a-z]+ \d{1,2}, \d{4}", text)
    if match is None:
        return None
    return match.group(0)


def date_label_to_iso(text: str) -> str:
    return datetime.strptime(text, "%B %d, %Y").date().isoformat()


def keyword_matches(text: str, keyword: str) -> bool:
    pattern = rf"\b{re.escape(keyword.lower())}\b"
    return re.search(pattern, text) is not None


class IntakeError(RuntimeError):
    """Base class for Stage 1 intake failures."""


class UpstreamFetchError(IntakeError):
    """Raised when the scout cannot fetch the upstream provider page."""


class UpstreamShapeError(IntakeError):
    """Raised when the upstream page no longer matches the expected structure."""


class ArtifactValidationError(IntakeError):
    """Raised when a Stage 1 artifact fails schema validation."""


@dataclass(slots=True)
class ReleaseNoteItem:
    provider: str
    kind: str
    external_id: str
    title: str
    url: str
    detected_at: str
    published_at: str
    body: str
    date_label: str
    anchor: str | None = None

    def to_document(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "kind": self.kind,
            "external_id": self.external_id,
            "title": self.title,
            "url": self.url,
            "detected_at": self.detected_at,
            "published_at": self.published_at,
            "body": self.body,
            "date_label": self.date_label,
            "anchor": self.anchor,
        }


class ReleaseNotesOverviewParser(HTMLParser):
    """Extract date sections and note bodies from Anthropic's release-note overview."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.sections: list[dict[str, Any]] = []
        self._in_article = False
        self._in_heading = False
        self._current_heading_parts: list[str] = []
        self._current_anchor: str | None = None
        self._current_block: str | None = None
        self._current_block_parts: list[str] = []
        self._current_section: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "article":
            self._in_article = True
            return
        if not self._in_article:
            return

        if tag == "h3":
            self._flush_current_section()
            self._in_heading = True
            self._current_heading_parts = []
            self._current_anchor = attributes.get("id")
            return

        if self._in_heading and self._current_anchor is None and attributes.get("id"):
            self._current_anchor = attributes["id"]

        if tag in {"li", "p"} and self._current_section is not None:
            self._current_block = tag
            self._current_block_parts = []
            return

        if tag == "br" and self._current_block is not None:
            self._current_block_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "article":
            if self._current_block is not None:
                self._flush_current_block()
            if self._in_heading:
                self._finalize_heading()
            self._flush_current_section()
            self._in_article = False
            return

        if not self._in_article:
            return

        if tag == "h3" and self._in_heading:
            self._finalize_heading()
            self._in_heading = False
            return

        if tag in {"li", "p"} and self._current_block == tag:
            self._flush_current_block()

    def handle_data(self, data: str) -> None:
        if not self._in_article:
            return
        if self._in_heading:
            self._current_heading_parts.append(data)
            return
        if self._current_block is not None:
            self._current_block_parts.append(data)

    def _finalize_heading(self) -> None:
        heading_text = normalize_whitespace("".join(self._current_heading_parts))
        date_label = extract_date_label(heading_text)
        if date_label is not None:
            self._current_section = {
                "date_label": date_label,
                "anchor": self._current_anchor,
                "items": [],
            }
        else:
            self._current_section = None
        self._current_heading_parts = []
        self._current_anchor = None

    def _flush_current_block(self) -> None:
        if self._current_section is None:
            self._current_block = None
            self._current_block_parts = []
            return

        text = normalize_whitespace("".join(self._current_block_parts))
        if text:
            self._current_section["items"].append(text)

        self._current_block = None
        self._current_block_parts = []

    def _flush_current_section(self) -> None:
        if self._current_section and self._current_section["items"]:
            self.sections.append(
                {
                    "date_label": self._current_section["date_label"],
                    "anchor": self._current_section["anchor"],
                    "items": list(self._current_section["items"]),
                }
            )
        self._current_section = None


class AnthropicScout:
    RELEASE_NOTES_URL = "https://platform.claude.com/docs/en/release-notes/overview"

    def __init__(self, source_url: str | None = None, timeout_seconds: int = 20) -> None:
        self.source_url = source_url or self.RELEASE_NOTES_URL
        self.timeout_seconds = timeout_seconds

    def fetch_release_notes_html(self) -> str:
        try:
            request = Request(
                self.source_url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; AutoMindsDBFactory/0.1)"},
            )
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return response.read().decode("utf-8", "replace")
        except (OSError, URLError) as exc:
            raise UpstreamFetchError(
                f"Unable to fetch Anthropic release notes from {self.source_url}: {exc}"
            ) from exc

    def list_items(
        self,
        *,
        html: str | None = None,
        detected_at: str | None = None,
    ) -> list[ReleaseNoteItem]:
        document = html if html is not None else self.fetch_release_notes_html()
        parser = ReleaseNotesOverviewParser()
        parser.feed(document)
        parser.close()

        if not parser.sections:
            raise UpstreamShapeError(
                "Anthropic release-note page shape changed: no dated entries were found in the article content."
            )

        observed_at = detected_at or utc_now()
        items: list[ReleaseNoteItem] = []
        duplicate_counts: dict[str, int] = {}
        for section in parser.sections:
            anchor = section["anchor"] or slugify(section["date_label"])
            published_at = date_label_to_iso(section["date_label"])
            base_url = urljoin(self.source_url, f"#{anchor}") if anchor else self.source_url
            for body in section["items"]:
                title = summarize_title(body)
                digest = hashlib.sha1(body.encode("utf-8")).hexdigest()[:8]
                identity_seed = f"anthropic-{published_at}-{anchor}-{digest}"
                duplicate_counts[identity_seed] = duplicate_counts.get(identity_seed, 0) + 1
                duplicate_suffix = ""
                if duplicate_counts[identity_seed] > 1:
                    duplicate_suffix = f"-dup{duplicate_counts[identity_seed]}"
                external_id = f"{identity_seed}{duplicate_suffix}"
                items.append(
                    ReleaseNoteItem(
                        provider="anthropic",
                        kind="release_note",
                        external_id=external_id,
                        title=title,
                        url=base_url,
                        detected_at=observed_at,
                        published_at=published_at,
                        body=body,
                        date_label=section["date_label"],
                        anchor=anchor,
                    )
                )
        return items


@dataclass(slots=True)
class Clarification:
    decision: str
    confidence: float
    expected_roi: str
    rationale: str
    flags: list[str]
    flag_hits: dict[str, list[str]]
    affected_surfaces: list[str]
    data_classification: str
    blast_radius: str
    rollback_class: str
    risk_score: int
    risk_tier: str
    assumptions: list[str]
    non_goals: list[str]


class Clarifier:
    def __init__(self, root: Path | None = None, policy_engine: PolicyEngine | None = None) -> None:
        self.root = repo_root(root)
        self.policy_engine = policy_engine or PolicyEngine(self.root)
        self.relevance_policy = self._load_yaml(
            self.root / "factory" / "policies" / "relevance.yaml"
        )
        self._weighted_factors: dict[str, int] = self.policy_engine.lane_policy["risk_score"][
            "weighted_factors"
        ]
        self._hard_override_flags = self.relevance_policy.get("hard_override_keywords", {})

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def _keyword_hits(self, text: str, keywords: list[str]) -> list[str]:
        hits: list[str] = []
        for keyword in keywords:
            if keyword_matches(text, keyword):
                hits.append(keyword)
        return hits

    def clarify(self, item: ReleaseNoteItem) -> Clarification:
        text = normalize_whitespace(f"{item.title} {item.body}").lower()
        technical_hits = self._keyword_hits(text, self.relevance_policy["technical_keywords"])
        defer_hits = self._keyword_hits(text, self.relevance_policy["defer_keywords"])
        watchlist_hits = self._keyword_hits(text, self.relevance_policy["watchlist_keywords"])
        ignore_hits = self._keyword_hits(text, self.relevance_policy["ignore_keywords"])
        high_roi_hits = self._keyword_hits(text, self.relevance_policy["high_roi_keywords"])

        if technical_hits:
            if defer_hits:
                decision = "backlog_candidate"
                rationale = (
                    f"Direct integration signal detected ({', '.join(technical_hits[:3])}), "
                    f"but defer signals ({', '.join(defer_hits[:3])}) keep it out of the active build queue."
                )
            else:
                decision = "active_build_candidate"
                rationale = (
                    f"Direct integration signal detected ({', '.join(technical_hits[:3])}), "
                    "so this item is concrete enough for the build queue."
                )
        elif watchlist_hits:
            decision = "watchlist"
            rationale = (
                f"The item points to adjacent surfaces ({', '.join(watchlist_hits[:3])}) "
                "but not to a concrete integration target for the MVP lane."
            )
        elif ignore_hits:
            decision = "ignore"
            rationale = (
                f"The item matches ignore signals ({', '.join(ignore_hits[:3])}) "
                "and does not create an actionable engineering change."
            )
        else:
            decision = "watchlist"
            rationale = (
                "The item is worth recording, but it does not yet expose a concrete enough "
                "implementation surface for autonomous ticketing."
            )

        signal_count = len(set(technical_hits + defer_hits + watchlist_hits + ignore_hits))
        base_confidence = {
            "active_build_candidate": 0.78,
            "backlog_candidate": 0.72,
            "watchlist": 0.64,
            "ignore": 0.7,
        }[decision]
        confidence = round(min(0.95, base_confidence + (0.04 * signal_count)), 2)

        if decision == "active_build_candidate" and (high_roi_hits or len(technical_hits) >= 2):
            expected_roi = "high"
        elif decision in {"active_build_candidate", "backlog_candidate"}:
            expected_roi = "medium"
        else:
            expected_roi = "low"

        affected_surfaces: list[str] = []
        for surface, keywords in self.relevance_policy["affected_surface_keywords"].items():
            if self._keyword_hits(text, keywords):
                affected_surfaces.append(surface)
        if not affected_surfaces:
            affected_surfaces = ["anthropic_integration"]

        flag_hits: dict[str, list[str]] = {}
        for flag, keywords in self.relevance_policy["risk_flag_keywords"].items():
            hits = self._keyword_hits(text, keywords)
            if hits:
                flag_hits[flag] = hits
        for flag, keywords in self._hard_override_flags.items():
            hits = self._keyword_hits(text, keywords)
            if hits:
                flag_hits[flag] = hits

        flags = list(flag_hits)
        risk_score = self.policy_engine.score_flags(flags)
        risk_tier = self.policy_engine.risk_tier(risk_score)

        if "regulated" in text or "hipaa" in text or "pci" in text:
            data_classification = "regulated"
        elif "sensitive_data_access" in flags:
            data_classification = "customer_confidential"
        else:
            data_classification = "internal"

        if decision in {"watchlist", "ignore"}:
            rollback_class = "none"
        elif "irreversible_migration" in flags or "rollback_complexity_high" in flags:
            rollback_class = "manual_recovery_required"
        elif "auth_or_permissions" in flags or "sensitive_data_access" in flags:
            rollback_class = "manual_recovery_required"
        elif (
            "model_behavior_change" in flags
            or "external_api_contract_change" in flags
            or "rollback_complexity_medium" in flags
            or "user_facing_llm_output_change" in flags
        ):
            rollback_class = "reversible_deploy"
        else:
            rollback_class = "immediate_flag_disable"

        if (
            "sensitive_data_access" in flags
            or "auth_or_permissions" in flags
            or "irreversible_migration" in flags
        ):
            blast_radius = "company_wide"
        elif "user_visible" in flags or "user_facing_llm_output_change" in flags:
            blast_radius = "customer_facing"
        elif len(affected_surfaces) >= 3 or {
            "external_api_contract_change",
            "model_behavior_change",
        }.intersection(flags):
            blast_radius = "multi_service"
        else:
            blast_radius = "single_service"

        assumptions = [
            "Stage 1 reasoning is based on the public Anthropic release note plus local factory policy.",
        ]
        if decision in {"active_build_candidate", "backlog_candidate"}:
            assumptions.append(
                "Any resulting implementation should stay behind a feature flag or controlled migration path."
            )
        if technical_hits:
            assumptions.append(f"Primary integration signal: {technical_hits[0]}.")

        if decision == "active_build_candidate":
            non_goals = [
                "Do not expand beyond the directly affected Anthropic integration surfaces in this first pass."
            ]
        elif decision == "backlog_candidate":
            non_goals = ["Do not enqueue implementation until a narrower rollout target is chosen."]
        elif decision == "watchlist":
            non_goals = ["Do not start implementation; keep this item available for later prioritization."]
        else:
            non_goals = ["Do not schedule implementation work from this intake item."]

        return Clarification(
            decision=decision,
            confidence=confidence,
            expected_roi=expected_roi,
            rationale=rationale,
            flags=flags,
            flag_hits=flag_hits,
            affected_surfaces=affected_surfaces,
            data_classification=data_classification,
            blast_radius=blast_radius,
            rollback_class=rollback_class,
            risk_score=risk_score,
            risk_tier=risk_tier,
            assumptions=assumptions,
            non_goals=non_goals,
        )

    def build_spec_packet(
        self,
        item: ReleaseNoteItem,
        *,
        artifact_id: str | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        clarification = self.clarify(item)
        spec_artifact_id = artifact_id or build_identifier("spec", item.external_id, max_length=56)
        created_at = item.detected_at
        updated_at = timestamp or item.detected_at
        base_slug = slugify(item.external_id)[:24] or "item"

        if clarification.decision in {"watchlist", "ignore"}:
            acceptance_criteria = [
                {
                    "id": f"ac-{base_slug}-001",
                    "description": "The release note is documented with a clear rationale and revisit condition.",
                    "verification_method": "manual",
                    "required": True,
                }
            ]
        else:
            acceptance_criteria = [
                {
                    "id": f"ac-{base_slug}-001",
                    "description": (
                        "The impacted surfaces are explicit enough to split into 1-2 day implementation tickets."
                    ),
                    "verification_method": "manual",
                    "required": True,
                },
                {
                    "id": f"ac-{base_slug}-002",
                    "description": (
                        "The rollout and regression expectations are concrete enough to define eval coverage before coding."
                    ),
                    "verification_method": "integration",
                    "required": True,
                },
            ]

        open_questions = [
            {
                "id": f"q-{base_slug}-001",
                "question": self._question_for(item, clarification),
                "blocking": False,
                "resolution_status": "non_blocking",
                "resolution_notes": self._resolution_notes_for(clarification.decision),
            }
        ]

        problem = f"Anthropic release note: {item.body}"
        proposed_capability = self._proposed_capability_for(item, clarification.decision)
        why_now = (
            f"The source item was published on {item.date_label}; Stage 1 captures the engineering impact "
            "before downstream ticketing or migration work starts."
        )

        factors = [
            {
                "name": flag,
                "score": self._weighted_factors.get(flag, 0),
                "notes": self._factor_notes(flag, clarification.flag_hits[flag]),
            }
            for flag in clarification.flags
        ]

        return {
            "artifact": {
                "id": spec_artifact_id,
                "version": 1,
                "source_stage": "spec_drafting",
                "next_stage": "policy_assignment",
                "status": "ready",
                "risk_tier": clarification.risk_tier,
                "owner_agent": "Clarifier",
                "rollback_class": clarification.rollback_class,
                "created_at": created_at,
                "updated_at": updated_at,
            },
            "source": {
                "provider": item.provider,
                "kind": item.kind,
                "external_id": item.external_id,
                "title": item.title,
                "url": item.url,
                "detected_at": item.detected_at,
            },
            "summary": {
                "problem": problem,
                "proposed_capability": proposed_capability,
                "why_now": why_now,
                "affected_surfaces": clarification.affected_surfaces,
                "assumptions": clarification.assumptions,
                "non_goals": clarification.non_goals,
            },
            "acceptance_criteria": acceptance_criteria,
            "open_questions": open_questions,
            "risk_profile": {
                "risk_score": clarification.risk_score,
                "data_classification": clarification.data_classification,
                "blast_radius": clarification.blast_radius,
                "rollback_class": clarification.rollback_class,
                "factors": factors,
            },
            "relevance": {
                "decision": clarification.decision,
                "rationale": clarification.rationale,
                "confidence": clarification.confidence,
                "expected_roi": clarification.expected_roi,
                "enqueue": clarification.decision == "active_build_candidate",
            },
        }

    @staticmethod
    def _proposed_capability_for(item: ReleaseNoteItem, decision: str) -> str:
        if decision == "ignore":
            return "Record the upstream change and keep it out of the active engineering queue."
        if decision == "watchlist":
            return "Track the upstream capability so it can be reconsidered once a clearer internal use case appears."
        if decision == "backlog_candidate":
            return (
                f"Translate '{item.title}' into a narrower future integration plan before ticketing begins."
            )
        return f"Add or update the affected Anthropic integration surface to support '{item.title}'."

    @staticmethod
    def _resolution_notes_for(decision: str) -> str:
        if decision == "active_build_candidate":
            return "Carry this forward into Stage 2 ticket shaping."
        if decision == "backlog_candidate":
            return "Revisit when a narrower rollout target or deadline exists."
        if decision == "watchlist":
            return "Review again if product pull or roadmap pressure appears."
        return "Re-open only if a new dependency makes the change operationally relevant."

    @staticmethod
    def _question_for(item: ReleaseNoteItem, clarification: Clarification) -> str:
        flags = set(clarification.flags)
        if "external_api_contract_change" in flags:
            return "Do any existing Anthropic wrappers need compatibility shims before this change can ship?"
        if "new_tool_permission" in flags:
            return "Do we need stricter tool-schema eval coverage before rollout?"
        if "model_behavior_change" in flags:
            return "Which internal routes depend on the affected model family or response behavior?"
        if clarification.decision == "watchlist":
            return "What concrete product signal should move this item from watchlist to backlog?"
        if clarification.decision == "ignore":
            return "What would have to change for this item to become engineering-relevant?"
        return f"Which internal service should adopt '{item.title}' first?"

    @staticmethod
    def _factor_notes(flag: str, hits: list[str]) -> str:
        joined = ", ".join(hits[:3])
        return f"Matched release-note terms for {flag}: {joined}."


@dataclass(slots=True)
class Stage1IntakeResult:
    source_item: ReleaseNoteItem
    spec_packet: dict[str, Any]
    policy_decision: dict[str, Any]
    work_item: WorkItem

    def to_document(self) -> dict[str, Any]:
        return {
            "source_item": self.source_item.to_document(),
            "spec_packet": self.spec_packet,
            "policy_decision": self.policy_decision,
            "work_item": self.work_item.to_document(),
            "history": [
                {
                    "event": record.event,
                    "from_state": record.from_state,
                    "to_state": record.to_state,
                    "artifact_id": record.artifact_id,
                    "occurred_at": record.occurred_at,
                }
                for record in self.work_item.history
            ],
        }


class Stage1IntakePipeline:
    """Runs Scout -> Clarifier -> PolicyEngine -> FactoryController for one item."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        policy_engine: PolicyEngine | None = None,
        controller: FactoryController | None = None,
        clarifier: Clarifier | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.policy_engine = policy_engine or PolicyEngine(self.root)
        self.controller = controller or FactoryController()
        self.clarifier = clarifier or Clarifier(self.root, self.policy_engine)
        self.validators = load_validators(self.root)

    def process_item(
        self,
        item: ReleaseNoteItem,
        *,
        spec_artifact_id: str | None = None,
        policy_artifact_id: str | None = None,
        work_item_id: str | None = None,
    ) -> Stage1IntakeResult:
        spec_packet = self.clarifier.build_spec_packet(item, artifact_id=spec_artifact_id)
        policy_decision = self.policy_engine.evaluate_change(
            spec_packet_id=spec_packet["artifact"]["id"],
            decision=spec_packet["relevance"]["decision"],
            flags=[factor["name"] for factor in spec_packet["risk_profile"]["factors"]],
            reasoning=self._policy_reasoning(spec_packet),
            artifact_id=policy_artifact_id or build_identifier("policy", item.external_id, 56),
            timestamp=spec_packet["artifact"]["updated_at"],
        )

        work_item = self.controller.create_work_item(
            source_provider=item.provider,
            source_external_id=item.external_id,
            title=item.title,
            work_item_id=work_item_id or build_identifier("wi", item.external_id, 56),
            created_at=item.detected_at,
        )
        self.controller.apply_event(
            work_item,
            event=ControllerEvent.CHANGELOG_ITEM_RECORDED,
            occurred_at=item.detected_at,
        )

        decision = policy_decision["decision"]
        if decision == "watchlist":
            self.controller.apply_event(
                work_item,
                event=ControllerEvent.RELEVANCE_WATCHLIST,
                artifact_id=policy_decision["artifact"]["id"],
                occurred_at=policy_decision["artifact"]["updated_at"],
                risk_score=policy_decision["risk_score"],
                policy_decision_id=policy_decision["artifact"]["id"],
            )
        elif decision == "ignore":
            self.controller.apply_event(
                work_item,
                event=ControllerEvent.RELEVANCE_REJECTED,
                artifact_id=policy_decision["artifact"]["id"],
                occurred_at=policy_decision["artifact"]["updated_at"],
                risk_score=policy_decision["risk_score"],
                policy_decision_id=policy_decision["artifact"]["id"],
            )
        else:
            self.controller.apply_event(
                work_item,
                event=ControllerEvent.RELEVANCE_ACCEPTED,
                occurred_at=spec_packet["artifact"]["created_at"],
                risk_score=policy_decision["risk_score"],
            )
            self.controller.apply_event(
                work_item,
                event=ControllerEvent.SPEC_PACKET_VALID,
                artifact_id=spec_packet["artifact"]["id"],
                occurred_at=spec_packet["artifact"]["updated_at"],
            )
            self.controller.apply_event(
                work_item,
                event=ControllerEvent.POLICY_DECISION_WRITTEN,
                artifact_id=policy_decision["artifact"]["id"],
                occurred_at=policy_decision["artifact"]["updated_at"],
                execution_lane=policy_decision["lane_assignment"]["lane"],
                policy_decision_id=policy_decision["artifact"]["id"],
            )

        self._validate_document("spec-packet", spec_packet)
        self._validate_document("policy-decision", policy_decision)
        self._validate_document("work-item", work_item.to_document())

        return Stage1IntakeResult(
            source_item=item,
            spec_packet=spec_packet,
            policy_decision=policy_decision,
            work_item=work_item,
        )

    @staticmethod
    def _policy_reasoning(spec_packet: dict[str, Any]) -> list[str]:
        surfaces = ", ".join(spec_packet["summary"]["affected_surfaces"])
        question_count = len(spec_packet["open_questions"])
        return [
            spec_packet["relevance"]["rationale"],
            f"Affected surfaces identified in Stage 1: {surfaces}.",
            f"Open questions captured: {question_count}, all currently marked non-blocking.",
        ]

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            joined = "; ".join(errors)
            raise ArtifactValidationError(f"{schema_name} failed validation: {joined}")
