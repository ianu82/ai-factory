from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from pathlib import Path

from auto_mindsdb_factory.automation import AutomationError, FactoryRunStore
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.intake import Stage1IntakePipeline
from auto_mindsdb_factory.linear_trigger import (
    LinearIssueSnapshot,
    LinearTriggerConfig,
    LinearTriggerStore,
    LinearTriggerWorker,
    LinearWebhookEnvelope,
    LinearWebhookReceiver,
)
from auto_mindsdb_factory.linear_workflow import LinearWorkflowError


def _signature(secret: str, payload: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _issue_payload(
    *,
    action: str = "create",
    issue_id: str = "issue-123",
    team_id: str = "team-123",
    state_id: str = "state-factory",
    created_at: str = "2026-04-24T12:00:00Z",
    webhook_timestamp: int = 1_777_033_600_000,
    updated_from: dict[str, str] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "action": action,
        "type": "Issue",
        "createdAt": created_at,
        "webhookTimestamp": webhook_timestamp,
        "webhookId": "webhook-123",
        "organizationId": "org-123",
        "url": "https://linear.app/example/issue/ENG-123/factory-intake",
        "data": {
            "id": issue_id,
            "teamId": team_id,
            "stateId": state_id,
        },
    }
    if updated_from is not None:
        payload["updatedFrom"] = updated_from
    return payload


def _headers(secret: str, raw_body: bytes, *, delivery_id: str = "delivery-123") -> dict[str, str]:
    return {
        "Linear-Delivery": delivery_id,
        "Linear-Event": "Issue",
        "Linear-Signature": _signature(secret, raw_body),
    }


def _snapshot(issue_id: str = "issue-123") -> LinearIssueSnapshot:
    return LinearIssueSnapshot(
        id=issue_id,
        identifier="ENG-123",
        title="Factory API should surface Linear intake status in the cockpit",
        description=(
            "The operator cockpit API should surface Linear-triggered factory runs and their status. "
            "Acceptance criteria:\n"
            "- include the latest Linear-triggered run status in the cockpit JSON output\n"
            "- show whether Stage 1 accepted or rejected the issue in the response format\n"
            "- keep the response schema compatibility-safe for existing callers\n"
            "- cover the output with CLI tests\n"
        ),
        url="https://linear.app/example/issue/ENG-123/factory-intake",
        team={"id": "team-123", "name": "Engineering"},
        state={"id": "state-factory", "name": "Factory Intake", "type": "unstarted"},
        labels=["ai-factory", "control-plane", "api"],
        priority=2,
        project={"id": "project-123", "name": "Factory"},
        creator={"id": "user-1", "name": "Alice"},
        assignee={"id": "user-2", "name": "Bob"},
        created_at="2026-04-24T11:00:00Z",
        updated_at="2026-04-24T11:30:00Z",
        comments=[
            {
                "id": "comment-1",
                "body": "Please get this into the factory intake lane today.",
                "created_at": "2026-04-24T11:15:00Z",
                "author": "Alice",
            }
        ],
    )


def _watchlist_snapshot(issue_id: str = "issue-123") -> LinearIssueSnapshot:
    return LinearIssueSnapshot(
        id=issue_id,
        identifier="ENG-124",
        title="Factory dashboard should highlight watched Linear intake issues",
        description=(
            "The operator dashboard should show which Linear issues are waiting in Factory Intake. "
            "Acceptance criteria:\n"
            "- show the count in the dashboard\n"
            "- keep the display concise for operators\n"
        ),
        url="https://linear.app/example/issue/ENG-124/factory-intake",
        team={"id": "team-123", "name": "Engineering"},
        state={"id": "state-factory", "name": "Factory Intake", "type": "unstarted"},
        labels=["ai-factory", "dashboard"],
        priority=3,
        project={"id": "project-123", "name": "Factory"},
        creator={"id": "user-1", "name": "Alice"},
        assignee={"id": "user-2", "name": "Bob"},
        created_at="2026-04-24T11:00:00Z",
        updated_at="2026-04-24T11:30:00Z",
        comments=[
            {
                "id": "comment-1",
                "body": "This can stay on the watchlist until we have a stronger delivery signal.",
                "created_at": "2026-04-24T11:20:00Z",
                "author": "Alice",
            }
        ],
    )


class FakeLinearClient:
    def __init__(self, snapshot: LinearIssueSnapshot) -> None:
        self.snapshot = snapshot
        self.comment_bodies: list[str] = []

    def fetch_issue_snapshot(self, issue_id: str) -> LinearIssueSnapshot:
        assert issue_id == self.snapshot.id
        return self.snapshot

    def create_comment(self, issue_id: str, body: str) -> str:
        assert issue_id == self.snapshot.id
        self.comment_bodies.append(body)
        return f"comment-for-{issue_id}"


@dataclass
class FakeHandoffResult:
    work_item_id: str

    def to_document(self) -> dict[str, object]:
        return {
            "work_item_id": self.work_item_id,
            "source_stage": "stage1",
            "source_state": "POLICY_ASSIGNED",
            "status": "progressed",
            "final_stage": "stage2",
            "final_state": "TICKETED",
            "stages_completed": ["stage2"],
            "stored_paths": {},
            "reason": None,
        }


class FakeCoordinator:
    def __init__(self, store_dir: Path, root: Path) -> None:
        self.store = FactoryRunStore(store_dir, repo_root_override=root)
        self.stage1_pipeline = Stage1IntakePipeline(root)
        self.handoff_calls: list[str] = []
        self.linear_workflow_sync = None

    def register_bundle(self, stage_name: str, document: dict) -> tuple[Path, object]:
        assert stage_name == "stage1"
        with self.store.state_transaction() as state:
            stored_path = self.store.save_stage_result(stage_name, document)
            self.store.apply_stage_result_to_state(state, stage_name, document)
        return stored_path, state

    def run_immediate_handoff(self, work_item_id: str, **kwargs) -> FakeHandoffResult:
        self.handoff_calls.append(work_item_id)
        return FakeHandoffResult(work_item_id=work_item_id)


class FailingHandoffCoordinator(FakeCoordinator):
    def run_immediate_handoff(self, work_item_id: str, **kwargs) -> FakeHandoffResult:
        raise AutomationError(f"simulated handoff failure for {work_item_id}")


class FailingWorkflowSyncCoordinator(FakeCoordinator):
    def register_bundle(self, stage_name: str, document: dict) -> tuple[Path, object]:
        raise LinearWorkflowError("simulated workflow sync failure")


def test_linear_webhook_receiver_accepts_target_state_create_and_persists_envelope(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    secret = "test-secret"
    payload = _issue_payload()
    raw_body = json.dumps(payload).encode("utf-8")
    store = LinearTriggerStore(tmp_path / "automation-store", repo_root_override=root)
    receiver = LinearWebhookReceiver(
        LinearTriggerConfig(
            webhook_secret=secret,
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        store,
    )

    response = receiver.handle_request(
        path="/hooks/linear",
        headers=_headers(secret, raw_body),
        raw_body=raw_body,
        now_ms=payload["webhookTimestamp"],
    )

    assert response.status_code == 200
    assert response.document["status"] == "accepted"
    stored_path = Path(response.document["stored_path"])
    stored_document = json.loads(stored_path.read_text(encoding="utf-8"))
    assert validation_errors_for(validators["linear-webhook-envelope"], stored_document) == []
    assert stored_document["logical_trigger_key"] == "linear:issue-123:state-factory:2026-04-24T12:00:00Z"


def test_linear_webhook_receiver_rejects_invalid_signature(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    payload = _issue_payload()
    raw_body = json.dumps(payload).encode("utf-8")
    store = LinearTriggerStore(tmp_path / "automation-store", repo_root_override=root)
    receiver = LinearWebhookReceiver(
        LinearTriggerConfig(
            webhook_secret="test-secret",
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        store,
    )

    response = receiver.handle_request(
        path="/hooks/linear",
        headers={
            "Linear-Delivery": "delivery-123",
            "Linear-Event": "Issue",
            "Linear-Signature": "bad-signature",
        },
        raw_body=raw_body,
        now_ms=payload["webhookTimestamp"],
    )

    assert response.status_code == 401
    assert response.document["reason"] == "invalid_signature"


def test_linear_webhook_receiver_rejects_stale_timestamp(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    secret = "test-secret"
    payload = _issue_payload(webhook_timestamp=1_700_000_000_000)
    raw_body = json.dumps(payload).encode("utf-8")
    store = LinearTriggerStore(tmp_path / "automation-store", repo_root_override=root)
    receiver = LinearWebhookReceiver(
        LinearTriggerConfig(
            webhook_secret=secret,
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        store,
    )

    response = receiver.handle_request(
        path="/hooks/linear",
        headers=_headers(secret, raw_body),
        raw_body=raw_body,
        now_ms=payload["webhookTimestamp"] + (120 * 1000),
    )

    assert response.status_code == 401
    assert response.document["reason"] == "stale_webhook_timestamp"


def test_linear_webhook_receiver_ignores_updates_that_stay_in_target_state(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    secret = "test-secret"
    payload = _issue_payload(
        action="update",
        updated_from={"stateId": "state-factory"},
    )
    raw_body = json.dumps(payload).encode("utf-8")
    store = LinearTriggerStore(tmp_path / "automation-store", repo_root_override=root)
    receiver = LinearWebhookReceiver(
        LinearTriggerConfig(
            webhook_secret=secret,
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        store,
    )

    response = receiver.handle_request(
        path="/hooks/linear",
        headers=_headers(secret, raw_body),
        raw_body=raw_body,
        now_ms=payload["webhookTimestamp"],
    )

    assert response.status_code == 200
    assert response.document["status"] == "ignored"
    assert store.iter_envelope_paths() == []


def test_linear_webhook_receiver_accepts_update_into_target_state(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    secret = "test-secret"
    payload = _issue_payload(
        action="update",
        updated_from={"stateId": "state-triage"},
    )
    raw_body = json.dumps(payload).encode("utf-8")
    store = LinearTriggerStore(tmp_path / "automation-store", repo_root_override=root)
    receiver = LinearWebhookReceiver(
        LinearTriggerConfig(
            webhook_secret=secret,
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        store,
    )

    response = receiver.handle_request(
        path="/hooks/linear",
        headers=_headers(secret, raw_body),
        raw_body=raw_body,
        now_ms=payload["webhookTimestamp"],
    )

    assert response.status_code == 200
    assert response.document["status"] == "accepted"
    assert len(store.iter_envelope_paths()) == 1


def test_linear_webhook_receiver_ignores_other_team(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    secret = "test-secret"
    payload = _issue_payload(team_id="team-other")
    raw_body = json.dumps(payload).encode("utf-8")
    store = LinearTriggerStore(tmp_path / "automation-store", repo_root_override=root)
    receiver = LinearWebhookReceiver(
        LinearTriggerConfig(
            webhook_secret=secret,
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        store,
    )

    response = receiver.handle_request(
        path="/hooks/linear",
        headers=_headers(secret, raw_body),
        raw_body=raw_body,
        now_ms=payload["webhookTimestamp"],
    )

    assert response.status_code == 200
    assert response.document["status"] == "ignored"
    assert store.iter_envelope_paths() == []


def test_linear_trigger_worker_processes_envelope_posts_comment_and_hands_off(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    trigger_store = LinearTriggerStore(store_dir, repo_root_override=root)
    payload = _issue_payload()
    envelope = LinearWebhookEnvelope.from_payload(
        delivery_id="delivery-123",
        event_type="Issue",
        received_at="2026-04-24T12:00:01Z",
        payload=payload,
    )
    trigger_store.save_envelope(envelope)
    fake_client = FakeLinearClient(_snapshot())
    fake_coordinator = FakeCoordinator(store_dir, root)
    worker = LinearTriggerWorker(
        store_dir,
        repo_root_override=root,
        config=LinearTriggerConfig(
            api_key="test-api-key",
            target_team_id="team-123",
            target_state_id="state-factory",
            trigger_base_url="https://factory.example.com",
        ),
        linear_client=fake_client,
        coordinator=fake_coordinator,
    )

    result = worker.run_cycle(repository="ianu82/ai-factory")

    assert result.failed_events == []
    assert result.skipped_events == []
    assert len(result.processed_events) == 1
    processed = result.processed_events[0]
    assert processed["decision"] == "active_build_candidate"
    assert processed["comment"]["status"] == "posted"
    assert processed["handoff"]["status"] == "progressed"
    assert processed["handoff"]["final_stage"] == "stage2"
    stage1_document = json.loads(Path(processed["stored_path"]).read_text(encoding="utf-8"))
    assert stage1_document["source_item"]["provider"] == "linear"
    assert stage1_document["spec_packet"]["summary"]["problem"].startswith("Linear issue:")
    assert fake_client.comment_bodies
    assert fake_coordinator.handoff_calls == [processed["work_item_id"]]
    assert "AI Factory intake result" in fake_client.comment_bodies[0]
    assert "https://factory.example.com" in fake_client.comment_bodies[0]
    assert trigger_store.iter_envelope_paths() == []


def test_linear_trigger_worker_comments_on_watchlist_without_handoff(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    trigger_store = LinearTriggerStore(store_dir, repo_root_override=root)
    payload = _issue_payload()
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-123",
            event_type="Issue",
            received_at="2026-04-24T12:00:01Z",
            payload=payload,
        )
    )
    fake_client = FakeLinearClient(_watchlist_snapshot())
    fake_coordinator = FakeCoordinator(store_dir, root)
    worker = LinearTriggerWorker(
        store_dir,
        repo_root_override=root,
        config=LinearTriggerConfig(
            api_key="test-api-key",
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        linear_client=fake_client,
        coordinator=fake_coordinator,
    )

    result = worker.run_cycle(repository="ianu82/ai-factory")

    assert result.failed_events == []
    assert result.skipped_events == []
    assert len(result.processed_events) == 1
    processed = result.processed_events[0]
    assert processed["decision"] == "watchlist"
    assert processed["comment"]["status"] == "posted"
    assert processed["handoff"] is None
    assert fake_coordinator.handoff_calls == []
    assert fake_client.comment_bodies
    assert "watchlist" in fake_client.comment_bodies[0]
    assert "Cockpit: not configured" in fake_client.comment_bodies[0]
    assert "Local run bundle" not in fake_client.comment_bodies[0]


def test_linear_trigger_worker_dedupes_duplicate_logical_trigger_keys(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    trigger_store = LinearTriggerStore(store_dir, repo_root_override=root)
    payload = _issue_payload()
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-1",
            event_type="Issue",
            received_at="2026-04-24T12:00:01Z",
            payload=payload,
        )
    )
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-2",
            event_type="Issue",
            received_at="2026-04-24T12:00:02Z",
            payload=payload,
        )
    )
    fake_client = FakeLinearClient(_snapshot())
    fake_coordinator = FakeCoordinator(store_dir, root)
    worker = LinearTriggerWorker(
        store_dir,
        repo_root_override=root,
        config=LinearTriggerConfig(
            api_key="test-api-key",
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        linear_client=fake_client,
        coordinator=fake_coordinator,
    )

    result = worker.run_cycle(repository="ianu82/ai-factory")

    assert len(result.processed_events) == 1
    assert result.skipped_events == [
        {
            "delivery_id": "delivery-2",
            "reason": "logical_trigger_already_processed",
        }
    ]
    assert fake_coordinator.handoff_calls == [result.processed_events[0]["work_item_id"]]


def test_linear_trigger_worker_creates_new_runs_when_issue_reenters_target_state(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    trigger_store = LinearTriggerStore(store_dir, repo_root_override=root)
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-1",
            event_type="Issue",
            received_at="2026-04-24T12:00:01Z",
            payload=_issue_payload(created_at="2026-04-24T12:00:00Z"),
        )
    )
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-2",
            event_type="Issue",
            received_at="2026-04-25T12:00:01Z",
            payload=_issue_payload(created_at="2026-04-25T12:00:00Z"),
        )
    )
    fake_client = FakeLinearClient(_snapshot())
    fake_coordinator = FakeCoordinator(store_dir, root)
    worker = LinearTriggerWorker(
        store_dir,
        repo_root_override=root,
        config=LinearTriggerConfig(
            api_key="test-api-key",
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        linear_client=fake_client,
        coordinator=fake_coordinator,
    )

    result = worker.run_cycle(repository="ianu82/ai-factory")

    assert len(result.processed_events) == 2
    assert result.skipped_events == []
    assert result.failed_events == []
    assert result.processed_events[0]["work_item_id"] != result.processed_events[1]["work_item_id"]


def test_linear_trigger_worker_records_handoff_failures_without_aborting_cycle(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    trigger_store = LinearTriggerStore(store_dir, repo_root_override=root)
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-1",
            event_type="Issue",
            received_at="2026-04-24T12:00:01Z",
            payload=_issue_payload(issue_id="issue-123", created_at="2026-04-24T12:00:00Z"),
        )
    )
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-2",
            event_type="Issue",
            received_at="2026-04-24T12:05:01Z",
            payload=_issue_payload(issue_id="issue-123", created_at="2026-04-24T12:05:00Z"),
        )
    )
    fake_client = FakeLinearClient(_snapshot(issue_id="issue-123"))
    fake_coordinator = FailingHandoffCoordinator(store_dir, root)
    worker = LinearTriggerWorker(
        store_dir,
        repo_root_override=root,
        config=LinearTriggerConfig(
            api_key="test-api-key",
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        linear_client=fake_client,
        coordinator=fake_coordinator,
    )

    result = worker.run_cycle(repository="ianu82/ai-factory")

    assert result.processed_events == []
    assert len(result.failed_events) == 2
    assert result.failed_events[0]["delivery_id"] == "delivery-1"
    assert "simulated handoff failure" in result.failed_events[0]["reason"]
    assert result.failed_events[1]["delivery_id"] == "delivery-2"


def test_linear_trigger_worker_records_workflow_sync_failures_without_aborting_cycle(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    trigger_store = LinearTriggerStore(store_dir, repo_root_override=root)
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-1",
            event_type="Issue",
            received_at="2026-04-24T12:00:01Z",
            payload=_issue_payload(issue_id="issue-123", created_at="2026-04-24T12:00:00Z"),
        )
    )
    trigger_store.save_envelope(
        LinearWebhookEnvelope.from_payload(
            delivery_id="delivery-2",
            event_type="Issue",
            received_at="2026-04-24T12:05:01Z",
            payload=_issue_payload(issue_id="issue-123", created_at="2026-04-24T12:05:00Z"),
        )
    )
    fake_client = FakeLinearClient(_snapshot(issue_id="issue-123"))
    fake_coordinator = FailingWorkflowSyncCoordinator(store_dir, root)
    worker = LinearTriggerWorker(
        store_dir,
        repo_root_override=root,
        config=LinearTriggerConfig(
            api_key="test-api-key",
            target_team_id="team-123",
            target_state_id="state-factory",
        ),
        linear_client=fake_client,
        coordinator=fake_coordinator,
    )

    result = worker.run_cycle(repository="ianu82/ai-factory")

    assert result.processed_events == []
    assert len(result.failed_events) == 2
    assert result.failed_events[0]["delivery_id"] == "delivery-1"
    assert "simulated workflow sync failure" in result.failed_events[0]["reason"]
    assert result.failed_events[1]["delivery_id"] == "delivery-2"
