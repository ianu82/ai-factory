from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from auto_mindsdb_factory.automation import FactoryRunStore, RunLeaseBusyError
from auto_mindsdb_factory.build_review import Stage3BuildReviewPipeline
from auto_mindsdb_factory.intake import AnthropicScout, Stage1IntakePipeline, build_manual_intake_item
from auto_mindsdb_factory.linear_trigger import LinearGraphQLClientError, LinearIssueSnapshot
from auto_mindsdb_factory.linear_workflow import (
    LINEAR_FACTORY_STAGES,
    LinearWorkflowConfig,
    LinearWorkflowError,
    LinearWorkflowStore,
    LinearWorkflowSync,
)
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


@pytest.fixture(autouse=True)
def _disable_linear_env(monkeypatch) -> None:
    monkeypatch.setenv("LINEAR_FACTORY_SYNC_DISABLED", "1")
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)
    monkeypatch.delenv("LINEAR_TARGET_TEAM_ID", raising=False)
    monkeypatch.delenv("LINEAR_TARGET_STATE_ID", raising=False)


def _fixture_html(root: Path) -> str:
    fixture_path = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    return fixture_path.read_text(encoding="utf-8")


def _active_github_stage1(root: Path):
    item = build_manual_intake_item(
        provider="github",
        external_id="github-issue-2",
        title="Factory cockpit should surface GitHub check conclusions and eval status",
        url="https://github.com/ianu82/ai-factory/issues/2",
        detected_at="2026-04-24T12:00:00Z",
        published_at="2026-04-24T11:30:00Z",
        body=(
            "The operator cockpit should surface GitHub pull request check conclusions, local eval "
            "status, and a clear health summary for each work item. This is a control-plane API and "
            "JSON schema change for the cockpit command, not a model-runtime change. Operators should "
            "not need to cross-check multiple artifacts to decide whether a run is healthy. Acceptance "
            "criteria: - update the factory cockpit tool output to include the latest GitHub check "
            "conclusions for each run - include the latest local eval status summary from vertical-slice "
            "or automation artifacts - add a single health field that resolves to ready, blocked, or "
            "warning based on PR checks, eval status, and monitoring alerts - cover the new output with "
            "CLI tests and contract-safe validation"
        ),
    )
    return Stage1IntakePipeline(root).process_item(item)


def _watchlist_linear_stage1(root: Path):
    item = build_manual_intake_item(
        provider="linear",
        external_id="linear:issue-123:state-new-feature:2026-04-24T12:00:00Z",
        title="Factory dashboard should highlight watched Linear intake issues",
        url="https://linear.app/example/issue/SOF-123/factory-intake",
        detected_at="2026-04-24T12:00:00Z",
        published_at="2026-04-24T11:30:00Z",
        body=(
            "The operator dashboard should show which Linear issues are waiting in Factory Intake. "
            "Acceptance criteria: - show the count in the dashboard - keep the display concise for operators"
        ),
    )
    return Stage1IntakePipeline(root).process_item(item)


def _active_linear_stage1(root: Path):
    item = build_manual_intake_item(
        provider="linear",
        external_id="linear:issue-123:state-new-feature:2026-04-24T12:00:00Z",
        title="Factory cockpit should surface Linear stage health",
        url="https://linear.app/example/issue/SOF-123/factory-intake",
        detected_at="2026-04-24T12:00:00Z",
        published_at="2026-04-24T11:30:00Z",
        body=(
            "The operator cockpit API should surface Linear stage health for factory runs. "
            "This is a control-plane API and JSON schema change for the cockpit command. "
            "Acceptance criteria: include the current Linear stage, blocked label status, "
            "and gate summary in the cockpit JSON output; cover it with CLI tests."
        ),
    )
    return Stage1IntakePipeline(root).process_item(item)


def _stage3_revision_document(root: Path) -> dict:
    item = AnthropicScout().list_items(
        html=_fixture_html(root),
        detected_at="2026-04-22T12:00:00Z",
    )[0]
    stage1_result = Stage1IntakePipeline(root).process_item(item)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )
    stage3_result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
        blocking_findings=["Parser still rejects valid tool_result payload variants."],
    )
    return stage3_result.to_document()


def _snapshot(issue_id: str, *, state_id: str = "state-new-feature", state_name: str = "New Feature") -> LinearIssueSnapshot:
    return LinearIssueSnapshot(
        id=issue_id,
        identifier="SOF-123",
        title="Factory dashboard should highlight watched Linear intake issues",
        description="Keep the display concise for operators.",
        url="https://linear.app/example/issue/SOF-123/factory-intake",
        team={"id": "team-123", "name": "software-factory"},
        state={"id": state_id, "name": state_name, "type": "backlog"},
        labels=["ai-factory"],
        priority=2,
        project={"id": "project-1", "name": "Factory"},
        creator={"id": "user-1", "name": "Alice"},
        assignee={"id": "user-2", "name": "Bob"},
        created_at="2026-04-24T11:00:00Z",
        updated_at="2026-04-24T11:30:00Z",
        comments=[],
    )


class FakeLinearWorkflowClient:
    def __init__(self) -> None:
        self.team_states = [
            {
                "id": "state-new-feature",
                "name": "New Feature",
                "type": "backlog",
                "position": 100.0,
            }
        ]
        self.issue_snapshots: dict[str, LinearIssueSnapshot] = {
            "issue-123": _snapshot("issue-123"),
        }
        self.existing_factory_issues: list[dict[str, object]] = []
        self.created_states: list[dict[str, object]] = []
        self.created_issues: list[dict[str, object]] = []
        self.updated_issues: list[dict[str, object]] = []
        self.comment_bodies: list[tuple[str, str]] = []
        self.team_labels: list[dict[str, object]] = [
            {"id": "label-blocked", "name": "blocked/stuck"},
        ]
        self.label_updates: list[dict[str, object]] = []

    def fetch_team_states(self, team_id: str) -> list[dict[str, object]]:
        assert team_id == "team-123"
        return [dict(state) for state in self.team_states]

    def create_workflow_state(
        self,
        *,
        team_id: str,
        name: str,
        state_type: str,
        color: str,
        description: str,
        position: float,
    ) -> dict[str, object]:
        created = {
            "id": f"state-{len(self.created_states) + 1}",
            "name": name,
            "type": state_type,
            "position": position,
        }
        self.created_states.append(
            {
                "team_id": team_id,
                "name": name,
                "state_type": state_type,
                "color": color,
                "description": description,
                "position": position,
            }
        )
        self.team_states.append(created)
        return created

    def fetch_issue_snapshot(self, issue_id: str) -> LinearIssueSnapshot:
        return self.issue_snapshots[issue_id]

    def find_factory_issue_by_work_item(
        self,
        *,
        team_id: str,
        work_item_id: str,
    ) -> dict[str, object] | None:
        assert team_id == "team-123"
        marker = f"Work item: `{work_item_id}`"
        matches = [
            issue
            for issue in self.existing_factory_issues
            if marker in str(issue.get("description") or "")
        ]
        if not matches:
            return None
        return dict(matches[0])

    def find_factory_ticket_issue(
        self,
        *,
        team_id: str,
        parent_issue_id: str,
        work_item_id: str,
        ticket_id: str,
    ) -> dict[str, object] | None:
        assert team_id == "team-123"
        work_item_marker = f"Parent work item: `{work_item_id}`"
        ticket_marker = f"Factory ticket: `{ticket_id}`"
        matches = [
            issue
            for issue in self.existing_factory_issues
            if issue.get("parent_id") == parent_issue_id
            and work_item_marker in str(issue.get("description") or "")
            and ticket_marker in str(issue.get("description") or "")
        ]
        if not matches:
            return None
        return dict(matches[0])

    def create_issue(
        self,
        *,
        team_id: str,
        title: str,
        description: str,
        state_id: str | None = None,
        parent_id: str | None = None,
    ) -> dict[str, object]:
        created = {
            "id": f"issue-{len(self.created_issues) + 900}",
            "identifier": f"SOF-{len(self.created_issues) + 900}",
            "title": title,
            "url": f"https://linear.app/example/issue/SOF-{len(self.created_issues) + 900}/factory-run",
            "state": {"id": state_id, "name": "created"},
        }
        self.created_issues.append(
            {
                "id": created["id"],
                "identifier": created["identifier"],
                "url": created["url"],
                "team_id": team_id,
                "title": title,
                "description": description,
                "state_id": state_id,
                "parent_id": parent_id,
            }
        )
        return created

    def update_issue_state(self, issue_id: str, state_id: str) -> dict[str, object]:
        self.updated_issues.append({"issue_id": issue_id, "state_id": state_id})
        return {
            "id": issue_id,
            "identifier": f"SOF-{issue_id.split('-')[-1]}",
            "title": "Updated issue",
            "url": f"https://linear.app/example/issue/{issue_id}",
            "state": {"id": state_id, "name": "updated"},
        }

    def create_comment(self, issue_id: str, body: str) -> str:
        self.comment_bodies.append((issue_id, body))
        return f"comment-{len(self.comment_bodies)}"

    def fetch_team_labels(self, team_id: str) -> list[dict[str, object]]:
        assert team_id == "team-123"
        return [dict(label) for label in self.team_labels]

    def create_issue_label(
        self,
        *,
        team_id: str,
        name: str,
        color: str,
        description: str,
    ) -> dict[str, object]:
        created = {
            "id": f"label-{len(self.team_labels) + 1}",
            "name": name,
            "color": color,
            "description": description,
        }
        self.team_labels.append(created)
        return dict(created)

    def add_issue_label(self, issue_id: str, label_id: str) -> None:
        self.label_updates.append(
            {"action": "add", "issue_id": issue_id, "label_id": label_id}
        )

    def remove_issue_label(self, issue_id: str, label_id: str) -> None:
        self.label_updates.append(
            {"action": "remove", "issue_id": issue_id, "label_id": label_id}
        )


def test_linear_workflow_ensure_stage_states_creates_missing_states(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )

    stage_states = sync.ensure_stage_states()

    assert list(stage_states) == [definition.key for definition in LINEAR_FACTORY_STAGES]
    assert len(fake_client.created_states) == len(LINEAR_FACTORY_STAGES)
    assert stage_states["stage1"]["name"] == "Stage 1 Intake"
    assert stage_states["stage9"]["type"] == "completed"


def test_linear_workflow_sync_stage1_active_non_linear_creates_issue_without_stall_comment(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _active_github_stage1(root)

    result = sync.sync_stage_result("stage1", stage1_result.to_document())

    assert result["status"] == "synced"
    assert result["state_update"] == "created"
    assert result["comment"]["status"] == "skipped"
    assert result["artifact_comment"]["status"] == "posted"
    assert fake_client.created_issues
    assert fake_client.created_issues[0]["title"].startswith("AI Factory:")
    binding_document = json.loads(
        LinearWorkflowStore(tmp_path / "automation-store", repo_root_override=root)
        .binding_path(stage1_result.work_item.work_item_id)
        .read_text(encoding="utf-8")
    )
    assert binding_document["created_by_factory"] is True


def test_linear_workflow_reuses_existing_factory_issue_for_same_work_item(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _active_github_stage1(root)
    fake_client.existing_factory_issues.append(
        {
            "id": "issue-existing",
            "identifier": "SOF-900",
            "title": "AI Factory: Factory cockpit should surface GitHub check conclusions and eval status",
            "description": (
                "This issue is synchronized automatically by the AI Factory.\n\n"
                f"- Work item: `{stage1_result.work_item.work_item_id}`"
            ),
            "url": "https://linear.app/example/issue/SOF-900/factory-run",
            "state": {"id": "state-new-feature", "name": "New Feature"},
        }
    )

    result = sync.sync_stage_result("stage1", stage1_result.to_document())

    assert result["status"] == "synced"
    assert result["issue_id"] == "issue-existing"
    assert result["state_update"] == "moved"
    assert fake_client.created_issues == []
    binding_document = json.loads(
        LinearWorkflowStore(tmp_path / "automation-store", repo_root_override=root)
        .binding_path(stage1_result.work_item.work_item_id)
        .read_text(encoding="utf-8")
    )
    assert binding_document["created_by_factory"] is True
    assert binding_document["issue_id"] == "issue-existing"


def test_linear_workflow_maybe_create_skips_live_linear_during_pytest(
    tmp_path,
    monkeypatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    monkeypatch.delenv("LINEAR_FACTORY_SYNC_DISABLED", raising=False)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_linear_workflow.py::live_guard")
    monkeypatch.setenv("LINEAR_API_KEY", "real-looking-key")
    monkeypatch.setenv("LINEAR_TARGET_TEAM_ID", "team-123")

    sync = LinearWorkflowSync.maybe_create(tmp_path / "automation-store", repo_root_override=root)

    assert sync is None


def test_linear_workflow_sync_stage1_linear_issue_reuses_issue_and_comments_watchlist(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _watchlist_linear_stage1(root)

    result = sync.sync_stage_result("stage1", stage1_result.to_document())

    assert result["issue_id"] == "issue-123"
    assert result["state_update"] == "moved"
    assert result["comment"]["status"] == "posted"
    assert result["artifact_comment"]["status"] == "posted"
    assert result["blocked_label"] == {"status": "applied", "label": "blocked/stuck"}
    assert fake_client.created_issues == []
    assert fake_client.label_updates == [
        {"action": "add", "issue_id": "issue-123", "label_id": "label-blocked"}
    ]
    assert fake_client.comment_bodies
    comment_text = "\n".join(body for _issue_id, body in fake_client.comment_bodies)
    assert "Stage 1 Intake" in comment_text
    assert "watchlist" in comment_text


def test_linear_workflow_sync_removes_blocked_label_when_run_moves_again(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)

    result = sync.sync_stage_result("stage1", stage1_result.to_document())

    assert result["status"] == "synced"
    assert result["comment"]["status"] == "skipped"
    assert result["artifact_comment"]["status"] == "posted"
    assert result["blocked_label"] == {"status": "removed", "label": "blocked/stuck"}
    assert fake_client.label_updates == [
        {"action": "remove", "issue_id": "issue-123", "label_id": "label-blocked"}
    ]


def test_linear_workflow_sync_stage3_revision_posts_blocking_comment(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage3_document = _stage3_revision_document(root)

    result = sync.sync_stage_result("stage3", stage3_document)

    assert result["status"] == "synced"
    assert result["linear_stage"] == "Stage 3 Build"
    assert result["comment"]["status"] == "posted"
    assert result["artifact_comment"]["status"] == "posted"
    assert fake_client.comment_bodies
    comment_text = "\n".join(body for _issue_id, body in fake_client.comment_bodies)
    assert "Stage 3 Build" in comment_text
    assert "Parser still rejects valid tool_result payload variants." in comment_text


def test_linear_workflow_sync_stage2_posts_ticket_artifact_comment(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )

    result = sync.sync_stage_result("stage2", stage2_result.to_document())

    assert result["status"] == "synced"
    assert result["linear_stage"] == "Stage 2 Ticketing"
    assert result["artifact_comment"]["status"] == "posted"
    assert result["comment"]["status"] == "skipped"
    comment_text = "\n".join(body for _issue_id, body in fake_client.comment_bodies)
    assert "AI Factory artifact update: `Stage 2 Ticketing`" in comment_text
    assert "Scoped tickets:" in comment_text
    assert stage2_result.ticket_bundle["tickets"][0]["title"] in comment_text


def test_linear_workflow_stage2_can_materialize_scoped_tickets_as_child_issues(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(
            api_key="test-key",
            team_id="team-123",
            materialize_stage2_tickets=True,
        ),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )

    result = sync.sync_stage_result("stage2", stage2_result.to_document())

    tickets = stage2_result.ticket_bundle["tickets"]
    assert result["ticket_issues"]["status"] == "synced"
    assert len(result["ticket_issues"]["created"]) == len(tickets)
    assert [issue["parent_id"] for issue in fake_client.created_issues] == [
        "issue-123",
    ] * len(tickets)
    child_descriptions = "\n\n".join(
        str(issue["description"]) for issue in fake_client.created_issues
    )
    assert "This child issue is synchronized automatically by the AI Factory." in child_descriptions
    assert f"Parent work item: `{stage1_result.work_item.work_item_id}`" in child_descriptions
    assert f"Factory ticket: `{tickets[0]['id']}`" in child_descriptions
    binding_document = json.loads(
        LinearWorkflowStore(tmp_path / "automation-store", repo_root_override=root)
        .binding_path(stage1_result.work_item.work_item_id)
        .read_text(encoding="utf-8")
    )
    assert sorted(binding_document["ticket_issue_bindings"]) == sorted(
        ticket["id"] for ticket in tickets
    )


def test_linear_workflow_ticket_artifacts_are_attached_to_child_issues(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(
            api_key="test-key",
            team_id="team-123",
            materialize_stage2_tickets=True,
        ),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )
    sync.sync_stage_result("stage2", stage2_result.to_document())
    fake_client.comment_bodies.clear()
    stage3_result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
    )

    result = sync.sync_stage_result("stage3", stage3_result.to_document())

    ticket_count = len(stage2_result.ticket_bundle["tickets"])
    assert result["ticket_issues"]["status"] == "synced"
    assert len(result["ticket_issues"]["posted"]) == ticket_count
    child_comment_bodies = [
        body
        for issue_id, body in fake_client.comment_bodies
        if issue_id.startswith("issue-90")
    ]
    assert len(child_comment_bodies) == ticket_count
    assert all(
        "AI Factory artifact update for scoped ticket" in body
        and "AI Factory artifact update: `Stage 3 Build`" in body
        for body in child_comment_bodies
    )


def test_linear_workflow_moves_child_tickets_before_parent_advances(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(
            api_key="test-key",
            team_id="team-123",
            materialize_stage2_tickets=True,
        ),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )
    sync.sync_stage_result("stage2", stage2_result.to_document())
    fake_client.updated_issues.clear()
    stage3_result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
    )

    result = sync.sync_stage_result("stage3", stage3_result.to_document())

    child_issue_ids = {
        issue["id"]
        for issue in fake_client.created_issues
        if issue["parent_id"] == "issue-123"
    }
    parent_update_index = next(
        index
        for index, update in enumerate(fake_client.updated_issues)
        if update["issue_id"] == "issue-123"
    )
    child_update_indexes = [
        index
        for index, update in enumerate(fake_client.updated_issues)
        if update["issue_id"] in child_issue_ids
    ]
    assert child_update_indexes
    assert max(child_update_indexes) < parent_update_index
    assert len(result["child_state_sync"]["moved"]) == len(stage2_result.ticket_bundle["tickets"])


def test_linear_workflow_does_not_move_parent_when_child_state_sync_fails(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]

    class FailingChildStateClient(FakeLinearWorkflowClient):
        def update_issue_state(self, issue_id: str, state_id: str) -> dict[str, object]:
            if issue_id.startswith("issue-90"):
                raise LinearGraphQLClientError("child issue state update failed")
            return super().update_issue_state(issue_id, state_id)

    fake_client = FailingChildStateClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(
            api_key="test-key",
            team_id="team-123",
            materialize_stage2_tickets=True,
        ),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )
    sync.sync_stage_result("stage2", stage2_result.to_document())
    fake_client.updated_issues.clear()
    stage3_result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
    )

    with pytest.raises(LinearWorkflowError, match="child issue state update failed"):
        sync.sync_stage_result("stage3", stage3_result.to_document())

    assert {"issue_id": "issue-123", "state_id": "state-3"} not in fake_client.updated_issues


def test_linear_workflow_artifact_comments_are_idempotent(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _active_linear_stage1(root)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )

    first = sync.sync_stage_result("stage2", stage2_result.to_document())
    second = sync.sync_stage_result("stage2", stage2_result.to_document())

    assert first["artifact_comment"]["status"] == "posted"
    assert second["artifact_comment"] == {
        "status": "skipped",
        "reason": "duplicate_artifact_comment",
    }
    assert len(fake_client.comment_bodies) == 1


def test_linear_workflow_rejects_existing_stage_with_wrong_type(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    fake_client.team_states.append(
        {
            "id": "state-bad-9",
            "name": "Stage 9 Feedback",
            "type": "started",
            "position": 900.0,
        }
    )
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )

    with pytest.raises(LinearWorkflowError) as exc_info:
        sync.ensure_stage_states()

    assert "Stage 9 Feedback" in str(exc_info.value)
    assert "expected 'completed'" in str(exc_info.value)


def test_linear_workflow_sync_existing_runs_skips_locked_runs(tmp_path, monkeypatch) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_client = FakeLinearWorkflowClient()
    sync = LinearWorkflowSync(
        tmp_path / "automation-store",
        repo_root_override=root,
        config=LinearWorkflowConfig(api_key="test-key", team_id="team-123"),
        linear_client=fake_client,
    )
    stage1_result = _active_github_stage1(root)
    store = FactoryRunStore(tmp_path / "automation-store", repo_root_override=root)
    store.save_stage_result("stage1", stage1_result.to_document())

    @contextmanager
    def _locked_run_lease(self, work_item_id: str, **kwargs):
        raise RunLeaseBusyError(f"Run '{work_item_id}' is currently locked by another automation worker.")
        yield

    monkeypatch.setattr(FactoryRunStore, "run_lease", _locked_run_lease)

    result = sync.sync_existing_runs()

    assert result.synced_runs == []
    assert result.failed_runs == []
    assert result.skipped_runs == [
        {
            "work_item_id": stage1_result.work_item.work_item_id,
            "reason": "run_locked",
        }
    ]
