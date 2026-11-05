"""Opted-in workflow routes cannot write arbitrary or unacknowledged states."""
from unittest.mock import Mock

import pytest

from auto_mindsdb_factory.linear_trigger import LinearGraphQLClientError
from auto_mindsdb_factory.product_linear_client import LinearProductClient
from auto_mindsdb_factory.product_linear_intake import LinearProductRoute, WORKFLOW_STATE_FIELDS


@pytest.fixture
def workflow_route():
    return LinearProductRoute.from_document({
        "version": 3, "team_id": "team", "required_label": "factory",
        **{key: key for key in WORKFLOW_STATE_FIELDS},
        "acceptance_criteria": [{"id": "test", "description": "test", "command": ["true"]}],
    })


def test_complete_distinct_workflow_configuration_required(workflow_route):
    document = workflow_route.document
    assert LinearProductRoute.from_document(dict(reversed(list(document.items())))) == workflow_route
    for key in WORKFLOW_STATE_FIELDS:
        incomplete = {k: v for k, v in document.items() if k != key}
        with pytest.raises(ValueError):
            LinearProductRoute.from_document(incomplete)
    for key in WORKFLOW_STATE_FIELDS[1:]:
        with pytest.raises(ValueError, match="distinct"):
            LinearProductRoute.from_document({**document, key: document["queued_state_id"]})


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_routes_reject_new_stage_fields(workflow_route, version):
    document = {**workflow_route.document, "version": version}
    with pytest.raises(ValueError):
        LinearProductRoute.from_document(document)


def acknowledgement(state="building_state_id", **issue_fields):
    return {"issueUpdate": {"success": True, "issue": {
        "id": "issue", "state": {"id": state}, "team": {"id": "team"}, **issue_fields}}}


def test_workflow_write_is_scoped_and_acknowledged(workflow_route):
    client = LinearProductClient.__new__(LinearProductClient)
    client._execute = Mock(return_value=acknowledgement())
    client.set_workflow_state("issue", "building_state_id", workflow_route)
    query, variables = client._execute.call_args.args
    assert "issueUpdate" in query
    assert variables == {"id": "issue", "state": "building_state_id"}
    for target in ("queued_state_id", "unknown"):
        client._execute.reset_mock()
        with pytest.raises(LinearGraphQLClientError, match="not authorized"):
            client.set_workflow_state("issue", target, workflow_route)
        client._execute.assert_not_called()


@pytest.mark.parametrize("response", [
    None, {}, {"issueUpdate": {"success": False}},
    acknowledgement(id="wrong"), acknowledgement(state="wrong"),
    acknowledgement(team={"id": "other"}),
])
def test_wrong_or_unknown_write_result_is_not_success(workflow_route, response):
    client = LinearProductClient.__new__(LinearProductClient)
    client._execute = Mock(return_value=response)
    with pytest.raises(LinearGraphQLClientError, match="acknowledgement"):
        client.set_workflow_state("issue", "building_state_id", workflow_route)


def test_all_output_states_are_validated_against_team(workflow_route):
    client = LinearProductClient.__new__(LinearProductClient)
    states = [{"id": key, "type": "unstarted" if key == "queued_state_id" else (
        "completed" if key == "completed_state_id" else "started")} for key in WORKFLOW_STATE_FIELDS]
    client.fetch_team_states = Mock(return_value=states)
    client.validate_route(workflow_route)
    for key in ("verifying_state_id", "merging_state_id"):
        client.fetch_team_states.return_value = [s for s in states if s["id"] != key]
        with pytest.raises(LinearGraphQLClientError):
            client.validate_route(workflow_route)
        client.fetch_team_states.return_value = [
            {**s, "type": "completed"} if s["id"] == key else s for s in states]
        with pytest.raises(LinearGraphQLClientError):
            client.validate_route(workflow_route)


def test_legacy_route_cannot_authorize_workflow_writes(workflow_route):
    document = workflow_route.document
    document = {k: v for k, v in document.items() if k not in WORKFLOW_STATE_FIELDS[1:]}
    route = LinearProductRoute.from_document({**document, "version": 2})
    client = LinearProductClient.__new__(LinearProductClient)
    client._execute = Mock()
    with pytest.raises(LinearGraphQLClientError, match="not authorized"):
        client.set_workflow_state("issue", "building_state_id", route)
    client._execute.assert_not_called()
