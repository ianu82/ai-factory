import copy
import json
import re
from unittest.mock import Mock

import pytest

from auto_mindsdb_factory.linear_trigger import LinearGraphQLClient, LinearGraphQLClientError, LinearTriggerConfig
from auto_mindsdb_factory.product_linear_client import LinearProductClient
from auto_mindsdb_factory.product_linear_intake import LinearProductRoute


@pytest.fixture
def route():
    return LinearProductRoute.from_document({
        "version": 1, "team_id": "team", "queued_state_id": "queue",
        "building_state_id": "building", "blocked_state_id": "blocked",
        "completed_state_id": "done", "required_label": 'factory"label',
        "acceptance_criteria": [{"id": "test", "description": "test", "command": ["true"]}],
    })


@pytest.fixture
def client():
    client = LinearProductClient.__new__(LinearProductClient)
    client._execute = Mock()
    return client


ID = "abcdef01-2345-6789-abcd-ef0123456789"


def connection(nodes=None, has_next=False, cursor=None):
    return {"issues": {"nodes": nodes if nodes is not None else [],
                       "pageInfo": {"hasNextPage": has_next, "endCursor": cursor}}}


def test_scoped_filter_and_single_bounded_page(client, route):
    assert isinstance(client, LinearGraphQLClient)
    client._execute.return_value = connection([{"id": ID.upper(), "identifier": "ABC-1"}], True, "next")
    assert client.queued_issue_ids(route, 100) == [ID]
    query, variables = client._execute.call_args.args
    compact = re.sub(r"\s+", "", query)
    assert "issues(first:$limit,filter:{id:{nin:$excluded},team:{id:{eq:$team}},state:{id:{eq:$state}},labels:{some:{name:{eq:$label}}}})" in compact
    assert variables == {"team": "team", "state": "queue", "label": 'factory"label', "limit": 100, "excluded": []}
    assert 'factory"label' not in query
    client._execute.assert_called_once()
    client._execute.return_value = connection()
    assert client.queued_issue_ids(route) == []
    assert client._execute.call_args.args[1]["limit"] == 10
    assert client.queued_issue_ids(route, 1) == []


@pytest.mark.parametrize("limit", [True, False, 0, -1, 101, 1.0, "10", None])
def test_invalid_limit_never_queries(client, route, limit):
    with pytest.raises(ValueError):
        client.queued_issue_ids(route, limit)
    client._execute.assert_not_called()


@pytest.mark.parametrize("result", [
    None, {}, {"issues": None}, {"issues": {}},
    {"issues": {"nodes": {}, "pageInfo": {"hasNextPage": False, "endCursor": None}}},
    {"issues": {"nodes": [], "pageInfo": {"hasNextPage": 1, "endCursor": None}}},
    {"issues": {"nodes": [], "pageInfo": {"hasNextPage": False}}},
    connection(has_next=True), connection(cursor=123),
    connection([None]), connection([{"id": "bad", "identifier": "ABC-1"}]),
    connection([{"id": ID, "identifier": " "}]),
    connection([{"id": ID, "identifier": "ABC-1"}, {"id": ID.upper(), "identifier": "ABC-2"}]),
])
def test_malformed_results_fail_closed(client, route, result):
    client._execute.return_value = result
    with pytest.raises(LinearGraphQLClientError):
        client.queued_issue_ids(route)


def test_too_many_nodes_fails_closed(client, route):
    client._execute.return_value = connection([{"id": ID, "identifier": "ABC-1"}] * 2)
    with pytest.raises(LinearGraphQLClientError):
        client.queued_issue_ids(route, 1)


def states(queued="unstarted"):
    return [{"id": i, "type": t} for i, t in
            [("queue", queued), ("building", "started"), ("blocked", "started"), ("done", "completed")]]


@pytest.mark.parametrize("queued", ["unstarted", "backlog"])
def test_route_state_semantics(client, route, queued):
    client.fetch_team_states = Mock(return_value=states(queued))
    assert client.validate_route(route) is None
    client.fetch_team_states.assert_called_once_with("team")


@pytest.mark.parametrize("index,wrong", [(0, "started"), (1, "unstarted"), (2, "completed"), (3, "canceled"), (3, None), (1, [])])
def test_wrong_state_types_fail(client, route, index, wrong):
    values = states()
    values[index]["type"] = wrong
    client.fetch_team_states = Mock(return_value=values)
    with pytest.raises(LinearGraphQLClientError):
        client.validate_route(route)


@pytest.mark.parametrize("values", [[], states()[:-1], states() + [copy.copy(states()[0])], [None], None])
def test_missing_or_malformed_team_states_fail(client, route, values):
    client.fetch_team_states = Mock(return_value=values)
    with pytest.raises(LinearGraphQLClientError):
        client.validate_route(route)


@pytest.mark.parametrize("team", [None, {}, {"id": "wrong-team", "states": {"nodes": []}}])
def test_route_rejects_missing_or_mismatched_response_team(client, route, team):
    client._execute.return_value = {"team": team}
    with pytest.raises(LinearGraphQLClientError):
        client.validate_route(route)


def test_route_checks_team_identity_from_transport(client, route):
    client._execute.return_value = {"team": {"id": "team", "states": {
        "nodes": [{**s, "name": s["id"]} for s in states()],
        "pageInfo": {"hasNextPage": False}}}}
    assert client.validate_route(route) is None
    assert client._execute.call_args.args[1] == {"id": "team"}


@pytest.fixture
def route_v2(route):
    config = route.document
    config["version"] = 2
    for key in ("building_state_id", "blocked_state_id", "completed_state_id"):
        del config[key]
    return LinearProductRoute.from_document(config)


def state_response(nodes, page=None):
    return {"team": {"id": "team", "states": {"nodes": nodes,
            "pageInfo": {"hasNextPage": False} if page is None else page}}}


@pytest.mark.parametrize("queued", ["unstarted", "backlog"])
def test_v2_queue_only(client, route_v2, queued):
    client._execute.return_value = state_response([{"id": "queue", "name": "Queue", "type": queued}])
    client.validate_route(route_v2)
    client._execute.return_value = connection([{"id": ID, "identifier": "ABC-1"}])
    assert client.queued_issue_ids(route_v2) == [ID]
    assert client._execute.call_args.args[1]["state"] == "queue"


@pytest.mark.parametrize("route_name", ["route", "route_v2"])
@pytest.mark.parametrize("nodes", [
    [{"id": "queue", "name": "Queue", "type": "unstarted"}, {"id": "queue", "type": "canceled"}],
    [{"id": "queue", "name": "Queue", "type": "unstarted"}, {"id": "queue", "name": "Cancelled", "type": "canceled"}],
    [{"id": "queue", "name": "Queue", "type": "canceled"}],
    [{"id": "queue", "name": "Queue"}], [None], [42],
])
def test_route_transport_rejects_malformed_or_contradictory_states(client, request, route_name, nodes):
    if route_name == "route":
        nodes = nodes + [{**s, "name": s["id"]} for s in states()[1:]]
    client._execute.return_value = state_response(nodes)
    with pytest.raises(LinearGraphQLClientError):
        client.validate_route(request.getfixturevalue(route_name))


@pytest.mark.parametrize("page", [{}, {"hasNextPage": True}, {"hasNextPage": 0}])
def test_route_rejects_incomplete_state_connection(client, route_v2, page):
    client._execute.return_value = state_response([{"id": "queue", "name": "Queue", "type": "unstarted"}], page)
    with pytest.raises(LinearGraphQLClientError):
        client.validate_route(route_v2)


def test_product_comment_uses_transport_without_touching_issue(client):
    client._execute.return_value = {"commentCreate": {"success": True, "comment": {"id": "comment-id"}}}
    assert client.create_comment("issue-id", "progress") == "comment-id"
    query, variables = client._execute.call_args.args
    assert "commentCreate" in query and "issueUpdate" not in query
    assert variables == {"issueId": "issue-id", "body": "progress"}
    client._execute.assert_called_once()


@pytest.mark.parametrize("result", [None, {}, {"commentCreate": None},
    {"commentCreate": {"success": False, "comment": {"id": "id"}}},
    {"commentCreate": {"success": 1, "comment": {"id": "id"}}},
    {"commentCreate": {"success": True}}, {"commentCreate": {"success": True, "comment": {"id": " "}}},
    {"commentCreate": {"success": True, "comment": {"id": 42}}}])
def test_rejected_comment_never_updates_issue(client, result):
    client._execute.return_value = result
    with pytest.raises(LinearGraphQLClientError):
        client.create_comment("issue-id", "progress")
    client._execute.assert_called_once()
    assert "issueUpdate" not in client._execute.call_args.args[0]


@pytest.mark.parametrize("error", ["Entity not found: Issue", "other GraphQL error"])
def test_comment_transport_failure_never_falls_back(client, error):
    client._execute.side_effect = LinearGraphQLClientError(error)
    with pytest.raises(LinearGraphQLClientError, match=error):
        client.create_comment("issue-id", "progress")
    client._execute.assert_called_once()
    assert "issueUpdate" not in client._execute.call_args.args[0]


@pytest.mark.parametrize("wrong", ["started", "completed", "canceled", "", None, []])
def test_v2_rejects_nonqueued_state_types(client, route_v2, wrong):
    client.fetch_team_states = Mock(return_value=[{"id": "queue", "type": wrong}])
    with pytest.raises(LinearGraphQLClientError):
        client.validate_route(route_v2)


def test_v2_does_not_require_other_workflow_states(client, route_v2):
    client._execute.return_value = state_response([
        {"id": "queue", "name": "Queue", "type": "backlog"},
        {"id": "other", "name": "Cancelled", "type": "canceled"},
    ])
    client.validate_route(route_v2)


@pytest.mark.parametrize("response", [
    {"errors": [{"message": "Entity not found: Issue"}]}, [],
] + [{"errors": errors, "data": {"commentCreate": {"success": True, "comment": {"id": "id"}}}}
     for errors in (None, [], {"message": "rejected"})])
def test_comment_uses_authenticated_transport_and_never_falls_back(monkeypatch, response):
    from auto_mindsdb_factory import linear_trigger
    http = Mock()
    http.return_value.__enter__ = Mock(return_value=Mock(read=Mock(return_value=json.dumps(response).encode())))
    http.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(linear_trigger.urllib_request, "urlopen", http)
    product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue", api_key="fake"))
    with pytest.raises(LinearGraphQLClientError):
        product.create_comment("issue-id", "progress")
    http.assert_called_once()
    request = http.call_args.args[0]
    assert request.get_header("Authorization") == "fake"
    sent = json.loads(request.data)
    assert "commentCreate" in sent["query"] and "issueUpdate" not in sent["query"]
    assert sent["variables"] == {"issueId": "issue-id", "body": "progress"}


def test_known_claims_are_excluded_before_queue_limit(client, route):
    client._execute.return_value = connection()
    assert client.queued_issue_ids(route, 1, exclude_ids=[ID.upper()]) == []
    query, variables = client._execute.call_args.args
    assert "nin: $excluded" in query
    assert variables["excluded"] == [ID.lower()] and variables["limit"] == 1
    client._execute.return_value = connection([{"id": ID, "identifier": "ABC-1"}])
    with pytest.raises(LinearGraphQLClientError):
        client.queued_issue_ids(route, 1, exclude_ids=[ID])


@pytest.mark.parametrize("values", ["bad", ["bad"], [ID, ID.upper()], [ID] * 10001, [None]])
def test_bad_exclusions_never_query(client, route, values):
    with pytest.raises(ValueError):
        client.queued_issue_ids(route, exclude_ids=values)
    client._execute.assert_not_called()


def test_product_state_mutations_are_disabled(client):
    with pytest.raises(LinearGraphQLClientError, match="preserves human workflow state"):
        client.update_issue_state("issue", "state")
    client._execute.assert_not_called()


@pytest.mark.parametrize("errors", [None, [], {"message": "old convention"}])
def test_strict_envelopes_do_not_change_legacy_transport(monkeypatch, errors):
    payload = {"errors": errors, "data": {"accepted": True}}
    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return json.dumps(payload).encode()
    monkeypatch.setattr("auto_mindsdb_factory.linear_trigger.urllib_request.urlopen", lambda *a, **k: Response())
    legacy = LinearGraphQLClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue", api_key="fake"))
    assert legacy._execute("query", {}) == {"accepted": True}
