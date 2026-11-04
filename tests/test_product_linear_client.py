import copy
import re
from unittest.mock import Mock

import pytest

from auto_mindsdb_factory.linear_trigger import LinearGraphQLClient, LinearGraphQLClientError
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
    assert "issues(first:$limit,filter:{team:{id:{eq:$team}},state:{id:{eq:$state}},labels:{some:{name:{eq:$label}}}})" in compact
    assert variables == {"team": "team", "state": "queue", "label": 'factory"label', "limit": 100}
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
        "nodes": [{**s, "name": s["id"]} for s in states()]}}}
    assert client.validate_route(route) is None
    assert client._execute.call_args.args[1] == {"id": "team"}
