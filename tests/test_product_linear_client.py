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
def test_comment_uses_authenticated_transport_and_never_falls_back(graphql_server, response):
    with graphql_server(response) as (url, requests):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        with pytest.raises(LinearGraphQLClientError):
            product.create_comment("issue-id", "progress")
    assert len(requests) == 1
    headers, sent = requests[0]
    assert headers.get("Authorization") == "fake"
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


@pytest.fixture
def graphql_server():
    import threading
    import time
    from contextlib import contextmanager
    from http.server import BaseHTTPRequestHandler, HTTPServer
    @contextmanager
    def serve(payload, trickle=False, extra_length=0, redirect_to=None, redirect_code=302,
              status=200, content_type="application/json", raw_body=None):
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                requests.append((self.headers, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
                self.reply()
            def do_GET(self):
                requests.append((self.headers, None))
                self.reply()
            def reply(self):
                if redirect_to:
                    self.send_response(redirect_code); self.send_header("Location", redirect_to)
                    self.send_header("Content-Length", "0"); self.end_headers(); return
                body = json.dumps(payload).encode() if raw_body is None else raw_body
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                if not trickle: self.send_header("Content-Length", str(len(body) + extra_length))
                self.end_headers()
                try:
                    if trickle:
                        for _ in range(3000):
                            self.wfile.write(b" "); self.wfile.flush(); time.sleep(.01)
                    else:
                        self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass
        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01), daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", requests
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=5)
    return serve


def test_trickling_response_cannot_outlive_request_deadline(graphql_server):
    import time
    with graphql_server({}, trickle=True) as (url, requests):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        started = time.monotonic()
        with pytest.raises(LinearGraphQLClientError, match="time budget"):
            product._execute("query", {}, timeout_seconds=5)
        assert time.monotonic() - started < 8
        assert len(requests) == 1


def test_product_response_size_is_bounded(graphql_server):
    with graphql_server({"data": {"large": "x" * (4 * 1024 * 1024 + 100)}}) as (url, _):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        with pytest.raises(LinearGraphQLClientError):
            product._execute("query", {})


def test_valid_comment_round_trips_through_request_process(graphql_server):
    with graphql_server({"data": {"commentCreate": {"success": True, "comment": {"id": "comment-1"}}}}) as (url, _):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        assert product.create_comment("issue", "body") == "comment-1"


def test_truncated_success_response_cannot_confirm_comment(graphql_server):
    payload = {"data": {"commentCreate": {"success": True, "comment": {"id": "id"}}}}
    with graphql_server(payload, extra_length=200) as (url, _):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        with pytest.raises(LinearGraphQLClientError):
            product.create_comment("issue", "body")


def test_request_process_cannot_import_from_callers_cwd(graphql_server, tmp_path, monkeypatch):
    package = tmp_path / "auto_mindsdb_factory"
    package.mkdir(); (package / "__init__.py").write_text("")
    marker = tmp_path / "stolen"
    (package / "product_linear_http.py").write_text(
        f"from pathlib import Path; Path({str(marker)!r}).write_text('shadowed')")
    monkeypatch.chdir(tmp_path)
    payload = {"data": {"commentCreate": {"success": True, "comment": {"id": "legitimate"}}}}
    with graphql_server(payload) as (url, requests):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        assert product.create_comment("issue", "body") == "legitimate"
    assert not marker.exists() and len(requests) == 1


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_product_redirect_never_forwards_credentials_or_confirms_write(graphql_server, code):
    success = {"data": {"commentCreate": {"success": True, "comment": {"id": "fabricated"}}}}
    with graphql_server(success) as (destination, received):
        with graphql_server({}, redirect_to=destination, redirect_code=code) as (origin, sent):
            product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
                api_key="fake", graphql_url=origin))
            with pytest.raises(LinearGraphQLClientError):
                product.create_comment("issue", "body")
    assert len(sent) == 1 and received == []


@pytest.mark.parametrize("options", [{"status": 206}, {"content_type": "text/html"}])
def test_non_graphql_http_success_cannot_confirm_comment(graphql_server, options):
    success = {"data": {"commentCreate": {"success": True, "comment": {"id": "id"}}}}
    with graphql_server(success, **options) as (url, _):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        with pytest.raises(LinearGraphQLClientError):
            product.create_comment("issue", "body")


@pytest.mark.parametrize("raw", [
    b'{"data":{"commentCreate":{"success":true,"comment":{"id":"id"}}},"extensions":{"number":NaN}}',
    b'{"data":{"commentCreate":{"success":false,"success":true,"comment":{"id":"id"}}}}',
    b'{"data":{"commentCreate":{"success":true,"comment":{"id":"id"}}},"extensions":{"number":1e9999}}',
    b'{"data":{"commentCreate":{"success":true,"comment":{"id":"\\ud800"}}}}',
    b'{"data":{"commentCreate":{"success":true,"comment":{"id":"id"}}},"error":"rejected"}',
    b'{"data":{"commentCreate":{"success":true,"comment":{"id":"id"}}},"extensions":null}',
])
def test_ambiguous_or_invalid_json_cannot_confirm_comment(graphql_server, raw):
    with graphql_server({}, raw_body=raw) as (url, _):
        product = LinearProductClient(LinearTriggerConfig(target_team_id="team", target_state_id="queue",
            api_key="fake", graphql_url=url))
        with pytest.raises(LinearGraphQLClientError):
            product.create_comment("issue", "body")


@pytest.mark.parametrize('budget, expected', [(5, 5), (45, 30)])
def test_real_http_launch_keeps_credentials_on_stdin(graphql_server, monkeypatch, budget, expected):
    import subprocess
    from pathlib import Path
    import auto_mindsdb_factory.product_linear_client as module
    actual_run = subprocess.run
    launches = []
    monkeypatch.setenv('OPENAI_API_KEY', 'model-secret')
    monkeypatch.setenv('LINEAR_API_KEY', 'linear-secret')
    monkeypatch.setenv('GH_TOKEN', 'github-secret')
    monkeypatch.setenv('PYTHONPATH', '/untrusted-source')
    monkeypatch.setenv('PYTHONSTARTUP', '/untrusted-startup')
    def launch(argv, **kwargs):
        launches.append((argv, kwargs))
        return actual_run(argv, **kwargs)
    monkeypatch.setattr(module.subprocess, 'run', launch)
    with graphql_server({'data': {'ok': True}}) as (url, requests):
        product = LinearProductClient(LinearTriggerConfig(target_team_id='team', target_state_id='queue',
            api_key='linear-secret', graphql_url=url))
        assert product._execute('query Probe { ok }', {}, timeout_seconds=budget) == {'ok': True}
    assert len(launches) == len(requests) == 1
    argv, options = launches[0]
    packet = json.loads(options['input'])
    assert packet['config']['api_key'] == 'linear-secret'
    assert packet['timeout_seconds'] == options['timeout'] == expected
    assert not any(secret in repr(argv) + repr(options['env'])
                   for secret in ('linear-secret', 'model-secret', 'github-secret'))
    assert not {'OPENAI_API_KEY', 'LINEAR_API_KEY', 'GH_TOKEN', 'PYTHONSTARTUP'} & options['env'].keys()
    assert options['cwd'] == Path(module.__file__).resolve().parents[1]
    assert options['env']['PYTHONPATH'] == str(options['cwd'])
    assert options['env']['PYTHONNOUSERSITE'] == '1'
