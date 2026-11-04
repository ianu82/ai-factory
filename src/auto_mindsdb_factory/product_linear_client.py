"""Narrow, fail-closed Linear polling for a trusted product route."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path
from uuid import UUID

from .linear_trigger import LinearGraphQLClient, LinearGraphQLClientError
from .product_linear_intake import LinearProductRoute


class LinearProductClient(LinearGraphQLClient):
    def _execute(self, query, variables, *, timeout_seconds=30):
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise LinearGraphQLClientError("Invalid Linear request time budget")
        timeout_seconds = min(30, timeout_seconds)
        config = {key: getattr(self.config, key) for key in
                  ("target_team_id", "target_state_id", "api_key", "graphql_url")}
        packet = {"config": config, "query": query, "variables": variables, "timeout_seconds": timeout_seconds}
        names = {"PATH", "LANG", "LC_ALL", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
                 "http_proxy", "https_proxy", "all_proxy", "no_proxy", "SSL_CERT_FILE", "SSL_CERT_DIR"}
        env = {key: value for key, value in os.environ.items() if key in names}
        env.update(PYTHONPATH=str(Path(__file__).resolve().parents[1]),
                   PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
        try:
            # The credential travels through stdin, never process arguments or logs.
            result = subprocess.run([sys.executable, "-m", "auto_mindsdb_factory.product_linear_http"],
                input=json.dumps(packet), capture_output=True, text=True, timeout=timeout_seconds, env=env,
                cwd=Path(__file__).resolve().parents[1])
        except subprocess.TimeoutExpired:
            raise LinearGraphQLClientError("Linear request exceeded its time budget") from None
        except OSError:
            raise LinearGraphQLClientError("Linear request process unavailable") from None
        try:
            payload = json.loads(result.stdout)
            if result.returncode or not isinstance(payload, dict) or set(payload) != {"data"} or not isinstance(payload["data"], dict):
                raise ValueError()
        except (ValueError, TypeError):
            raise LinearGraphQLClientError("Linear GraphQL request failed") from None
        return payload["data"]

    def _decode_response(self, raw):
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("Duplicate JSON object key")
                result[key] = value
            return result
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
        pending = [result]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                pending.extend(value.keys())
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, str):
                value.encode("utf-8")
            elif isinstance(value, float) and not math.isfinite(value):
                raise ValueError("Non-finite JSON number")
        return result

    def _validate_envelope(self, payload):
        if not isinstance(payload, dict):
            raise LinearGraphQLClientError("Linear response must be an object")
        if (set(payload) - {"data", "errors", "extensions"}
                or ("extensions" in payload and not isinstance(payload["extensions"], dict))):
            raise LinearGraphQLClientError("Invalid Linear GraphQL response envelope")
        if "errors" in payload and (not isinstance(payload["errors"], list) or not payload["errors"]):
            raise LinearGraphQLClientError("Invalid Linear GraphQL errors envelope")

    def update_issue_state(self, issue_id: str, state_id: str):
        raise LinearGraphQLClientError("Product delivery preserves human workflow state")

    def queued_issue_ids(self, route: LinearProductRoute, limit: int = 10,
                         *, exclude_ids: list[str] | None = None) -> list[str]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("Linear queue limit must be an integer from 1 to 100")
        excluded = [] if exclude_ids is None else exclude_ids
        if not isinstance(excluded, list) or len(excluded) > 10000 or any(
            not isinstance(value, str) or not re.fullmatch(
                r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value) for value in excluded
        ):
            raise ValueError("Excluded issue IDs must be a bounded list of UUIDs")
        excluded = [str(UUID(value)) for value in excluded]
        if len(excluded) != len(set(excluded)):
            raise ValueError("Excluded issue IDs must be unique")
        config = route.document
        data = self._execute(
            """
            query ProductLinearQueue($team: ID!, $state: ID!, $label: String!, $limit: Int!, $excluded: [ID!]!) {
              issues(first: $limit, filter: {id: {nin: $excluded}, team: {id: {eq: $team}},
                state: {id: {eq: $state}}, labels: {some: {name: {eq: $label}}}}) {
                nodes { id identifier }
                pageInfo { hasNextPage endCursor }
              }
            }
            """,
            {"team": config["team_id"], "state": config["queued_state_id"],
             "label": config["required_label"], "limit": limit, "excluded": excluded},
        )
        connection = data.get("issues") if isinstance(data, dict) else None
        if not isinstance(connection, dict):
            raise LinearGraphQLClientError("Invalid Linear issue connection")
        nodes, page = connection.get("nodes"), connection.get("pageInfo")
        if (not isinstance(nodes, list) or len(nodes) > limit or not isinstance(page, dict)
                or not isinstance(page.get("hasNextPage"), bool) or "endCursor" not in page
                or (page["endCursor"] is not None and
                    (not isinstance(page["endCursor"], str) or not page["endCursor"].strip()))
                or (page["hasNextPage"] and not page["endCursor"])):
            raise LinearGraphQLClientError("Invalid Linear issue connection")
        ids: list[str] = []
        for node in nodes:
            if (not isinstance(node, dict) or not isinstance(node.get("id"), str)
                    or not re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", node["id"])
                    or not isinstance(node.get("identifier"), str) or not node["identifier"].strip()):
                raise LinearGraphQLClientError("Invalid Linear issue node")
            issue_id = str(UUID(node["id"]))
            if issue_id in ids or issue_id in excluded:
                raise LinearGraphQLClientError("Duplicate or excluded Linear issue ID")
            ids.append(issue_id)
        return ids

    def create_comment(self, issue_id: str, body: str) -> str:
        # Requirements are frozen: even an Entity not found error must not fall
        # back to the legacy description update.
        try:
            data = self._execute(
                """mutation FactoryLinearComment($issueId: String!, $body: String!) {
                  commentCreate(input: {issueId: $issueId, body: $body}) {
                    success comment { id }
                  }
                }""",
                {"issueId": issue_id, "body": body},
            )
        except AttributeError as exc:
            # The inherited transport assumes the JSON envelope is an object.
            raise LinearGraphQLClientError("Invalid Linear comment response") from exc
        payload = data.get("commentCreate") if isinstance(data, dict) else None
        comment = payload.get("comment") if isinstance(payload, dict) else None
        comment_id = comment.get("id") if isinstance(comment, dict) else None
        if (not isinstance(payload, dict) or payload.get("success") is not True
                or not isinstance(comment_id, str) or not comment_id.strip()):
            raise LinearGraphQLClientError("Invalid Linear commentCreate response")
        return comment_id

    def fetch_team_states(self, team_id: str) -> list[dict]:
        # The legacy reader filters malformed nodes. Inspect the raw connection
        # here so a malformed duplicate cannot hide a contradictory state.
        data = self._execute(
            """query FactoryLinearTeamStates($id: String!) {
              team(id: $id) { id states(first: 250) {
                nodes { id name type } pageInfo { hasNextPage }
              } }
            }""", {"id": team_id},
        )
        team = data.get("team") if isinstance(data, dict) else None
        connection = team.get("states") if isinstance(team, dict) else None
        if (not isinstance(team, dict) or team.get("id") != team_id
                or not isinstance(connection, dict)
                or not isinstance(connection.get("nodes"), list)
                or not isinstance(connection.get("pageInfo"), dict)
                or connection["pageInfo"].get("hasNextPage") is not False):
            raise LinearGraphQLClientError("Invalid Linear team states")
        nodes = connection["nodes"]
        if len(nodes) > 250 or any(not isinstance(node, dict) or any(
                not isinstance(node.get(key), str) or not node[key].strip()
                for key in ("id", "name", "type")) for node in nodes):
            raise LinearGraphQLClientError("Invalid Linear team states")
        return nodes

    def validate_route(self, route: LinearProductRoute) -> None:
        config = route.document
        states = self.fetch_team_states(config["team_id"])
        if not isinstance(states, list):
            raise LinearGraphQLClientError("Invalid Linear team states")
        by_id = {}
        for state in states:
            if (not isinstance(state, dict) or not isinstance(state.get("id"), str)
                    or not state["id"].strip() or state["id"] in by_id):
                raise LinearGraphQLClientError("Invalid Linear team states")
            by_id[state["id"]] = state.get("type")
        fields = [("queued_state_id", {"unstarted", "backlog"})]
        if config["version"] == 1:
            fields += [("building_state_id", {"started"}), ("blocked_state_id", {"started"}),
                       ("completed_state_id", {"completed"})]
        for field, allowed in fields:
            state_type = by_id.get(config[field])
            if not isinstance(state_type, str) or state_type not in allowed:
                raise LinearGraphQLClientError(f"Invalid Linear route state: {field}")
