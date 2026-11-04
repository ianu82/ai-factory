"""Narrow, fail-closed Linear polling for a trusted product route."""

from __future__ import annotations

import re
from uuid import UUID

from .linear_trigger import LinearGraphQLClient, LinearGraphQLClientError
from .product_linear_intake import LinearProductRoute


class LinearProductClient(LinearGraphQLClient):
    def queued_issue_ids(self, route: LinearProductRoute, limit: int = 10) -> list[str]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("Linear queue limit must be an integer from 1 to 100")
        config = route.document
        data = self._execute(
            """
            query ProductLinearQueue($team: ID!, $state: ID!, $label: String!, $limit: Int!) {
              issues(first: $limit, filter: {team: {id: {eq: $team}},
                state: {id: {eq: $state}}, labels: {some: {name: {eq: $label}}}}) {
                nodes { id identifier }
                pageInfo { hasNextPage endCursor }
              }
            }
            """,
            {"team": config["team_id"], "state": config["queued_state_id"],
             "label": config["required_label"], "limit": limit},
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
            if issue_id in ids:
                raise LinearGraphQLClientError("Duplicate Linear issue ID")
            ids.append(issue_id)
        return ids

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
        for field, allowed in (("queued_state_id", {"unstarted", "backlog"}),
                               ("building_state_id", {"started"}),
                               ("blocked_state_id", {"started"}),
                               ("completed_state_id", {"completed"})):
            state_type = by_id.get(config[field])
            if not isinstance(state_type, str) or state_type not in allowed:
                raise LinearGraphQLClientError(f"Invalid Linear route state: {field}")
