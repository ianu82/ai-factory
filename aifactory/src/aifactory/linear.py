"""Linear access for the control plane only; credentials never enter a sandbox."""

from __future__ import annotations

from typing import Any, Protocol

import httpx

from .db import session_scope
from .models import Secret
from .security import decrypt


class LinearError(RuntimeError):
    def __init__(self, category: str):
        self.category = category
        super().__init__(f"Linear request failed ({category})")


class LinearProvider(Protocol):
    async def viewer(self) -> dict[str, Any]: ...
    async def teams(self) -> list[dict[str, Any]]: ...
    async def team_states(self, team_id: str) -> list[dict[str, Any]]: ...
    async def issues(self, team_id: str, state_id: str, label: str) -> list[dict[str, Any]]: ...
    async def issue_state(self, issue_id: str) -> dict[str, Any]: ...
    async def update_issue_state(self, issue_id: str, state_id: str) -> None: ...
    async def comments(self, issue_id: str) -> list[dict[str, Any]]: ...
    async def create_comment(self, issue_id: str, body: str) -> dict[str, Any]: ...


def resolve_token(workspace_id: str) -> str:
    with session_scope() as db:
        sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == "linear_api_key").one_or_none()
        if sec is not None and sec.status != "invalid":
            return decrypt(sec.ciphertext)
    raise LinearError("not_configured")


def _object(value: Any, *fields: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(value.get(field), str) for field in fields):
        raise LinearError("malformed")
    return value


class Linear:
    def __init__(self, token: str, *, transport: httpx.AsyncBaseTransport | None = None):
        self._token = token
        self._transport = transport

    async def _request(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=httpx.Timeout(15, connect=5)) as client:
                response = await client.post("https://api.linear.app/graphql", headers={"Authorization": self._token},
                                             json={"query": query, "variables": variables or {}})
            if response.status_code in (401, 403):
                raise LinearError("authentication")
            if response.status_code == 429:
                raise LinearError("rate_limit")
            if response.status_code >= 500:
                raise LinearError("unavailable")
            if not response.is_success:
                raise LinearError("http")
            payload = _object(response.json())
            if payload.get("errors"):
                errors = payload["errors"]
                codes = {error.get("extensions", {}).get("code") for error in errors
                         if isinstance(error, dict) and isinstance(error.get("extensions"), dict)} if isinstance(errors, list) else set()
                category = "authentication" if codes & {"AUTHENTICATION_ERROR", "UNAUTHENTICATED"} else "graphql"
                if codes & {"RATELIMITED", "RATE_LIMITED"}:
                    category = "rate_limit"
                raise LinearError(category)
            return _object(payload.get("data"))
        except httpx.TimeoutException:
            raise LinearError("timeout") from None
        except httpx.RequestError:
            raise LinearError("network") from None
        except (ValueError, TypeError):
            raise LinearError("malformed") from None

    async def _nodes(self, query: str, variables: dict[str, Any], path: tuple[str, ...],
                     fields: tuple[str, ...]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = None
        for _page in range(100):
            connection = await self._request(query, {**variables, "after": cursor})
            for key in path:
                connection = _object(connection.get(key))
            nodes = connection.get("nodes")
            if not isinstance(nodes, list):
                raise LinearError("malformed")
            rows.extend(_object(node, *fields) for node in nodes)
            page = _object(connection.get("pageInfo"))
            if page.get("hasNextPage") is False:
                return rows
            next_cursor = page.get("endCursor")
            if page.get("hasNextPage") is not True or not isinstance(next_cursor, str) or next_cursor == cursor:
                raise LinearError("malformed")
            cursor = next_cursor
        raise LinearError("pagination_limit")

    async def viewer(self) -> dict[str, Any]:
        return _object((await self._request("query { viewer { id name } }")).get("viewer"), "id", "name")

    async def teams(self) -> list[dict[str, Any]]:
        return await self._nodes("""query($after: String) { teams(first: 100, after: $after) {
            nodes { id name key } pageInfo { hasNextPage endCursor } } }""", {}, ("teams",), ("id", "name"))

    async def team_states(self, team_id: str) -> list[dict[str, Any]]:
        return await self._nodes("""query($id: String!, $after: String) { team(id: $id) {
            states(first: 100, after: $after) { nodes { id name type } pageInfo { hasNextPage endCursor } }
            } }""", {"id": team_id}, ("team", "states"), ("id", "name", "type"))

    async def issues(self, team_id: str, state_id: str, label: str) -> list[dict[str, Any]]:
        return await self._nodes("""query($filter: IssueFilter!, $after: String) {
            issues(filter: $filter, first: 100, after: $after) {
            nodes { id identifier title description url priority state { id name } }
            pageInfo { hasNextPage endCursor } } }""",
            {"filter": {"team": {"id": {"eq": team_id}}, "state": {"id": {"eq": state_id}},
                        "labels": {"some": {"name": {"eq": label}}}}}, ("issues",), ("id", "identifier", "title", "url"))

    async def issue_state(self, issue_id: str) -> dict[str, Any]:
        data = await self._request("query($id: String!) { issue(id: $id) { state { id name } } }", {"id": issue_id})
        return _object(_object(data.get("issue")).get("state"), "id", "name")

    async def update_issue_state(self, issue_id: str, state_id: str) -> None:
        data = await self._request("""mutation($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) { success } }""", {"id": issue_id, "input": {"stateId": state_id}})
        self._mutation(data, "issueUpdate")

    async def comments(self, issue_id: str) -> list[dict[str, Any]]:
        return await self._nodes("""query($id: String!, $after: String) { issue(id: $id) {
            comments(first: 100, after: $after) { nodes { id body } pageInfo { hasNextPage endCursor } }
            } }""", {"id": issue_id}, ("issue", "comments"), ("id", "body"))

    async def create_comment(self, issue_id: str, body: str) -> dict[str, Any]:
        data = await self._request("""mutation($input: CommentCreateInput!) {
            commentCreate(input: $input) { success comment { id body } } }""", {"input": {"issueId": issue_id, "body": body}})
        return _object(self._mutation(data, "commentCreate").get("comment"), "id", "body")

    @staticmethod
    def _mutation(data: dict[str, Any], key: str) -> dict[str, Any]:
        result = _object(data.get(key))
        if result.get("success") is False:
            raise LinearError("mutation_failed")
        if result.get("success") is not True:
            raise LinearError("malformed")
        return result
