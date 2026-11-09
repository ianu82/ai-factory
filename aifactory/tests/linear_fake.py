"""Reusable in-process GraphQL endpoint; never makes network requests."""

import json

import httpx


class FakeLinear(httpx.MockTransport):
    def __init__(self, data=None, *, status=200, errors=None, failure=None):
        self.requests = []
        self.data = data if data is not None else {"viewer": {"id": "viewer-1", "name": "Fake Account"}}
        self.status, self.errors, self.failure = status, errors, failure
        super().__init__(self.handle)

    def handle(self, request):
        self.requests.append((request, json.loads(request.content)))
        if self.failure:
            raise self.failure
        return httpx.Response(self.status, json={"data": self.data, "errors": self.errors})


def connection(nodes, cursor=None):
    return {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}
