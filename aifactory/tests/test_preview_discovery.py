"""Discovery with fake GitHub HTTP and deterministic monotonic time."""
import asyncio
from dataclasses import asdict
import json

import httpx
import pytest

from aifactory.github import GitHub, REQUIRED_PERMISSIONS, PERMISSION_INSTRUCTION
from aifactory.github_app import APP_PERMISSIONS
from aifactory.pipeline.preview_discovery import discover_preview

SHA = "a" * 40
TOKEN = "control-plane-secret"
URL = "https://preview.example.test"
CLIENT = httpx.AsyncClient


class Clock:
    now = 0.0

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.now += seconds
        await asyncio.sleep(0)


@pytest.fixture
def fake(monkeypatch):
    calls = []

    def install(handler):
        def checked(request):
            calls.append(request)
            assert request.headers["authorization"] == "Bearer " + TOKEN
            assert request.url.host == "api.github.com"
            assert TOKEN not in str(request.url) + request.content.decode()
            return handler(request)

        monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: CLIENT(**kw, transport=httpx.MockTransport(checked)))
        return calls

    return install


def deployment(id=1, environment="Preview", sha=SHA):
    return {"id": id, "sha": sha, "environment": environment, "payload": {"url": URL}, "url": URL}


def status(id=1, state="success", url=URL):
    return {"id": id, "state": state, "environment_url": url, "target_url": URL}


async def discover(**kwargs):
    clock = kwargs.pop("clock", Clock())
    return await discover_preview(GitHub(TOKEN), "acme/app", SHA, clock=clock, sleep=clock.sleep,
                                  timeout_seconds=kwargs.pop("timeout_seconds", 6), poll_interval=2, **kwargs)


@pytest.mark.parametrize("environment", ["Preview", "railway-pr-25", "deploy-preview-25", "upsun/pr-25"])
async def test_provider_neutral_paginated_exact_head_and_environment(fake, environment):
    def handler(r):
        page = int(r.url.params["page"])
        if r.url.path.endswith("/deployments"):
            assert r.url.params["sha"] == SHA
            return httpx.Response(200, json=[deployment(i + 100, "production") for i in range(100)] if page == 1 else [
                deployment(5, environment, "b" * 40), deployment(4, environment.swapcase()), deployment(3, environment)])
        assert "/deployments/3/statuses" in r.url.path
        return httpx.Response(200, json=[status(i + 1, "failure") for i in range(100)] if page == 1 else [status(101)])

    calls = fake(handler)
    result = await discover(environment_pattern=environment)
    assert result.outcome == "success"
    assert (result.sha, result.deployment_id, result.status_id, result.environment, result.url) == (SHA, 3, 101, environment, URL)
    assert len(calls) == 4
    assert TOKEN not in json.dumps(asdict(result))


@pytest.mark.parametrize("state", ["failure", "inactive", "pending", "queued", "in_progress", "error"])
async def test_newest_deployment_never_falls_back_to_older_success(fake, state):
    def handler(r):
        if r.url.path.endswith("/deployments"):
            # Deliberately unsorted: selection is by ID, not API array order.
            return httpx.Response(200, json=[deployment(10), deployment(20), deployment(5)])
        assert r.url.path.endswith("/20/statuses"), "Must never query the older successful deployment"
        return httpx.Response(200, json=[status(2, state), status(1)])

    fake(handler)
    clock = Clock()
    result = await discover(clock=clock)
    assert result.outcome == "timeout" and result.elapsed_seconds == 6
    assert result.deployment_id == 20 and result.status_id == 2
    assert result.deployment_state == state and result.url is None
    assert clock.now == 6


@pytest.mark.parametrize("url", ["", None, "not a URL", "javascript:alert(1)", "https://user:password@preview.test",
                                  "https://preview.test/?token=secret", "https://preview.test/#secret",
                                  "https://preview.test/" + TOKEN, "https://preview.test/\n"])
async def test_only_valid_status_environment_url(fake, url):
    fake(lambda r: httpx.Response(200, json=[deployment()] if r.url.path.endswith("/deployments") else [status(url=url)]))
    result = await discover()
    assert result.outcome == "timeout" and result.url is None


async def test_delayed_success_and_fnmatch(fake):
    clock = Clock()
    fake(lambda r: httpx.Response(200, json=[deployment(environment="Preview/pr-25")] if r.url.path.endswith("/deployments")
                                  else [status(state="pending" if clock.now < 4 else "success")]))
    result = await discover(clock=clock, environment_pattern="Preview/pr-*")
    assert result.outcome == "success" and result.elapsed_seconds == 4


async def test_no_status_and_no_matching_head_timeout(fake):
    fake(lambda r: httpx.Response(200, json=[deployment(sha="b" * 40)]))
    result = await discover()
    assert result.outcome == "timeout" and result.deployment_id is None


async def test_newest_without_status_does_not_fall_back(fake):
    calls = fake(lambda r: httpx.Response(200, json=[deployment(2), deployment()] if r.url.path.endswith("/deployments") else []))
    result = await discover()
    assert result.outcome == "timeout" and result.deployment_id == 2
    assert all(not r.url.path.endswith("/1/statuses") for r in calls)


@pytest.mark.parametrize("code,outcome", [(401, "api_error"), (403, "missing_permission"), (404, "missing_permission"),
                                          (429, "api_error"), (500, "api_error")])
async def test_permissions_and_api_errors_are_safe(fake, code, outcome):
    calls = fake(lambda r: httpx.Response(code, json={"message": TOKEN}))
    result = await discover()
    assert result.outcome == outcome
    assert TOKEN not in json.dumps(asdict(result))
    assert len(calls) == 1  # no retries outside the discovery budget
    if outcome == "missing_permission":
        assert "Deployments read permission" in result.message


async def test_request_timeout_uses_remaining_budget(monkeypatch):
    clock = Clock()
    seen = []

    async def request(self, method, path, **kwargs):
        seen.append(kwargs)
        clock.now += kwargs["timeout"]
        raise TimeoutError

    monkeypatch.setattr(GitHub, "request", request)
    result = await discover(clock=clock, timeout_seconds=0.75)
    assert result.outcome == "timeout" and result.elapsed_seconds == 0.75
    assert seen[0]["timeout"] == 0.75 and seen[0]["retries"] == 0


async def test_cancellation_interrupts_inflight_request(monkeypatch):
    event = asyncio.Event()
    cleaned = asyncio.Event()

    async def request(self, *args, **kwargs):
        event.set()
        try:
            await asyncio.Future()
        finally:
            cleaned.set()

    monkeypatch.setattr(GitHub, "request", request)
    result = await discover(cancel=event)
    assert result.outcome == "cancelled" and cleaned.is_set()


async def test_calling_task_cancellation_propagates(monkeypatch):
    entered = asyncio.Event()
    cleaned = asyncio.Event()

    async def request(self, *args, **kwargs):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cleaned.set()

    monkeypatch.setattr(GitHub, "request", request)
    task = asyncio.create_task(discover())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set()


async def test_manual_template_bypasses_github(fake):
    calls = fake(lambda r: pytest.fail("Manual mode must not contact GitHub"))
    result = await discover(manual_template="https://pr-{pr_number}.example.test/{owner}/{repo}/{head_sha}/{branch}",
                            pr_number=25, branch="feature/a?b#c")
    assert result.outcome == "success" and result.provenance == "operator_supplied"
    assert result.url == f"https://pr-25.example.test/acme/app/{SHA}/feature%2Fa%3Fb%23c"
    assert result.deployment_id is None and "operator supplied" in result.message
    assert calls == []


@pytest.mark.parametrize("template", ["https://{unknown}.test", "https://{branch.__class__}.test",
                                      "https://{repo[0]}.test", "https://{repo!r}.test", "https://{repo:>10}.test",
                                      "https://{repo", "https://user:secret@preview.test", "file:///tmp/preview"])
async def test_manual_template_rejects_unsafe_placeholders(fake, template):
    calls = fake(lambda r: pytest.fail("Manual mode must not contact GitHub"))
    result = await discover(manual_template=template)
    assert result.outcome == "invalid_template" and calls == []


def test_deployment_permission_contracts():
    assert APP_PERMISSIONS["deployments"] == REQUIRED_PERMISSIONS["deployments"] == "read"
    assert "Deployments: Read-only" in PERMISSION_INSTRUCTION


@pytest.mark.parametrize("code", [403, 404])
async def test_credential_check_lists_missing_deployments_permission(fake, code):
    def handler(r):
        if r.url.path == "/user":
            return httpx.Response(200, json={"login": "operator"})
        if r.url.path == "/repos/acme/app":
            return httpx.Response(200, json={"private": True, "default_branch": "main"})
        return httpx.Response(code if r.url.path.endswith("/deployments") else 200, json=[])

    fake(handler)
    state, _, metadata = await GitHub(TOKEN).diagnose("acme/app")
    assert state == "invalid" and metadata["diagnostic_code"] == "github_permissions_missing"
    permission = next(p for p in metadata["permissions"] if p["permission"] == "deployments")
    assert permission["state"] == "missing" and "Deployments: Read-only" in permission["instruction"]


@pytest.mark.parametrize("sha_length", [40, 64])
async def test_uppercase_head_is_canonicalized_for_query_comparison_and_evidence(fake, sha_length):
    sha = "abcdef01" * (sha_length // 8)

    def handler(r):
        if r.url.path.endswith("/deployments"):
            assert r.url.params["sha"] == sha
            return httpx.Response(200, json=[deployment(sha=sha)])
        return httpx.Response(200, json=[status()])

    fake(handler)
    clock = Clock()
    result = await discover_preview(GitHub(TOKEN), "acme/app", sha.upper(), clock=clock, sleep=clock.sleep)
    assert result.outcome == "success" and result.sha == sha and result.url == URL


@pytest.mark.parametrize("installation", [False, True])
@pytest.mark.parametrize("code,headers,message", [
    (403, {"x-ratelimit-remaining": "0"}, "Forbidden"),
    (403, {"retry-after": "60"}, "Forbidden"),
    (403, {}, "You have exceeded a secondary rate limit"),
    (403, {}, "API rate limit exceeded"),
    (403, {}, "Request blocked due to abuse detection"),
    (429, {}, "Too many requests"),
])
async def test_rate_limits_are_not_missing_permissions(fake, monkeypatch, installation, code, headers, message):
    from aifactory.github_app import InstallationCredential

    credential = TOKEN
    if installation:
        credential = InstallationCredential("workspace", "123")
        monkeypatch.setattr(InstallationCredential, "resolve", lambda self: TOKEN)
        monkeypatch.setattr(InstallationCredential, "invalidate", lambda self: pytest.fail("Rate limiting is not invalid auth"))
    calls = fake(lambda r: httpx.Response(code, headers=headers, json={"message": message + " " + TOKEN}))
    clock = Clock()
    result = await discover_preview(GitHub(credential), "acme/app", SHA, clock=clock, sleep=clock.sleep)
    assert result.outcome == "api_error" and "rate limited" in result.message
    assert TOKEN not in json.dumps(asdict(result))
    assert len(calls) == 1
