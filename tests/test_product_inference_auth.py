import asyncio
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from auto_mindsdb_factory.__main__ import main
from auto_mindsdb_factory import product_inference_auth as auth
from auto_mindsdb_factory.product_inference import InferencePolicy, InferencePolicyError


FACTORY_WORKSPACE = "00000000-0000-4000-8000-000000000001"
AUTH_WORKSPACE = "00000000-0000-4000-8000-000000000002"
REFERENCE = "00000000-0000-4000-8000-000000000003"
KEY = "mdb_example.private-test-value"


def policy_document():
    return {"version": 1, "provider": "mindshub", "protocol": "responses",
        "endpoint": "https://api.mindshub.ai/v1/responses", "workspace_id": FACTORY_WORKSPACE,
        "credential_ref": {"workspace_id": FACTORY_WORKSPACE, "id": REFERENCE, "revision": 1}}


def profile_file(root):
    path = root / "profile.json"
    path.write_text(json.dumps({"version": 1, "name": "sample", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["true"]},
        "smoke": [["true"]], "inference": policy_document()}))
    return path


def test_missing_mindshub_key_never_borrows_openai(validation_checkout, monkeypatch, capsys):
    root, target = validation_checkout
    monkeypatch.delenv("AI_FACTORY_MINDSHUB_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", KEY)
    async def forbidden(*args):
        raise AssertionError("No credential or network fallback")
    monkeypatch.setattr(auth, "_read_identity", forbidden)
    assert main(["product-inference-check", "--profile", str(profile_file(root)),
        "--target-root", str(target), "--auth-workspace-id", AUTH_WORKSPACE]) == 1
    result = capsys.readouterr()
    assert "MindsHub API key is not configured" in result.err
    assert result.out == "" and KEY not in result.err


def identity(**extra):
    return {"valid": True, "auth_method": "api_key", "user_id": REFERENCE,
        "organization_id": FACTORY_WORKSPACE, "workspace_id": AUTH_WORKSPACE,
        "principal_kind": "user", "key_type": "user", "credential_kind": "api_key",
        "key_prefix": "mdb_example", "email": KEY, "entitlements": {"private": KEY}, **extra}


def test_receipt_binds_distinct_workspaces_reference_and_verified_identity():
    policy = InferencePolicy.from_document(policy_document())
    result = auth._receipt(policy, KEY, AUTH_WORKSPACE, identity())
    assert result["workspace_id"] == FACTORY_WORKSPACE
    assert result["mindshub_identity"]["workspace_id"] == AUTH_WORKSPACE
    assert result["execution_ready"] is False and KEY not in json.dumps(result)
    doc = policy.document
    doc["credential_ref"]["revision"] = 2
    assert auth._receipt(InferencePolicy.from_document(doc), KEY, AUTH_WORKSPACE,
        identity())["identity_sha256"] != result["identity_sha256"]
    service = identity(key_type="service", principal_kind="service_account", credential_kind="service_account")
    assert auth._receipt(policy, KEY, AUTH_WORKSPACE, service)["mindshub_identity"]["key_type"] == "service"


@pytest.mark.parametrize("extra", [{"valid": 1}, {"auth_method": "jwt"}, {"key_type": "turn"},
    {"key_type": []}, {"principal_kind": "service_account"}, {"credential_kind": "device_key"},
    {"workspace_id": REFERENCE}, {"user_id": KEY}, {"organization_id": None}, {"key_prefix": KEY}])
def test_unverified_or_wrong_workspace_identity_is_never_ready(extra):
    with pytest.raises(InferencePolicyError) as error:
        auth._receipt(InferencePolicy.from_document(policy_document()), KEY, AUTH_WORKSPACE, identity(**extra))
    assert KEY not in str(error.value)


@pytest.mark.parametrize("key,workspace", [("", AUTH_WORKSPACE), (KEY, "invalid"),
    ("session.token.value", AUTH_WORKSPACE), (KEY + "\r\n", AUTH_WORKSPACE)])
def test_invalid_input_stops_before_network_or_sdk(key, workspace, monkeypatch):
    async def forbidden(*args):
        raise AssertionError("No network permitted")
    monkeypatch.setattr(auth, "_read_identity", forbidden)
    with pytest.raises(InferencePolicyError):
        asyncio.run(auth.check_credential(InferencePolicy.from_document(policy_document()), key, workspace))


def test_public_explicit_credential_file_is_not_loaded_into_global_environment(validation_checkout, monkeypatch, capsys):
    root, target = validation_checkout
    monkeypatch.delenv("AI_FACTORY_MINDSHUB_API_KEY", raising=False)
    env = root / "controller.env"
    env.write_text("AI_FACTORY_MINDSHUB_API_KEY=" + KEY + "\n")
    async def checked(policy, key, workspace):
        assert key == KEY and workspace == AUTH_WORKSPACE
        return auth._receipt(policy, key, workspace, identity())
    monkeypatch.setattr(auth, "check_credential", checked)
    args = ["product-inference-check", "--profile", str(profile_file(root)), "--target-root", str(target),
        "--auth-workspace-id", AUTH_WORKSPACE, "--env-file"]
    assert main(args + [str(env)]) == 0
    assert json.loads(capsys.readouterr().out)["execution_ready"] is False
    import os
    assert "AI_FACTORY_MINDSHUB_API_KEY" not in os.environ
    assert main(args + [str(target / ".env")]) == 1
    assert "outside candidate" in capsys.readouterr().err
    env.write_text(KEY + "=invalid\n")
    assert main(args + [str(env)]) == 1
    error = capsys.readouterr().err
    assert "Could not read" in error and KEY not in error


@pytest.fixture
def pinned_http():
    http = pytest.importorskip("httpx2")
    assert inspect.signature(http.AsyncHTTPTransport).parameters["retries"].default == 0
    from auto_mindsdb_factory.anton_session import validate_anton_installation
    from auto_mindsdb_factory.product_agent import AgentSessionError
    try:
        validate_anton_installation()
    except AgentSessionError:
        pytest.skip("Requires the genuine pinned controller SDK")
    return http


@pytest.mark.parametrize("case", [200, 302, 401, 403, 429, 503, "malformed", "duplicate", "oversized"])
def test_pinned_readonly_auth_transport_closes_and_never_retries(pinned_http, monkeypatch, case):
    http = pinned_http
    clients, calls = [], []
    raw = json.dumps(identity()).encode()
    raw = {"malformed": b"private error", "duplicate": b'{"valid":true,"valid":false}',
           "oversized": b"x" * 65537}.get(case, raw)
    async def respond(request):
        calls.append(request)
        assert str(request.url) == "https://auth.mindshub.ai/v1/authenticate/"
        assert request.method == "GET" and request.headers["Authorization"] == "Bearer " + KEY
        return http.Response(case if type(case) is int else 200, content=raw,
            headers={"Location": "https://other.invalid/never-follow"})
    original = http.AsyncClient
    def factory(**options):
        assert options["follow_redirects"] is False
        clients.append(original(transport=http.MockTransport(respond), **options))
        return clients[-1]
    monkeypatch.setattr(http, "AsyncClient", factory)
    action = auth.check_credential(InferencePolicy.from_document(policy_document()), KEY, AUTH_WORKSPACE)
    if case == 200:
        assert asyncio.run(action)["status"] == "credential_identity_verified"
    else:
        with pytest.raises(InferencePolicyError) as error:
            asyncio.run(action)
        assert KEY not in str(error.value) and "private error" not in str(error.value)
    assert len(calls) == len(clients) == 1 and clients[0].is_closed


@pytest.mark.parametrize("cancel", [False, True])
def test_pinned_auth_deadline_or_cancel_drains_and_closes(pinned_http, monkeypatch, cancel):
    http = pinned_http
    clients, stopped = [], []
    entered = asyncio.Event()
    async def respond(request):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            stopped.append(True)
    original = http.AsyncClient
    def factory(**options):
        clients.append(original(transport=http.MockTransport(respond), **options))
        return clients[-1]
    monkeypatch.setattr(http, "AsyncClient", factory)
    monkeypatch.setattr(auth, "AUTH_TIMEOUT_SECONDS", 5.0 if cancel else 0.02)
    async def exercise():
        task = asyncio.create_task(auth.check_credential(InferencePolicy.from_document(policy_document()), KEY, AUTH_WORKSPACE))
        await entered.wait()
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else InferencePolicyError):
            await task
    asyncio.run(exercise())
    assert stopped == [True] and len(clients) == 1 and clients[0].is_closed


@pytest.mark.parametrize("via_path", [False, True])
@pytest.mark.parametrize("in_file", [False, True])
def test_candidate_http_client_cannot_receive_credential(pinned_http, validation_checkout, via_path, in_file):
    root, target = validation_checkout
    captured = root / "captured.txt"
    (target / "httpx2.py").write_text(f'''from pathlib import Path
class AsyncClient:
    def __init__(self, **options): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass
    def stream(self, *args, headers, **options):
        Path({str(captured)!r}).write_text(headers['Authorization'])
        raise RuntimeError('Untrusted HTTP client received the credential')
''')
    source = Path(auth.__file__).resolve().parents[2]
    env = {"PATH": os.environ["PATH"], "HOME": str(root), "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(source / "src") + (os.pathsep + str(target) if via_path else ""),
        "HTTPS_PROXY": "http://127.0.0.1:1", "NO_PROXY": ""}
    args = [sys.executable, "-m", "auto_mindsdb_factory", "product-inference-check",
        "--profile", str(profile_file(root)), "--target-root", str(target), "--auth-workspace-id", AUTH_WORKSPACE]
    if in_file:
        credential = root / "credential.env"
        credential.write_text("AI_FACTORY_MINDSHUB_API_KEY=" + KEY + "\n")
        args += ["--env-file", str(credential)]
    else:
        env["AI_FACTORY_MINDSHUB_API_KEY"] = KEY
    result = subprocess.run(args, cwd=source if via_path else target, env=env,
        capture_output=True, text=True, timeout=15)
    assert result.returncode == 1 and "Product workflow failed:" in result.stderr
    assert not captured.exists(), "Candidate HTTP client received the synthetic credential"


def test_unrelated_import_path_stops_before_readiness(monkeypatch, tmp_path):
    paths = [*sys.path, str(tmp_path)]
    monkeypatch.setattr(sys, "path", paths)
    with pytest.raises(InferencePolicyError, match="trusted controller imports"):
        asyncio.run(auth.check_credential(InferencePolicy.from_document(policy_document()), KEY, AUTH_WORKSPACE))
    assert sys.path == paths


@pytest.mark.parametrize("cached", [False, True])
def test_sdk_client_origin_is_checked_before_use(pinned_http, monkeypatch, tmp_path, cached):
    from importlib.machinery import ModuleSpec
    from types import SimpleNamespace
    bad = str(tmp_path / "httpx2.py")
    paths = list(sys.path)
    def forbidden(*args):
        raise AssertionError("Untrusted origin must not be imported")
    monkeypatch.setattr(auth, "import_module", (lambda name: SimpleNamespace(__file__=bad)) if cached else forbidden)
    if not cached:
        monkeypatch.setattr(auth, "find_spec", lambda name: ModuleSpec(name, None, origin=bad))
    with pytest.raises(InferencePolicyError, match="trusted controller imports"):
        auth._trusted_http_client()
    assert sys.path == paths


def test_candidate_transitive_http_dependency_is_not_imported(pinned_http, validation_checkout):
    root, target = validation_checkout
    captured = root / "imported.txt"
    (target / "httpcore2.py").write_text(
        f"from pathlib import Path\nPath({str(captured)!r}).write_text('untrusted dependency executed')\nraise RuntimeError('shadow')\n")
    source = Path(auth.__file__).resolve().parents[2]
    env = {"PATH": os.environ["PATH"], "HOME": str(root), "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(source / "src") + os.pathsep + str(target), "AI_FACTORY_MINDSHUB_API_KEY": KEY,
        "HTTPS_PROXY": "http://127.0.0.1:1", "NO_PROXY": ""}
    result = subprocess.run([sys.executable, "-m", "auto_mindsdb_factory", "product-inference-check",
        "--profile", str(profile_file(root)), "--target-root", str(target), "--auth-workspace-id", AUTH_WORKSPACE],
        cwd=source, env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 1 and "trusted controller imports" in result.stderr
    assert not captured.exists()
