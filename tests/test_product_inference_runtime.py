from dataclasses import FrozenInstanceError, asdict, replace
import hashlib
import json
import os

import pytest

from auto_mindsdb_factory import product_inference_runtime as runtime
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_execution import codex_execution_snapshot, role_config
from auto_mindsdb_factory.product_inference import InferencePolicy, InferencePolicyError
from auto_mindsdb_factory.product_inference_auth import _receipt


KEY = "mdb_synthetic.private-only"
UUID = "00000000-0000-4000-8000-000000000001"


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "require_controller_import_path", lambda target: None)
    root = tmp_path.resolve()
    root.chmod(0o700)
    settings = {"builder_model": "gpt-6.1-terra", "reviewer_model": "pinned-reviewer",
        "agent_max_output_tokens": 10000, "inference": {"version": 1, "scope": "controller",
        "provider": "mindshub", "protocol": "responses", "endpoint": runtime.MindsHubQualificationConfig.base_url,
        "credential_ref": {"id": UUID, "revision": 1}}}
    key = root / "key.env"
    key.write_text("AI_FACTORY_MINDSHUB_API_KEY=" + KEY + "\n")
    key.chmod(0o600)
    receipt = _receipt(InferencePolicy.from_document(settings["inference"]), KEY, None,
        {"valid": True, "auth_method": "api_key", "key_type": "user", "principal_kind": "user",
        "credential_kind": "api_key", "user_id": UUID, "organization_id": UUID,
        "workspace_id": UUID, "key_prefix": "mdb_synthetic"})
    path = root / "receipt.json"
    path.write_text(json.dumps(receipt))
    return settings, dict(reasoning_effort="high", timeout_seconds=180, receipt_path=path,
        receipt_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), credential_file=key,
        credential_version=runtime._version(key.stat()), target_root=root / "candidate")


def test_resolved_config_is_frozen_secret_safe_and_ignores_global_provider(inputs, monkeypatch):
    settings, kwargs = inputs
    monkeypatch.setenv("OPENAI_API_KEY", "wrong-global-key")
    monkeypatch.setenv("AI_FACTORY_OPENAI_BASE_URL", "https://wrong.invalid/responses")
    monkeypatch.setenv("AI_FACTORY_OPENAI_FALLBACK_MODEL", "unapproved")
    original_hash = runtime.hashlib.sha256
    def safe_hash(data):
        assert KEY.encode() not in data
        return original_hash(data)
    monkeypatch.setattr(runtime.hashlib, "sha256", safe_hash)
    config = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    assert config.api_key == KEY and config.fallback_model is None
    assert (config.model, config.reasoning_effort, config.max_output_tokens, config.timeout_seconds) == (
        "gpt-6.1-terra", "high", 10000, 180)
    with pytest.raises(InferencePolicyError):
        replace(config)  # Copies cannot manufacture a new resolver-issued capability.
    assert KEY not in repr(config) + json.dumps(config.inference_binding) + json.dumps(codex_execution_snapshot(config))
    with pytest.raises(FrozenInstanceError):
        config.api_key = "changed"
    with pytest.raises(TypeError):
        replace(config, api_key="changed")
    with pytest.raises(InferencePolicyError, match="not execution-ready"):
        role_config(settings, "builder")
    assert runtime.resolve_qualification_config(settings, "verifier", **kwargs).model == "pinned-reviewer"


@pytest.mark.parametrize("change", ["policy", "digest", "version", "workspace", "candidate", "key-mode",
    "parent-mode", "symlink-key", "symlink-receipt", "fifo", "key-format", "oversized", "receipt-extra"])
def test_untrusted_or_stale_input_cannot_resolve(inputs, change):
    settings, kwargs = inputs
    key, receipt = kwargs["credential_file"], kwargs["receipt_path"]
    if change == "policy": settings["inference"]["credential_ref"]["revision"] = 2
    elif change == "digest": kwargs["receipt_sha256"] = "a" * 64
    elif change == "version": kwargs["credential_version"] = (0,) * 6
    elif change == "workspace": kwargs["auth_workspace_id"] = "00000000-0000-4000-8000-000000000002"
    elif change == "candidate": kwargs["target_root"] = key.parent
    elif change == "key-mode": key.chmod(0o644)
    elif change == "parent-mode": key.parent.chmod(0o755)
    elif change.startswith("symlink"):
        path = key if change == "symlink-key" else receipt
        path.rename(path.with_suffix(".original"))
        path.symlink_to(path.with_suffix(".original"))
    elif change == "fifo":
        key.unlink()
        os.mkfifo(key)
    elif change in ("key-format", "oversized"):
        key.write_text(KEY if change == "key-format" else "x" * 5000)
        kwargs["credential_version"] = runtime._version(key.stat())
    elif change == "receipt-extra":
        data = json.loads(receipt.read_text()); data["unknown"] = True
        receipt.write_text(json.dumps(data))
        kwargs["receipt_sha256"] = hashlib.sha256(receipt.read_bytes()).hexdigest()
    with pytest.raises(InferencePolicyError) as error:
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
    assert KEY not in str(error.value)


def test_import_guard_and_invalid_settings_precede_file_read(inputs, monkeypatch):
    settings, kwargs = inputs
    def refused(*args, **options): raise AssertionError("must not read any file")
    monkeypatch.setattr(runtime, "_read_owned", refused)
    def import_guard(target): raise InferencePolicyError("untrusted imports")
    monkeypatch.setattr(runtime, "require_controller_import_path", import_guard)
    with pytest.raises(InferencePolicyError, match="untrusted imports"):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
    monkeypatch.setattr(runtime, "require_controller_import_path", lambda target: None)
    kwargs["timeout_seconds"] = True
    with pytest.raises(InferencePolicyError):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)


def test_legacy_provider_fingerprint_is_unchanged():
    config = OpenAIResponsesAgentConfig(api_key="synthetic-only")
    provider = {k: asdict(config)[k] for k in ("base_url", "organization", "project")}
    assert codex_execution_snapshot(config)["provider_sha256"] == hashlib.sha256(
        json.dumps(provider, sort_keys=True).encode()).hexdigest()


def test_key_rotation_requires_new_frozen_execution_receipt(inputs, tmp_path):
    from auto_mindsdb_factory.product_execution import require_codex_execution
    from auto_mindsdb_factory.product_agent import HarnessStopped
    from auto_mindsdb_factory.run_journal import RunJournal
    settings, kwargs = inputs
    config = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    journal = RunJournal(tmp_path, "bound")
    journal.create({"execution_snapshots": {"builder": codex_execution_snapshot(config)}})
    key = kwargs["credential_file"]
    key.write_text("AI_FACTORY_MINDSHUB_API_KEY=mdb_synthetic.rotated\n")
    with pytest.raises(InferencePolicyError):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
    kwargs["credential_version"] = runtime._version(key.stat())
    changed = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    with pytest.raises(HarnessStopped, match="frozen Codex"):
        require_codex_execution(changed, "builder", journal)
    assert not journal.events()


@pytest.mark.parametrize("change", ["write", "short-read"])
def test_credential_change_during_read_is_rejected(inputs, monkeypatch, change):
    settings, kwargs = inputs
    read = runtime.os.read
    def raced(descriptor, count):
        data = read(descriptor, count)
        if data.startswith(b"AI_FACTORY_MINDSHUB_API_KEY="):
            if change == "short-read":
                return data[:-1]
            kwargs["credential_file"].write_text("AI_FACTORY_MINDSHUB_API_KEY=mdb_synthetic.changed\n")
        return data
    monkeypatch.setattr(runtime.os, "read", raced)
    with pytest.raises(InferencePolicyError):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)


def test_context_resolves_once_and_derives_roles_without_file_access(inputs, monkeypatch, tmp_path):
    from auto_mindsdb_factory.product_execution import execution_manifest, execution_config
    from auto_mindsdb_factory.run_journal import RunJournal
    settings, kwargs = inputs
    reads = []
    original = runtime._read_owned
    def counted(*args, **options):
        reads.append(options.get("credential", False))
        return original(*args, **options)
    monkeypatch.setattr(runtime, "_read_owned", counted)
    context = runtime.resolve_execution_context(settings, **kwargs)
    assert reads.count(True) == 1
    def denied(*args, **options): raise AssertionError("No file reads after privilege drop")
    monkeypatch.setattr(runtime, "_read_owned", denied)
    monkeypatch.setenv("OPENAI_API_KEY", "wrong-global")
    monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "low")
    monkeypatch.setenv("AI_FACTORY_OPENAI_TIMEOUT_SECONDS", "1")
    manifest = execution_manifest(settings, inference_context=context)
    journal = RunJournal(tmp_path, "context")
    journal.create(manifest)
    for role in ("planner", "builder", "verifier"):
        config = execution_config(settings, role, journal, inference_context=context)
        assert config.api_key == KEY and config.reasoning_effort == "high" and config.timeout_seconds == 180
        assert config.model == settings["reviewer_model" if role == "verifier" else "builder_model"]
    assert KEY not in repr(context) + json.dumps(context.inspect()) + json.dumps(manifest)
    with pytest.raises(FrozenInstanceError): context._settings = "changed"
    with pytest.raises(InferencePolicyError, match="not execution-ready"):
        role_config(settings, "builder")


@pytest.mark.parametrize("change", ["policy", "builder", "reviewer", "output", "scope", "role", "absent"])
def test_context_rejects_settings_or_authority_drift(inputs, change):
    settings, kwargs = inputs
    context = runtime.resolve_execution_context(settings, **kwargs)
    role = "builder"
    if change == "policy": settings["inference"]["credential_ref"]["revision"] = 2
    elif change == "builder": settings["builder_model"] = "other"
    elif change == "reviewer": settings["reviewer_model"] = "other"
    elif change == "output": settings["agent_max_output_tokens"] += 1
    elif change == "scope": settings["allowed_paths"] = ["**"]
    elif change == "role": role = "other"
    elif change == "absent": del settings["inference"]
    with pytest.raises(InferencePolicyError, match="execution context"):
        role_config(settings, role, inference_context=context)


def test_context_rotation_or_limits_cannot_reuse_frozen_execution(inputs, tmp_path):
    from auto_mindsdb_factory.product_execution import execution_manifest, execution_config
    from auto_mindsdb_factory.product_agent import HarnessStopped
    from auto_mindsdb_factory.run_journal import RunJournal
    settings, kwargs = inputs
    first = runtime.resolve_execution_context(settings, **kwargs)
    journal = RunJournal(tmp_path, "context-drift")
    journal.create(execution_manifest(settings, inference_context=first))
    for override in ({"reasoning_effort": "low"}, {"timeout_seconds": 1}):
        changed = runtime.resolve_execution_context(settings, **{**kwargs, **override})
        assert changed.inspect()["contract_sha256"] != first.inspect()["contract_sha256"]
        with pytest.raises(HarnessStopped, match="frozen execution"):
            execution_config(settings, "builder", journal, inference_context=changed)
    key = kwargs["credential_file"]
    key.write_text("AI_FACTORY_MINDSHUB_API_KEY=mdb_synthetic.rotated\n")
    kwargs["credential_version"] = runtime._version(key.stat())
    changed = runtime.resolve_execution_context(settings, **kwargs)
    with pytest.raises(HarnessStopped, match="frozen execution"):
        execution_config(settings, "builder", journal, inference_context=changed)
    assert execution_config(settings, "builder", journal, inference_context=first).api_key == KEY
    assert not journal.events()


@pytest.mark.parametrize("model", [None, 1, True, ""])
def test_context_rejects_invalid_reviewer_before_credential_read(inputs, monkeypatch, model):
    settings, kwargs = inputs
    settings["reviewer_model"] = model
    def denied(*args, **options): raise AssertionError("Invalid role must not resolve credential")
    monkeypatch.setattr(runtime, "_read_owned", denied)
    with pytest.raises(InferencePolicyError, match="role models"):
        runtime.resolve_execution_context(settings, **kwargs)


def test_plain_profile_object_cannot_supply_trusted_context(inputs):
    from auto_mindsdb_factory.product_agent import HarnessStopped
    settings, _ = inputs
    with pytest.raises(HarnessStopped, match="trusted inference context"):
        role_config(settings, "builder", inference_context={"api_key": KEY})


def test_unresolved_public_construction_cannot_authorize_execution(inputs):
    settings, _ = inputs
    with pytest.raises(InferencePolicyError):
        config = runtime.MindsHubQualificationConfig((KEY, json.dumps({"policy": settings["inference"]})),
            settings["builder_model"], "high", settings["agent_max_output_tokens"], 180)
        context = runtime.MindsHubExecutionContext(runtime._canonical_settings(settings), config)
        role_config(settings, "builder", inference_context=context)


def test_replaced_or_identity_forged_context_cannot_authorize_execution(inputs):
    settings, kwargs = inputs
    context = runtime.resolve_execution_context(settings, **kwargs)
    with pytest.raises(InferencePolicyError): replace(context)
    import pickle
    with pytest.raises(TypeError): pickle.dumps(context)
    with pytest.raises(TypeError): pickle.dumps(context._config)
    forged = object.__new__(runtime.MindsHubExecutionContext)
    for key, value in vars(context).items(): object.__setattr__(forged, key, value)
    with pytest.raises(InferencePolicyError):
        role_config(settings, "builder", inference_context=forged)


@pytest.mark.parametrize("field", ["builder_harness", "reviewer_harness"])
def test_bound_codex_is_rejected_before_receipt_or_credential_resolution(inputs, monkeypatch, field):
    settings, kwargs = inputs
    settings.update({field: "codex", "execution_lane": "codex-qualification"})
    reads = []
    original = runtime._read_owned
    def counted(*args, **options):
        reads.append(options.get("credential", False))
        return original(*args, **options)
    monkeypatch.setattr(runtime, "_read_owned", counted)
    with pytest.raises(InferencePolicyError, match="Codex"):
        runtime.resolve_execution_context(settings, **kwargs)
    assert not reads


@pytest.mark.parametrize("mode", ["success", "drop", "attempts", "redirect", "error", "cancel", "timeout", "read-cancel", "cleanup", "cleanup-slow-start", "startup-timeout", "bad-usage"])
def test_native_mindshub_transport_uses_bound_request_and_closes(inputs, tmp_path, monkeypatch, mode):
    pytest.importorskip("anton.core.session")
    import sys
    import time
    from auto_mindsdb_factory import mindshub_native_process
    from auto_mindsdb_factory.mindshub_native import MindsHubNativeConnector
    from auto_mindsdb_factory.product_agent import ProductAgent, AgentSessionError
    from auto_mindsdb_factory.connectors import FactoryConnectorError
    from auto_mindsdb_factory.product_roles import snapshot_roles
    from auto_mindsdb_factory.run_journal import RunJournal
    script = r'''import asyncio,json
from pathlib import Path
from auto_mindsdb_factory import mindshub_native_worker as worker
http=worker._trusted_http_client()
actual=http.AsyncClient
report={"status":"passed","summary":"Checked","findings":[],"criteria":[{"id":"a","status":"passed","evidence":"Read source"}]}
class PendingBody(http.AsyncByteStream):
 async def __aiter__(self):
  yield b'{"id":'
  await asyncio.Event().wait()
async def respond(request):
 body=json.loads(request.content)
 assert request.headers["x-mindshub-allow-failover"]=="false"
 assert request.headers["authorization"].startswith("Bearer mdb_synthetic.")
 assert body["store"] is False and body["reasoning"]["effort"]=="high"
 assert {tool["name"] for tool in body["tools"]}=={"run","finish"}
 if MODE=="cancel": await asyncio.Event().wait()
 if MODE=="redirect": return http.Response(307,headers={"location":"https://other.invalid"})
 if MODE=="error": return http.Response(503,text="private provider error")
 headers={"x-mindshub-attempts":"2" if MODE=="attempts" else "1","x-mindshub-failover":"false"}
 if MODE=="drop": headers["x-mindshub-dropped-params"]="private dropped field"
 if MODE in {"read-cancel","timeout"}: return http.Response(200,headers=headers,stream=PendingBody())
 finish=any(item.get("type")=="function_call_output" for item in body["input"])
 name,args=("finish",report) if finish else ("run",{"command":["true"]})
 return http.Response(200,headers=headers,json={"model":body["model"],"status":"completed",
  "usage":{"input_tokens":7,"output_tokens":3,"total_tokens":11 if MODE=="bad-usage" else 10},
  "output":[{"type":"function_call","call_id":name,"name":name,"arguments":json.dumps(args)}]})
class Client:
 async def __aenter__(self):
  self.client=actual(transport=http.MockTransport(respond));return await self.client.__aenter__()
 async def __aexit__(self,*args):
  await self.client.__aexit__(*args)
  if MODE=="cleanup":
   Path(CLEANUP_MARKER).touch()
   while True:
    try:await asyncio.sleep(60)
    except asyncio.CancelledError:continue
http.AsyncClient=lambda **options:Client()
raise SystemExit(worker.main())
'''
    cleanup = mode in {"cleanup", "cleanup-slow-start"}
    cleanup_marker = tmp_path / "cleanup-entered"
    startup = "import time; time.sleep(.5)\n" if mode in {"cleanup-slow-start", "startup-timeout"} else ""
    child_mode = "cleanup" if cleanup or mode == "startup-timeout" else mode
    constants = "MODE=" + repr(child_mode) + "\nCLEANUP_MARKER=" + repr(str(cleanup_marker)) + "\n"
    monkeypatch.setattr(mindshub_native_process, "_command", lambda: [sys.executable, "-c", startup + constants + script])
    settings, kwargs = inputs
    config = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    journal = RunJournal(tmp_path, "native-http")
    journal.create({"role_snapshots": snapshot_roles()})
    commands, started = [], time.monotonic()
    bounded = mode in {"cancel", "timeout", "read-cancel", "startup-timeout"}
    def cancelled():
        # Exercise cleanup after observing a response, independently of SDK startup speed.
        if cleanup:
            return cleanup_marker.exists()
        return mode in {"cancel", "read-cancel"} and time.monotonic() - started > .3
    connector = MindsHubNativeConnector(config, deadline=started + (.4 if bounded else 3),
        cancelled=cancelled)
    agent = ProductAgent(connector, journal, max_turns=3, max_total_tokens=1000, deadline=started + 3)
    def command(argv):
        commands.append(argv)
        return {"exit_code": 0}
    if mode == "success":
        assert agent.run("builder", {"acceptance_criteria": [{"id": "a", "description": "Read source"}]}, command)["status"] == "passed"
        assert commands == [["true"]]
    else:
        with pytest.raises((AgentSessionError, FactoryConnectorError)): agent.run("builder", {}, command)
        assert not commands
    assert time.monotonic() - started < 3 and connector.worker_facts["reaped"]
    if cleanup:
        assert cleanup_marker.exists() and connector.worker_facts["packet_sent"] and connector.worker_facts["forced"]
    elif mode == "startup-timeout":
        assert not cleanup_marker.exists() and not connector.worker_facts["packet_sent"]
    evidence = json.dumps(journal.events())
    assert all(value not in evidence for value in (KEY, "private provider error", "private dropped field"))
    assert journal.summary()["total_tokens"] == (20 if mode == "success" else 10 if mode in {"drop", "attempts", "cleanup", "cleanup-slow-start"} else 0), connector.worker_facts
    assert journal.summary()["usage_complete"] is (mode in {"success", "drop"})


def test_native_mindshub_invalid_admission_never_opens_client(inputs, monkeypatch):
    from auto_mindsdb_factory import mindshub_native
    from auto_mindsdb_factory.product_agent import HarnessStopped
    settings, kwargs = inputs
    config = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    def denied(): raise AssertionError("Invalid admission must not open client")
    from auto_mindsdb_factory import mindshub_native_process
    monkeypatch.setattr(mindshub_native_process, "_command", denied)
    connector = mindshub_native.MindsHubNativeConnector(config)
    body = {"model": config.model, "reasoning": {"effort": config.reasoning_effort}}
    for timeout in (0, -1, True, float("inf"), float("nan")):
        with pytest.raises(HarnessStopped, match="deadline"):
            connector.request_response(body, timeout_seconds=timeout)
    with pytest.raises(HarnessStopped, match="configuration mismatch"):
        connector.request_response({**body, "model": "other"})


def test_normal_delivery_uses_one_context_for_planning_and_independent_anton_roles(inputs, tmp_path, monkeypatch):
    pytest.importorskip("anton.core.session")
    import time
    from auto_mindsdb_factory.product_profile import ProductProfile
    from auto_mindsdb_factory.product_delivery import ProductDelivery
    from auto_mindsdb_factory.anton_session import AntonAgent
    from auto_mindsdb_factory.mindshub_native import MindsHubNativeConnector
    from auto_mindsdb_factory.run_journal import RunJournal
    settings, kwargs = inputs
    profile = ProductProfile.from_document({"version": 1, "name": "bound", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]],
        "builder_harness": "anton", "reviewer_harness": "anton", **settings})
    context = runtime.resolve_execution_context(profile.document, **kwargs)
    monkeypatch.setattr(ProductProfile, "validate_repository", lambda *args: None)
    monkeypatch.setenv("OPENAI_API_KEY", "wrong-global")
    delivery = ProductDelivery(profile, tmp_path / "target", tmp_path / "store", inference_context=context)
    delivery.deadline = time.monotonic() + 60
    journal = RunJournal(tmp_path / "store", "bound-roles")
    journal.create(delivery._execution_manifest())
    builder, verifier = (delivery._agent(role, journal) for role in ("builder", "verifier"))
    assert isinstance(builder, AntonAgent) and isinstance(verifier, AntonAgent) and builder is not verifier
    assert builder.config.api_key == verifier.config.api_key == KEY
    assert builder.config.model == settings["builder_model"] and verifier.config.model == settings["reviewer_model"]
    planning = delivery._connector(delivery._execution_config("planner", journal), deadline=delivery.deadline - 20)
    assert isinstance(planning, MindsHubNativeConnector) and planning.config.api_key == KEY
    assert planning.deadline == delivery.deadline - 20 and planning.cancelled == delivery.cancelled
    assert KEY not in json.dumps(json.loads((journal.root / "manifest.json").read_text()))
