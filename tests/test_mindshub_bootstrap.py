"""Keyless bootstrap ordering; every privileged operation is replaced by a double."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory import mindshub_bootstrap as bootstrap


@pytest.fixture
def admission(monkeypatch):
    events, state = [], {"uid": 0, "gid": 0, "groups": [0]}
    argv = ["product-deliver", "--profile", "/profile.json", "--task", "/task.json",
            "--target-root", "/target", "--publish", "--runner", "railway"]
    document = {"version": 1, "argv_sha256": bootstrap.argv_digest(argv), "profile_sha256": "a" * 64,
        "target_root": "/target", "receipt_path": "/receipt.json", "receipt_sha256": "b" * 64,
        "credential_file": "/private/key.env", "credential_version": [1, 2, 3, 4, 0, 384],
        "reasoning_effort": "high", "timeout_seconds": 180, "uid": 10001, "gid": 10001}
    raw = json.dumps(document).encode()
    context = object()
    for name in ("getuid", "geteuid"):
        monkeypatch.setattr(bootstrap.os, name, lambda: state["uid"])
    for name in ("getgid", "getegid"):
        monkeypatch.setattr(bootstrap.os, name, lambda: state["gid"])
    monkeypatch.setattr(bootstrap.os, "getgroups", lambda: state["groups"])
    for name, field in (("getresuid", "uid"), ("getresgid", "gid")):
        monkeypatch.setattr(bootstrap.os, name, lambda field=field: (state[field],) * 3, raising=False)
    def change(field, value):
        events.append(field); state[field] = value
    monkeypatch.setattr(bootstrap.os, "setgroups", lambda value: change("groups", value))
    monkeypatch.setattr(bootstrap.os, "setgid", lambda value: change("gid", value))
    monkeypatch.setattr(bootstrap.os, "setuid", lambda value: change("uid", value))
    monkeypatch.setattr(bootstrap, "require_controller_import_path", lambda target: events.append("imports"))
    profile = SimpleNamespace(digest="a" * 64, document={"inference": "synthetic-policy"})
    monkeypatch.setattr(bootstrap.ProductProfile, "load", lambda *args, **kwargs: (events.append("profile"), profile)[1])
    monkeypatch.setattr(bootstrap, "_read_owned", lambda *args: raw)
    def resolve(settings, **kwargs):
        assert state["uid"] == 0 and kwargs["credential_file"] == "/private/key.env"
        events.append("resolve-once")
        return context
    monkeypatch.setattr(bootstrap, "resolve_execution_context", resolve)
    def dispatch(supplied, *, inference_context):
        assert supplied == argv and inference_context is context and state["uid"] == state["gid"] == 10001
        assert state["groups"] == []
        events.append("normal-main")
        return 0
    monkeypatch.setattr(bootstrap, "product_main", dispatch)
    return SimpleNamespace(argv=argv, document=document, raw=raw, events=events, state=state,
        options={"authorization": "/authorization.json", "authorization_sha256": hashlib.sha256(raw).hexdigest()})


def test_bootstrap_validates_then_resolves_once_drops_and_uses_normal_main(admission):
    assert bootstrap.bootstrap(admission.argv, **admission.options) == 0
    assert admission.events == ["imports", "profile", "resolve-once", "groups", "gid", "uid", "imports", "normal-main"]


@pytest.mark.parametrize("change", ["command", "argv", "profile", "target", "digest", "extra", "uid-bool", "uid-root", "version-bool", "version-item", "symlink-path", "effort", "uid-overflow", "relative-target"])
def test_unapproved_metadata_stops_before_secret_resolution(admission, monkeypatch, change):
    document = admission.document.copy()
    if change == "command": admission.argv[0] = "product-replay"
    elif change == "argv": admission.argv.append("--publish")
    elif change == "relative-target": admission.argv[6] = "target"
    elif change == "profile": document["profile_sha256"] = "c" * 64
    elif change == "target": document["target_root"] = "/other"
    elif change == "digest": admission.options["authorization_sha256"] = "c" * 64
    elif change == "extra": document["api_key"] = "forbidden-field"
    elif change == "uid-bool": document["uid"] = True
    elif change == "uid-root": document["uid"] = 0
    elif change == "uid-overflow": document["uid"] = 2**32
    elif change == "version-bool": document["version"] = True
    elif change == "version-item": document["credential_version"] = [True] * 6
    elif change == "symlink-path": document["credential_file"] = "/tmp/../private/key.env"
    elif change == "effort": document["reasoning_effort"] = "unapproved"
    if change != "digest":
        raw = json.dumps(document).encode()
        monkeypatch.setattr(bootstrap, "_read_owned", lambda *args: raw)
        admission.options["authorization_sha256"] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError): bootstrap.bootstrap(admission.argv, **admission.options)
    assert "resolve-once" not in admission.events and "normal-main" not in admission.events


def test_failed_permanent_drop_never_enters_normal_main(admission, monkeypatch):
    monkeypatch.setattr(bootstrap.os, "setuid", lambda value: None)
    with pytest.raises(ValueError): bootstrap.bootstrap(admission.argv, **admission.options)
    assert admission.events.count("resolve-once") == 1 and "normal-main" not in admission.events


def test_cli_error_never_prints_private_exception(monkeypatch, capsys):
    def refused(*args, **kwargs): raise ValueError("private credential or provider detail")
    monkeypatch.setattr(bootstrap, "bootstrap", refused)
    assert bootstrap.main(["--authorization", "/authorization.json", "--authorization-sha256", "a" * 64,
                           "--", "product-deliver"]) == 1
    assert "private credential" not in str(capsys.readouterr())


@pytest.mark.parametrize("change", ["nonroot", "imports", "duplicate-json", "nonfinite-json"])
def test_bootstrap_untrusted_process_or_document_never_resolves(admission, monkeypatch, change):
    if change == "nonroot": admission.state["uid"] = 10001
    elif change == "imports":
        def denied(target): raise ValueError("untrusted imports")
        monkeypatch.setattr(bootstrap, "require_controller_import_path", denied)
    else:
        raw = b'{"version":1,"version":1}' if change == "duplicate-json" else b'{"version":NaN}'
        monkeypatch.setattr(bootstrap, "_read_owned", lambda *args: raw)
        admission.options["authorization_sha256"] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError): bootstrap.bootstrap(admission.argv, **admission.options)
    assert "resolve-once" not in admission.events and "normal-main" not in admission.events



def test_prepare_context_is_same_drop_boundary_without_dispatch(admission):
    context, admitted = bootstrap.prepare_context(admission.argv, **admission.options)
    assert type(admitted) is tuple and list(admitted) == admission.argv
    assert context is not None and admission.state["uid"] == admission.state["gid"] == 10001
    assert admission.events.count("resolve-once") == 1 and "normal-main" not in admission.events
