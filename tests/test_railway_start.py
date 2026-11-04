import importlib.util
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest


def supervisor():
    path = Path(__file__).parents[1] / "deploy/railway/start.py"
    if not path.exists():
        pytest.skip("Deployment supervision is not present in this source export")
    spec = importlib.util.spec_from_file_location("railway_start", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_first_boot_pauses_without_credentials_and_preserves_state(tmp_path, monkeypatch):
    module = supervisor()
    for name, path in {"DATA": tmp_path, "CONFIG": tmp_path / "config", "TARGET": tmp_path / "target", "STORE": tmp_path / "evidence"}.items():
        monkeypatch.setattr(module, name, path)
    monkeypatch.setattr(Path, "is_mount", lambda self: self == tmp_path)
    monkeypatch.setattr(module.os, "getuid", lambda: 10001)
    monkeypatch.setattr(module, "time", SimpleNamespace(time=time.time,
        sleep=lambda _: setattr(module, "stopping", True)))
    monkeypatch.setattr(module, "signal", SimpleNamespace(SIGTERM=15, SIGINT=2, signal=lambda *_: None))
    module.STORE.mkdir()
    (module.STORE / "retained.json").write_text("immutable")
    module.main()
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["mode"] == "paused"
    assert len(status["profile_sha"]) == len(status["route_sha"]) == 64
    assert (module.STORE / "retained.json").read_text() == "immutable"
    assert (module.TARGET / "HEAD").exists()


def test_ephemeral_controller_storage_is_rejected(monkeypatch):
    module = supervisor()
    monkeypatch.setattr(Path, "is_mount", lambda _: False)
    with pytest.raises(RuntimeError, match="persistent volume"):
        module.main()
