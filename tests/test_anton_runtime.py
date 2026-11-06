import json
import re
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory import anton_session
from auto_mindsdb_factory.product_agent import AgentSessionError


def test_dependency_manifest_preserves_every_overlapping_factory_pin():
    root = Path(__file__).resolve().parents[1]
    text = (root / "src/auto_mindsdb_factory/anton_requirements.txt").read_text()
    pins = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([a-z0-9-]+)==([^ ]+) ((?:--hash=sha256:[0-9a-f]{64} ?)+)", line)
        assert match, line
        name, version, hashes = match.groups()
        assert name not in pins
        pins[name] = version
    factory = tomllib.loads((root / "uv.lock").read_text())
    assert pins and all(pins[item["name"]] == item["version"] for item in factory["package"] if item["name"] in pins)


def test_runtime_drift_is_rejected_without_loading_sdk(monkeypatch):
    metadata = json.dumps({"url": "https://github.com/mindsdb/anton",
                          "vcs_info": {"commit_id": anton_session.ANTON_COMMIT}})
    monkeypatch.setattr(anton_session, "distribution", lambda name:
        SimpleNamespace(read_text=lambda path: metadata, version="unreviewed"))
    with pytest.raises(AgentSessionError, match="pinned"):
        anton_session.validate_anton_installation()
