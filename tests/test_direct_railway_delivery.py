import json

import pytest

from auto_mindsdb_factory.product_delivery import ProductDelivery
from auto_mindsdb_factory.railway_runner import RailwayDelivery


@pytest.mark.parametrize("entry", ["run", "plan", "verify"])
@pytest.mark.parametrize("fail", [False, True])
def test_each_public_entry_owns_cleanup_and_nested_phases_share_pool(monkeypatch, entry, fail):
    from auto_mindsdb_factory import railway_runner
    pools, calls = [], []
    class Pool:
        def __init__(self):
            self.closed = False
            pools.append(self)
        def close(self):
            self.closed = True
    monkeypatch.setattr(railway_runner, "RailwayPool", Pool)
    def phase(delivery, *args):
        calls.append(delivery.sandbox_factory)
        if fail:
            raise RuntimeError("phase failed")
        return "checked"
    def run(delivery, *args):
        delivery.plan()
        return delivery.verify()
    monkeypatch.setattr(ProductDelivery, "plan", phase)
    monkeypatch.setattr(ProductDelivery, "verify", phase)
    monkeypatch.setattr(ProductDelivery, "run", run)
    delivery = object.__new__(RailwayDelivery)
    previous = delivery.sandbox_factory = object()
    if fail:
        with pytest.raises(RuntimeError):
            getattr(delivery, entry)()
    else:
        assert getattr(delivery, entry)() == "checked"
    assert len(pools) == 1 and pools[0].closed and set(calls) == {pools[0]}
    assert delivery.sandbox_factory is previous and delivery._railway_pool is None


@pytest.mark.parametrize("mode", ["product-deliver", "product-replay"])
@pytest.mark.parametrize("runner", ["docker", "railway"])
def test_direct_cli_runner_selection_does_not_use_linear(validation_checkout, monkeypatch, capsys, mode, runner):
    from auto_mindsdb_factory import product_commands
    from auto_mindsdb_factory.__main__ import main
    root, target = validation_checkout
    profile = root / "profile.json"
    profile.write_text(json.dumps({"version": 1, "name": "direct", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]]}))
    task = root / "task.json"
    task.write_text(json.dumps({"id": "direct", "title": "Fix", "goal": "fix",
        "acceptance_criteria": [{"id": "a", "description": "works", "command": ["true"]}]}))
    selected = []
    def run(delivery, task, **kwargs):
        selected.append(type(delivery))
        return {"status": "verified"}
    def replay(*args, **kwargs):
        selected.append(kwargs["delivery_factory"])
        return {"total": 1, "correct": 1}
    monkeypatch.setattr(ProductDelivery, "run", run)
    monkeypatch.setattr(RailwayDelivery, "run", run)
    monkeypatch.setattr(product_commands, "run_suite", replay)
    monkeypatch.setattr(product_commands, "LinearProductClient", lambda *a: pytest.fail("Linear accessed"))
    extra = ["--task", str(task)] if mode == "product-deliver" else ["--suite", str(root / "suite.json")]
    assert main([mode, "--profile", str(profile), "--target-root", str(target),
                 "--store-dir", str(root / "store"), "--runner", runner, *extra]) == 0
    assert selected == [RailwayDelivery if runner == "railway" else ProductDelivery]
