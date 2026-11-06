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


@pytest.mark.parametrize("transport", ["controller", "operator-handoff"])
def test_direct_cli_preloaded_base_and_publication_transport_are_explicit(validation_checkout, monkeypatch, transport):
    from auto_mindsdb_factory import product_commands
    from auto_mindsdb_factory.__main__ import main
    from auto_mindsdb_factory.github_handoff_channel import GitHubHandoff
    root, target = validation_checkout
    profile = root / "profile.json"
    profile.write_text(json.dumps({"version": 1, "name": "direct", "repository": "owner/product",
        "image": "node@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]]}))
    task = root / "task.json"
    task.write_text(json.dumps({"id": "direct", "title": "Fix", "goal": "fix",
        "acceptance_criteria": [{"id": "a", "description": "works", "command": ["true"]}]}))
    calls = []
    def promotion(*args, **kwargs):
        assert (isinstance(kwargs["transport"], GitHubHandoff)) == (transport == "operator-handoff")
        assert kwargs["before_merge"].__self__ is calls[0]
        return lambda *args: {"status": "complete"}
    def run(delivery, supplied_task, **kwargs):
        calls.append(delivery)
        assert kwargs["base_sha"] == "b" * 40
        assert delivery.publication_transport == ("operator-handoff-v1" if transport == "operator-handoff" else "controller-github-v1")
        return kwargs["promote"]({}, None) if kwargs["promote"] else {"status": "verified"}
    monkeypatch.setattr(RailwayDelivery, "run", run)
    monkeypatch.setattr(product_commands, "GitHubPromotion", promotion)
    monkeypatch.setattr(product_commands, "LinearProductClient", lambda *args: pytest.fail("Linear accessed"))
    arguments = ["product-deliver", "--profile", str(profile), "--target-root", str(target),
        "--store-dir", str(root / "store"), "--runner", "railway", "--task", str(task),
        "--base-sha", "b" * 40, "--github-transport", transport, "--publish"]
    assert main(arguments) == 0 and len(calls) == 1
    assert main(arguments[:-1]) == (1 if transport == "operator-handoff" else 0)


def test_handoff_public_cli_checks_scope_before_network(tmp_path, monkeypatch):
    from auto_mindsdb_factory.__main__ import main
    from auto_mindsdb_factory import github_handoff_executor
    monkeypatch.setattr(github_handoff_executor, "execute_request", lambda *a, **k: pytest.fail("Invalid scope reached executor"))
    assert main(["product-github-handoff", "--request", str(tmp_path / "request.json"),
        "--receipts-dir", str(tmp_path / "receipts"), "--target-root", str(tmp_path / "target"),
        "--repository", "owner/repo", "--run-id", "../bad", "--base-sha", "a" * 40,
        "--candidate-sha", "b" * 40]) == 1
