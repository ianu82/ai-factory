"""Agent interruption uses the existing job watcher, without duplicate aborts."""

import threading
import time

import pytest

from auto_mindsdb_factory.product_agent import HarnessStopped
from auto_mindsdb_factory.product_delivery import ProductDelivery


def delivery():
    owner = object.__new__(ProductDelivery)
    owner.deadline = time.monotonic() + 20
    owner.settings = {"command_timeout_seconds": 7}
    owner._cancellation = lambda: False
    return owner


@pytest.mark.parametrize("mode", ["agent", "operator"])
def test_active_command_has_one_abort_owner_and_is_drained(mode):
    owner = delivery()
    started, retired = threading.Event(), threading.Event()
    cancelled, aborts, failures = threading.Event(), [], []
    owner._cancellation = cancelled.is_set
    class Sandbox:
        def run(self, arguments):
            started.set()
            assert retired.wait(2)
            return {"exit_code": 0}
        def abort(self):
            aborts.append(True); retired.set()
    sandbox = Sandbox(); command = owner._agent_command(sandbox)
    def run():
        try: command(["blocked-job"])
        except HarnessStopped: failures.append(True)
    thread = threading.Thread(target=run); thread.start()
    try:
        assert started.wait(1)
        if mode == "agent": command.abort(); command.abort()
        else: cancelled.set()
        thread.join(timeout=2)
        assert not thread.is_alive() and failures == [True] and aborts == [True]
        assert sandbox.timeout == 7
        with pytest.raises(HarnessStopped): command(["must-not-restart"])
        assert aborts == [True]
    finally:
        retired.set(); thread.join(timeout=2)


def test_prestart_abort_and_normal_result_preserve_command_policy():
    owner = delivery(); calls = []
    class Sandbox:
        def run(self, arguments): calls.append(arguments); return {"exit_code": 1}
        def abort(self): raise AssertionError("No active job to stop")
    sandbox = Sandbox(); command = owner._agent_command(sandbox)
    command.abort()
    with pytest.raises(HarnessStopped): command(["not-started"])
    assert not calls
    fresh = owner._agent_command(sandbox)
    assert fresh(["check"]) == {"exit_code": 1} and calls == [["check"]] and sandbox.timeout == 7
