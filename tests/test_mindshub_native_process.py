"""Keyless real-child lifecycle tests; no network or candidate execution."""

import os
import sys
import time

import pytest

from auto_mindsdb_factory import mindshub_native_process as process


PROGRAM = '''import asyncio,json,os,sys,time
print(json.dumps({"kind":"ready"}),flush=True)
packet=json.load(sys.stdin)
if MODE == "request-stall": time.sleep(30)
summary={"model":"pinned","status":"completed","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}
print(json.dumps({"kind":"observed","summary":summary,"gateway":{"single_attempt":True}}),flush=True)
print(json.dumps({"kind":"payload","payload":summary}),flush=True)
if MODE == "cleanup-stall":
 async def stubborn():
  async with asyncio.timeout(.01):
   while True:
    try: await asyncio.sleep(30)
    except asyncio.CancelledError: continue
 asyncio.run(stubborn())
print(json.dumps({"kind":"retired"}),flush=True)
'''


@pytest.mark.parametrize("mode", ["success", "request-stall", "cleanup-stall"])
def test_owned_process_retirement_is_bounded_and_preserves_observed_usage(monkeypatch, mode):
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-c", "MODE=" + repr(mode) + "\n" + PROGRAM])
    start = time.monotonic()
    observed, payload, complete, facts = process.exchange(
        {"api_key": "synthetic-private-pipe-value", "body": {}, "deadline": start + .35},
        deadline=start + .35, cancelled=lambda: False)
    assert time.monotonic() - start < .7 and facts["reaped"] and facts["packet_sent"]
    assert complete is (mode == "success") and facts["forced"] is (mode != "success")
    assert (observed is None) is (mode == "request-stall")
    if observed is not None:
        assert observed["summary"]["usage"]["total_tokens"] == 10
    with pytest.raises(ChildProcessError): os.waitpid(facts["pid"], os.WNOHANG)
    assert "synthetic-private-pipe-value" not in str((observed, payload, facts))


def test_cancelled_process_cannot_complete_or_remain_live(monkeypatch):
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-c", "MODE='request-stall'\n" + PROGRAM])
    start = time.monotonic()
    _, _, complete, facts = process.exchange({"api_key": "synthetic", "body": {}, "deadline": start + 1},
        deadline=start + 1, cancelled=lambda: time.monotonic() - start > .15)
    assert not complete and facts["forced"] and facts["reaped"]
    assert time.monotonic() - start < .5


def test_expired_or_oversized_packet_never_spawns(monkeypatch):
    def denied(): raise AssertionError("must not spawn")
    monkeypatch.setattr(process, "_command", denied)
    for packet, deadline in (({}, time.monotonic() - 1), ({"data": "x" * process.MAX_BYTES}, time.monotonic() + 1)):
        with pytest.raises(RuntimeError, match="admission"):
            process.exchange(packet, deadline=deadline, cancelled=lambda: False)
