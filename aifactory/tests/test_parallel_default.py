import pytest

from .conftest import default_workers


@pytest.mark.parametrize(("overrides", "expected"), [
    pytest.param({"cpu_count": 2}, 4, id="two-cpus"),
    pytest.param({"cpu_count": 6}, 6, id="six-cpus"),
    pytest.param({"cpu_count": 32}, 8, id="worker-cap"),
    pytest.param({"cpu_count": None}, 4, id="unknown-cpus"),
    pytest.param({"shard": "1/3"}, 0, id="ci-shard"),
    pytest.param({"xdist_worker": "gw0"}, 0, id="inside-worker"),
    pytest.param({"xdist_worker": ""}, 0, id="worker-env-set-empty"),
    pytest.param({"numprocesses": 0}, 0, id="explicit-serial"),
    pytest.param({"numprocesses": 2}, 0, id="explicit-workers"),
    pytest.param({"numprocesses": "auto"}, 0, id="explicit-auto"),
    pytest.param({"dist": "loadfile"}, 0, id="explicit-distribution"),
    pytest.param({"usepdb": True}, 0, id="debugger"),
    pytest.param({"has_xdist": False}, 0, id="xdist-unavailable-or-disabled"),
    pytest.param({"serial_env": "1"}, 0, id="serial-opt-out"),
    pytest.param({"serial_env": "0"}, 4, id="serial-not-opted-out"),
])
def test_default_workers(overrides, expected):
    options = {
        "shard": "",
        "xdist_worker": None,
        "numprocesses": None,
        "dist": "no",
        "usepdb": False,
        "has_xdist": True,
        "serial_env": "",
        "cpu_count": 2,
    }
    options.update(overrides)
    assert default_workers(**options) == expected
