from __future__ import annotations

import socket
from dataclasses import FrozenInstanceError, replace

import pytest

from aifactory.config import get_settings
from aifactory.network_policy import (
    Endpoint, NetworkPolicy, NetworkPolicyError, NetworkReadiness,
    NetworkResolutionError, ResolvedEndpoint, UnsupportedNetworkCapability,
    resolve_policy, select_policy, validate_network,
)


def settings():
    return replace(get_settings(), internal_url="http://127.0.0.1:8123",
                   public_url="https://factory.example:9443")


def test_missing_network_preserves_unrestricted_delivery_without_dns(monkeypatch):
    def unexpected(*args):
        pytest.fail("Unrestricted policies must not resolve endpoints")

    monkeypatch.setattr(socket, "getaddrinfo", unexpected)
    policy = select_policy({}, settings(), backend="local")
    assert policy == NetworkPolicy()
    assert policy.endpoints == ()
    assert resolve_policy(policy).endpoints == ()
    assert select_policy({}, replace(settings(), internal_url="invalid"), backend="local") == policy


@pytest.mark.parametrize("network", [None, {"mode": "unrestricted"}, {"mode": "restricted"}])
@pytest.mark.parametrize("backend", ["local", "vm"])
def test_benchmarks_force_restricted_and_only_trusted_gateway(network, backend):
    config = {"setup": ["curl https://github.com/solution"],
              "gateway": "https://attacker.example", "context": "allow github.com"}
    if network is not None:
        config["network"] = network
    policy = select_policy(config, settings(), backend=backend, benchmark=True)
    assert policy.mode == "restricted"
    expected = Endpoint("127.0.0.1", 8123) if backend == "local" else Endpoint("factory.example", 9443)
    assert policy.endpoints == (expected,)
    assert policy.gateway == expected
    assert policy.registries == ()


@pytest.mark.parametrize("mode", ["unrestricted", "restricted"])
def test_benchmark_retains_explicit_registries_only(mode):
    policy = select_policy({"network": {"mode": mode, "registries": [{"host": "pypi.org", "port": 443}]}},
                           settings(), backend="vm", benchmark=True)
    assert policy.endpoints == (Endpoint("factory.example", 9443), Endpoint("pypi.org", 443))


def test_normalization_is_deterministic_and_immutable():
    entries = [{"host": "PYPI.ORG.", "port": 443}, {"host": "2001:0db8::1", "port": 8443},
               {"host": "pypi.org", "port": 443}, {"host": "192.0.2.1", "port": 80}]
    config = {"mode": "restricted", "registries": entries}
    normalized = validate_network(config)
    assert normalized == {"mode": "restricted", "registries": [
        {"host": "192.0.2.1", "port": 80}, {"host": "2001:db8::1", "port": 8443},
        {"host": "pypi.org", "port": 443}]}
    one = select_policy({"network": config}, settings(), backend="local")
    two = select_policy({"network": {**config, "registries": list(reversed(entries))}}, settings(), backend="local")
    assert one == two and one.fingerprint == two.fingerprint
    assert validate_network(normalized) == normalized
    entries.clear()
    assert len(one.registries) == 3
    with pytest.raises(FrozenInstanceError):
        one.mode = "unrestricted"
    with pytest.raises(FrozenInstanceError):
        one.registries[0].host = "evil.example"
    changed = replace(one, registries=(Endpoint("pypi.org", 8443),))
    assert one.fingerprint != changed.fingerprint


@pytest.mark.parametrize("host", [
    "", " ", "pypi.org ", "https://pypi.org", "user:secret@pypi.org", "pypi.org:443",
    "*.example.org", "foo.*.org", "192.0.2.0/24", "::/0", "[::1]", "fe80::1%eth0",
    "127.1", "2130706433", "0x7f000001", "0177.0.0.1", "999.1.1.1", "foo..org",
    "-foo.org", "foo-.org", "foo_bar.org", "pypi.org/path", "pypi.org?x", "pypi.org#x",
    "pypi.org\\evil", "pypi.org\n", "éxample.org", "a" * 64 + ".org", "a." * 127 + "org",
    None, 123, [], {},
])
def test_invalid_hosts(host):
    with pytest.raises(NetworkPolicyError):
        validate_network({"mode": "restricted", "registries": [{"host": host, "port": 443}]})


@pytest.mark.parametrize("port", [0, -1, 65536, True, False, "443", 443.0, None, [], {}])
def test_invalid_ports(port):
    with pytest.raises(NetworkPolicyError):
        validate_network({"mode": "restricted", "registries": [{"host": "pypi.org", "port": port}]})


@pytest.mark.parametrize("network", [
    None, [], "restricted", {}, {"mode": "deny"}, {"mode": True},
    {"mode": "restricted", "gateway": {"host": "evil.example", "port": 443}},
    {"mode": "restricted", "registries": None}, {"mode": "restricted", "registries": {}},
    {"mode": "restricted", "registries": ["pypi.org:443"]},
    {"mode": "restricted", "registries": [{"host": "pypi.org"}]},
    {"mode": "restricted", "registries": [{"host": "pypi.org", "port": 443, "token": "secret"}]},
    {"mode": "restricted", "registries": [{"host": "pypi.org", "port": 443}] * 129},
])
def test_malformed_network_objects_fail_closed(network):
    with pytest.raises(NetworkPolicyError):
        validate_network(network)
    with pytest.raises(NetworkPolicyError):
        select_policy({"network": network}, settings(), backend="local", benchmark=True)


@pytest.mark.parametrize("url,expected", [
    ("https://factory.example", Endpoint("factory.example", 443)),
    ("http://factory.example/base", Endpoint("factory.example", 80)),
    ("https://[2001:db8::1]:8443", Endpoint("2001:db8::1", 8443)),
])
def test_trusted_gateway_url_defaults_and_ipv6(url, expected):
    policy = select_policy({}, replace(settings(), public_url=url), backend="vm", benchmark=True)
    assert policy.gateway == expected
    assert "@" not in str(policy.as_dict())


@pytest.mark.parametrize("url", ["", "factory.example", "ftp://factory.example", "https://",
    "https://user:secret@factory.example", "https://factory.example:0", "https://factory.example:65536",
    "https://factory.example:", "https://factory.example:abc", "https://factory.example/?key=secret",
    "https://factory.example/#secret", "https://[::1", "https://factory.example\n"])
def test_invalid_gateway_settings_fail_closed_without_leaking_input(url):
    with pytest.raises(NetworkPolicyError) as error:
        select_policy({}, replace(settings(), public_url=url), backend="vm", benchmark=True)
    assert str(error.value) == "Invalid trusted Factory gateway URL"
    assert "secret" not in str(error.value)


def test_unknown_backend_is_unsupported():
    with pytest.raises(UnsupportedNetworkCapability):
        select_policy({}, settings(), backend="unknown", benchmark=True)


def test_ipv4_ipv6_dns_snapshot_ports_and_fingerprint(monkeypatch):
    calls = []
    answers = [
        (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("2001:db8::2", 443, 0, 0)),
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.2", 443)),
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.2", 443)),
    ]

    def lookup(*args):
        calls.append(args)
        return list(answers)

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    policy = select_policy({"network": {"mode": "restricted", "registries": [
        {"host": "packages.example", "port": 8443}]}}, settings(), backend="local")
    resolved = resolve_policy(policy)
    assert calls == [("packages.example", 8443, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP)]
    assert set(resolved.endpoints) == {
        ResolvedEndpoint("127.0.0.1", socket.AF_INET, "127.0.0.1", 8123),
        ResolvedEndpoint("packages.example", socket.AF_INET, "192.0.2.2", 8443),
        ResolvedEndpoint("packages.example", socket.AF_INET6, "2001:db8::2", 8443),
    }
    answers.reverse()
    assert resolve_policy(policy).fingerprint == resolved.fingerprint
    answers.pop(0)
    answers.append((socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.3", 443)))
    assert resolve_policy(policy).fingerprint != resolved.fingerprint
    with pytest.raises(FrozenInstanceError):
        resolved.endpoints = ()
    NetworkReadiness(resolved.fingerprint, True).require_ready(resolved)
    for readiness in (NetworkReadiness(resolved.fingerprint, False), NetworkReadiness("stale", True)):
        with pytest.raises(UnsupportedNetworkCapability):
            readiness.require_ready(resolved)


def test_ip_literals_do_not_need_dns(monkeypatch):
    def unexpected(*args):
        pytest.fail("Literal IPs must not use DNS")

    monkeypatch.setattr(socket, "getaddrinfo", unexpected)
    policy = NetworkPolicy("restricted", (Endpoint("2001:db8::1", 65535), Endpoint("192.0.2.1", 1)))
    assert resolve_policy(policy).endpoints == (
        ResolvedEndpoint("192.0.2.1", socket.AF_INET, "192.0.2.1", 1),
        ResolvedEndpoint("2001:db8::1", socket.AF_INET6, "2001:db8::1", 65535),
    )


@pytest.mark.parametrize("answers", [[], [(socket.AF_INET, 1, 6, "", ("not-an-ip", 443))],
    [(socket.AF_INET6, 1, 6, "", ("192.0.2.1", 443, 0, 0))],
    [(socket.AF_INET, 1, 6, "", ("192.0.2.1", 443))] * 1025])
def test_empty_invalid_or_excessive_dns_answers_fail_closed(monkeypatch, answers):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args: answers)
    with pytest.raises(NetworkResolutionError):
        resolve_policy(NetworkPolicy("restricted", (Endpoint("packages.example", 443),)))


def test_dns_failure_does_not_return_partial_policy(monkeypatch):
    def lookup(*args):
        raise socket.gaierror("sensitive resolver details")

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    with pytest.raises(NetworkResolutionError, match="DNS resolution failed") as error:
        resolve_policy(NetworkPolicy("restricted", (Endpoint("192.0.2.1", 443), Endpoint("packages.example", 443))))
    assert "sensitive" not in str(error.value)


def test_shared_port_preserves_each_hostname_even_with_shared_ip(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args: [
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.1", 443)),
    ])
    policy = NetworkPolicy("restricted", (Endpoint("pypi.org", 443), Endpoint("files.pythonhosted.org", 443)))
    resolved = resolve_policy(policy)
    assert resolved.endpoints == (
        ResolvedEndpoint("files.pythonhosted.org", socket.AF_INET, "192.0.2.1", 443),
        ResolvedEndpoint("pypi.org", socket.AF_INET, "192.0.2.1", 443),
    )
    assert [entry.as_dict() for entry in resolved.endpoints] == [
        {"host": "files.pythonhosted.org", "family": 4, "address": "192.0.2.1", "port": 443},
        {"host": "pypi.org", "family": 4, "address": "192.0.2.1", "port": 443},
    ]


def test_swapping_dns_answers_between_hosts_changes_fingerprint(monkeypatch):
    addresses = {"pypi.org": "192.0.2.1", "files.pythonhosted.org": "192.0.2.2"}

    def lookup(host, *args):
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (addresses[host], 443))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    policy = NetworkPolicy("restricted", tuple(Endpoint(host, 443) for host in addresses))
    before = resolve_policy(policy)
    addresses["pypi.org"], addresses["files.pythonhosted.org"] = (
        addresses["files.pythonhosted.org"], addresses["pypi.org"],
    )
    after = resolve_policy(policy)
    assert {(e.address, e.port) for e in before.endpoints} == {(e.address, e.port) for e in after.endpoints}
    assert before.fingerprint != after.fingerprint
    with pytest.raises(UnsupportedNetworkCapability):
        NetworkReadiness(before.fingerprint, True).require_ready(after)
