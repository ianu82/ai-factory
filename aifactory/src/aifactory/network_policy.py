"""Trusted, immutable egress policy selection and resolution.

Repository JSON is administrative input, never inferred from commands or checkout
files. DNS runs in the control plane before sandbox launch; backends enforce the
resulting IP/port snapshot, not names resolved again inside an untrusted sandbox.
This module defines the contract, not a packet-filter implementation.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import socket
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit

from .config import Settings

MAX_REGISTRIES = 128
MAX_RESOLVED_ENDPOINTS = 1024
Mode = Literal["unrestricted", "restricted"]
BackendKind = Literal["local", "vm"]


class NetworkPolicyError(ValueError):
    """Invalid policy or trusted gateway configuration (messages contain no input)."""


class NetworkResolutionError(NetworkPolicyError):
    """An allowlisted endpoint could not be fully resolved; do not launch."""


class UnsupportedNetworkCapability(RuntimeError):
    """The backend cannot prove the requested isolation; never fall back."""


def _host(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 253 or not value.isascii():
        raise NetworkPolicyError("Network host must be an ASCII DNS name or IP address")
    if any(c in value for c in "/@*%[]\\") or value != value.strip():
        raise NetworkPolicyError("Invalid network host")
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        pass
    host = value.lower().removesuffix(".")
    # Numeric shorthand (127.1, octal, integer addresses) is resolver-dependent.
    if (not host or re.fullmatch(r"[0-9.]+", host) or ":" in host
            or re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)", host.split(".")[-1])
            or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                   for label in host.split("."))):
        raise NetworkPolicyError("Invalid network host")
    return host


@dataclass(frozen=True, order=True)
class Endpoint:
    host: str
    port: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", _host(self.host))
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise NetworkPolicyError("Network port must be an integer from 1 to 65535")

    def as_dict(self) -> dict[str, Any]:
        return {"host": self.host, "port": self.port}


def _fingerprint(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class NetworkPolicy:
    mode: Mode = "unrestricted"
    registries: tuple[Endpoint, ...] = ()
    gateway: Endpoint | None = None

    def __post_init__(self) -> None:
        if self.mode not in ("unrestricted", "restricted"):
            raise NetworkPolicyError("Network mode must be unrestricted or restricted")
        object.__setattr__(self, "registries", tuple(sorted(set(self.registries))))

    @property
    def endpoints(self) -> tuple[Endpoint, ...]:
        if self.mode == "unrestricted":
            return ()
        return tuple(sorted(set(self.registries + ((self.gateway,) if self.gateway else ()))))

    def as_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "registries": [e.as_dict() for e in self.registries],
                "gateway": self.gateway.as_dict() if self.gateway else None}

    @property
    def fingerprint(self) -> str:
        return _fingerprint(self.as_dict())


def validate_network(value: object) -> dict[str, Any]:
    """Validate a complete repository `network` object, returning canonical JSON.

    Omit the key for the legacy unrestricted default. Explicit null, unknown
    fields (including a repository-supplied gateway), and missing mode fail.
    No DNS or other external effects occur during configuration validation.
    """
    if not isinstance(value, dict) or set(value) - {"mode", "registries"}:
        raise NetworkPolicyError("Network policy must contain only mode and registries")
    mode = value.get("mode")
    if mode not in ("unrestricted", "restricted"):
        raise NetworkPolicyError("Network mode must be unrestricted or restricted")
    entries = value.get("registries", [])
    if not isinstance(entries, list) or len(entries) > MAX_REGISTRIES:
        raise NetworkPolicyError("Network registries must be a list of at most 128 endpoints")
    endpoints = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"host", "port"}:
            raise NetworkPolicyError("Each network registry must contain only host and port")
        endpoints.append(Endpoint(entry["host"], entry["port"]))
    return {"mode": mode, "registries": [e.as_dict() for e in sorted(set(endpoints))]}


def _gateway(settings: Settings, backend: BackendKind) -> Endpoint:
    if backend not in ("local", "vm"):
        raise UnsupportedNetworkCapability("Unknown network enforcement backend")
    url = settings.internal_url if backend == "local" else settings.public_url
    try:
        parsed = urlsplit(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or any(c.isspace() for c in url)
                or parsed.netloc.endswith(":")):
            raise ValueError
        port = parsed.port
        return Endpoint(parsed.hostname, port if port is not None else (443 if parsed.scheme == "https" else 80))
    except (TypeError, ValueError):
        raise NetworkPolicyError("Invalid trusted Factory gateway URL") from None


def select_policy(config: dict[str, Any], settings: Settings, *, backend: BackendKind,
                  benchmark: bool = False) -> NetworkPolicy:
    """Select only admin-configured registries and the backend's trusted gateway.

    A benchmark overrides even explicit unrestricted mode. Invalid persisted
    configuration fails closed, including when benchmark mode overrides it.
    """
    network = validate_network(config["network"]) if "network" in config else {
        "mode": "unrestricted", "registries": []}
    mode = "restricted" if benchmark else network["mode"]
    gateway = _gateway(settings, backend) if mode == "restricted" else None
    return NetworkPolicy(mode, tuple(Endpoint(**entry) for entry in network["registries"]), gateway)


@dataclass(frozen=True, order=True)
class ResolvedEndpoint:
    """A pinned host→IP TCP destination; family is AF_INET or AF_INET6."""

    host: str
    family: int
    address: str
    port: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", _host(self.host))
        endpoint = Endpoint(self.address, self.port)
        try:
            address = ipaddress.ip_address(endpoint.host)
        except ValueError:
            raise NetworkResolutionError("Resolved endpoint must be an IP address") from None
        expected = socket.AF_INET if address.version == 4 else socket.AF_INET6
        if self.family != expected:
            raise NetworkResolutionError("Resolved endpoint address family mismatch")
        object.__setattr__(self, "address", str(address))

    def as_dict(self) -> dict[str, Any]:
        return {"host": self.host, "family": 4 if self.family == socket.AF_INET else 6,
                "address": self.address, "port": self.port}


@dataclass(frozen=True)
class ResolvedNetworkPolicy:
    policy: NetworkPolicy
    endpoints: tuple[ResolvedEndpoint, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "endpoints", tuple(sorted(set(self.endpoints))))

    @property
    def fingerprint(self) -> str:
        """Identifies both the selected policy and its exact DNS snapshot."""
        return _fingerprint({"policy": self.policy.as_dict(),
                             "endpoints": [e.as_dict() for e in self.endpoints]})


def resolve_policy(policy: NetworkPolicy) -> ResolvedNetworkPolicy:
    """Resolve all A/AAAA answers on the trusted host, or fail without a policy.

    No DNS exception text is exposed (it can contain untrusted input). Literal
    addresses bypass DNS. The backend must pin this snapshot for the job's life;
    no sandbox DNS access or additional destination is implicitly allowed.
    """
    endpoints: set[ResolvedEndpoint] = set()
    for endpoint in policy.endpoints:
        try:
            literal = ipaddress.ip_address(endpoint.host)
        except ValueError:
            literal = None
        if literal is not None:
            addresses = [(socket.AF_INET if literal.version == 4 else socket.AF_INET6, str(literal))]
        else:
            try:
                answers = socket.getaddrinfo(endpoint.host, endpoint.port, socket.AF_UNSPEC,
                                             socket.SOCK_STREAM, socket.IPPROTO_TCP)
            except OSError:
                raise NetworkResolutionError("Network endpoint DNS resolution failed") from None
            if not answers or len(answers) > MAX_RESOLVED_ENDPOINTS:
                raise NetworkResolutionError("Network endpoint DNS answers empty or exceed limit")
            addresses = [(family, sockaddr[0]) for family, _, _, _, sockaddr in answers]
        for family, address in addresses:
            endpoints.add(ResolvedEndpoint(endpoint.host, family, address, endpoint.port))
            if len(endpoints) > MAX_RESOLVED_ENDPOINTS:
                raise NetworkResolutionError("Resolved network endpoint limit exceeded")
    return ResolvedNetworkPolicy(policy, tuple(endpoints))


@dataclass(frozen=True)
class NetworkReadiness:
    fingerprint: str
    ready: bool

    def require_ready(self, policy: ResolvedNetworkPolicy) -> None:
        if not self.ready or self.fingerprint != policy.fingerprint:
            raise UnsupportedNetworkCapability("Network enforcement readiness not established")


class NetworkEnforcement(Protocol):
    """Job-scoped handle owned by the trusted launcher, never the sandbox user.

    readiness must attest that deny-by-default TCP/UDP/other egress rules cover
    BOTH IP families (even if the allowlist is IPv4-only), allow only the resolved
    TCP pairs, pin each source hostname to its resolved addresses without sandbox
    DNS access, and cannot be changed or bypassed by the sandbox. No execution
    (including setup) may start until readiness.require_ready succeeds. Restricted
    unsupported environments raise UnsupportedNetworkCapability, never downgrade.
    cleanup is idempotent, runs on success, cancellation AND failed installation,
    and raises on failure; never attest cleanup that could not be verified.
    """

    def readiness(self) -> NetworkReadiness: ...

    def cleanup(self) -> None: ...


class NetworkBackend(Protocol):
    def apply(self, policy: ResolvedNetworkPolicy) -> NetworkEnforcement:
        """Install before execution; roll back partial effects on any exception.

        Return a handle only after installation. Unsupported isolation, IPv6
        coverage or tamper resistance raises UnsupportedNetworkCapability.
        """
        ...
