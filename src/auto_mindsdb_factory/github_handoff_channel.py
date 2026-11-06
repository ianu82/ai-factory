"""Private, append-only handoff channel; candidates never receive these paths."""

import hashlib
import json
import os
import re
import time
from dataclasses import asdict
from uuid import uuid4

from .github_handoff import HandoffError, PublicationScope, digest
from .product_workspace import git


def write_exclusive(path, value):
    data = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, 0o600)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        descriptor = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def read_document(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024 * 1024:
        raise HandoffError("Publication response is absent, linked or oversized")
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        raise HandoffError("Publication response is unreadable") from None


class GitHubHandoff:
    def __init__(self, profile, task, target, *, deadline, check_cancelled):
        self.profile, self.task, self.target = profile, task, target
        self.deadline, self.check_cancelled = deadline, check_cancelled
        self.scope = self.root = self.journal = None

    def bind(self, candidate, journal):
        if self.scope is not None:
            raise HandoffError("A publication channel cannot be rebound")
        self.check_cancelled()
        settings = self.profile.document
        self.scope = PublicationScope(settings["repository"], self.task.document["id"],
            candidate["base_sha"], candidate["candidate_sha"], settings["base_branch"])
        self.root, self.journal = journal.root / "github-handoff", journal
        self.root.mkdir(mode=0o700)  # Existing history must never be resumed or overwritten.
        bundle = self.root / "candidate.bundle"
        refs = git(self.target, "for-each-ref", "--format=%(refname)",
                   "--points-at=" + self.scope.candidate_sha, "refs/factory/" + self.scope.run_id).splitlines()
        refs = [ref for ref in refs if re.fullmatch(r"refs/factory/" + re.escape(self.scope.run_id) + r"/attempt-[1-5]", ref)]
        if not refs:
            raise HandoffError("Candidate has no original retained attempt reference")
        git(self.target, "bundle", "create", str(bundle), refs[0], "^" + self.scope.base_sha)
        os.chmod(bundle, 0o600)
        with bundle.open("rb") as stream:
            os.fsync(stream.fileno())
        write_exclusive(self.root / "scope.json", {"version": 1, "scope": asdict(self.scope),
            "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest()})
        journal.append("publication_transport_bound", transport="operator-handoff-v1", scope=asdict(self.scope))

    def api(self, path, body=None):
        return self._request("api", {"path": path, "body": body})

    def git(self, *arguments):
        return self._request("git", list(arguments))

    def _request(self, operation, arguments):
        if self.scope is None:
            raise HandoffError("Publication transport is not authorized")
        self.check_cancelled()
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise HandoffError("Publication deadline exhausted")
        request = {"version": 1, "id": uuid4().hex, "scope": asdict(self.scope),
            "expires": time.time() + min(remaining, 300), "operation": operation, "arguments": arguments}
        self.scope.validate_request(request)
        fingerprint = digest(request)
        write_exclusive(self.root / (request["id"] + ".request.json"), request)
        self.journal.append("publication_transport_requested", request_id=request["id"],
                            request_sha256=fingerprint, operation=operation)
        response_path = self.root / (request["id"] + ".response.json")
        while time.monotonic() < self.deadline and time.time() < request["expires"]:
            self.check_cancelled()
            if response_path.exists() or response_path.is_symlink():
                response = read_document(response_path)
                if (type(response) is not dict or set(response) != {"request_sha256", "ok", "result"}
                        or response["request_sha256"] != fingerprint or response["ok"] is not True
                        or (operation == "git" and type(response["result"]) is not str)):
                    raise HandoffError("Publication failed or its result is uncertain; inspect remote state")
                self.journal.append("publication_transport_completed", request_id=request["id"],
                                    request_sha256=fingerprint, response_sha256=digest(response))
                return response["result"]
            time.sleep(min(0.1, max(0, self.deadline - time.monotonic())))
        raise HandoffError("Publication handoff timed out; inspect remote state before any new delivery")
