"""Bounded publication requests for a separately authenticated GitHub operator."""

import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass
from urllib.parse import urlencode


class HandoffError(RuntimeError):
    """Uncertain publication is terminal; never retry a write automatically."""


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class PublicationScope:
    repository: str
    run_id: str
    base_sha: str
    candidate_sha: str
    base_branch: str

    def __post_init__(self):
        patterns = {"repository": r"[A-Za-z0-9][A-Za-z0-9_-]*/[A-Za-z0-9_.-]+",
            "run_id": r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}",
            "base_sha": r"[0-9a-f]{40}", "candidate_sha": r"[0-9a-f]{40}",
            "base_branch": r"[A-Za-z0-9][A-Za-z0-9_/-]*"}
        if any(type(value) is not str or not re.fullmatch(patterns[key], value)
               for key, value in asdict(self).items()):
            raise HandoffError("Invalid publication scope")
        if (self.repository.split('/')[1] in {".", ".."} or self.base_sha == self.candidate_sha
                or "//" in self.base_branch or self.base_branch.endswith("/")):
            raise HandoffError("Invalid publication base")

    @property
    def branch(self):
        return "codex/factory-" + self.run_id

    def validate(self, operation, arguments):
        if operation == "git":
            reads = [["ls-remote", "--heads", "origin", "refs/heads/" + b]
                     for b in (self.base_branch, self.branch)]
            writes = [["push", "--force-with-lease=refs/heads/" + b + ":" + expected,
                       "origin", self.candidate_sha + ":refs/heads/" + b]
                      for b, expected in ((self.branch, ""), (self.base_branch, self.base_sha))]
            if arguments not in reads + writes:
                raise HandoffError("Git operation exceeds publication scope")
        elif operation == "api":
            if type(arguments) is not dict or set(arguments) != {"path", "body"}:
                raise HandoffError("Invalid API request")
            path, body = arguments["path"], arguments["body"]
            reads = {"pulls?" + urlencode({"state": "all", "head": self.repository.split('/')[0] + ':' + self.branch}),
                f"commits/{self.candidate_sha}/status?per_page=100",
                f"commits/{self.candidate_sha}/check-runs?per_page=100&filter=latest"}
            if body is None:
                if type(path) is not str or not (path in reads or re.fullmatch(r"pulls/[1-9][0-9]*", path)):
                    raise HandoffError("API read exceeds publication scope")
            elif path == "pulls" and type(body) is dict and set(body) == {"title", "head", "base", "body"}:
                if (body["head"] != self.branch or body["base"] != self.base_branch
                        or any(type(body[k]) is not str or not 0 < len(body[k]) <= 30000 for k in ("title", "body"))):
                    raise HandoffError("Pull request exceeds publication scope")
            elif path == f"statuses/{self.candidate_sha}" and type(body) is dict:
                expected = {"state": "success", "context": "ai-factory/verification",
                    "description": "Independent executable checks and review passed"}
                url = body.get("target_url")
                if (set(body) != {*expected, "target_url"} or any(body[k] != v for k, v in expected.items())
                        or type(url) is not str or not re.fullmatch(
                            re.escape(f"https://github.com/{self.repository}/pull/") + r"[1-9][0-9]*", url)):
                    raise HandoffError("Status write exceeds publication scope")
            else:
                raise HandoffError("API write exceeds publication scope")
        else:
            raise HandoffError("Unknown publication operation")

    def validate_request(self, request, *, now=None):
        if (type(request) is not dict or set(request) != {"version", "id", "scope", "expires", "operation", "arguments"}
                or request["version"] != 1 or request["scope"] != asdict(self)
                or type(request["id"]) is not str or not re.fullmatch(r"[0-9a-f]{32}", request["id"])
                or type(request["expires"]) not in (int, float)
                or not (time.time() if now is None else now) < request["expires"] <= (time.time() if now is None else now) + 3600):
            raise HandoffError("Stale or mismatched publication request")
        self.validate(request["operation"], request["arguments"])
