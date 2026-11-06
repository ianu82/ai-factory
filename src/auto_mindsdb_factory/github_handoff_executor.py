"""GitHub network tool using the operator's existing, independently scoped auth."""

import hashlib
import json
import re
import subprocess
import time

from .github_handoff import HandoffError, digest
from .github_handoff_channel import read_document, write_exclusive
from .product_workspace import git


def import_candidate(scope, target, bundle, bundle_sha256):
    if (bundle.is_symlink() or not bundle.is_file() or bundle.stat().st_size > 64 * 1024 * 1024
            or not re.fullmatch(r"[0-9a-f]{64}", bundle_sha256)
            or hashlib.sha256(bundle.read_bytes()).hexdigest() != bundle_sha256):
        raise HandoffError("Candidate bundle checksum unavailable or mismatched")
    refs = git(target, "bundle", "list-heads", str(bundle)).splitlines()
    if (len(refs) != 1 or not re.fullmatch(re.escape(scope.candidate_sha + " refs/factory/" + scope.run_id)
                                       + r"/attempt-[1-5]", refs[0])):
        raise HandoffError("Candidate bundle does not match the operator scope")
    git(target, "bundle", "verify", str(bundle))
    git(target, "fetch", "--no-tags", str(bundle), refs[0].split()[1])


def execute_request(scope, request, target, receipt, *, bundle=None, bundle_sha256=None, runner=subprocess.run):
    """Exactly one network operation; a retained intent forbids replay after uncertainty."""
    scope.validate_request(request)
    target, receipt = target.resolve(), receipt.absolute()
    if receipt.resolve().is_relative_to(target):
        raise HandoffError("Publication receipts must be outside candidate source")
    urls = {f"https://github.com/{scope.repository}.git", f"https://github.com/{scope.repository}",
            f"git@github.com:{scope.repository}.git"}
    if (git(target, "remote", "get-url", "--all", "origin") not in urls
            or git(target, "remote", "get-url", "--push", "--all", "origin") not in urls):
        raise HandoffError("Operator repository does not match the publication scope")
    if bundle is not None:
        import_candidate(scope, target, bundle, bundle_sha256 or "")
    if git(target, "rev-list", "--parents", "-n", "1", scope.candidate_sha).split() != [scope.candidate_sha, scope.base_sha]:
        raise HandoffError("Operator candidate is not the exact single-commit descendant")
    scope.validate_request(request)  # Preparation cannot extend an expired authorization.
    intent = receipt.with_name(receipt.name + ".intent")
    write_exclusive(intent, {"request_sha256": digest(request), "request": request})
    body = None
    if request["operation"] == "git":
        command = ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
                   "-C", str(target), *request["arguments"]]
    else:
        body = request["arguments"]["body"]
        command = ["gh", "api", "--hostname", "github.com",
                   f"repos/{scope.repository}/" + request["arguments"]["path"]]
        if body is not None:
            command += ["--method", "POST", "--input", "-"]
    response = {"request_sha256": digest(request), "ok": False, "result": None}
    try:
        remaining = request["expires"] - time.time()
        if remaining <= 0:
            raise HandoffError("Publication request expired before execution")
        result = runner(command, input=json.dumps(body) if body is not None else None,
            capture_output=True, text=True, timeout=min(60, remaining))
        if result.returncode or len(result.stdout.encode()) > 4 * 1024 * 1024:
            raise HandoffError("GitHub operation failed or its result is uncertain")
        response["result"] = json.loads(result.stdout) if request["operation"] == "api" else result.stdout.rstrip("\n")
        response["ok"] = True
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise HandoffError("GitHub result unavailable; inspect remote state before any new operation") from None
    finally:
        write_exclusive(receipt, response)
    return response
