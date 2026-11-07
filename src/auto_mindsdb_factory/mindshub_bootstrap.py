"""Trusted root bootstrap; retain one frozen binding after permanent privilege drop."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys

from .__main__ import build_parser, main as product_main
from .product_profile import ProductProfile
from .product_inference_auth import require_controller_import_path, _object, _constant
from .product_inference_runtime import _read_owned, resolve_execution_context, MindsHubQualificationConfig


FIELDS = frozenset({"version", "argv_sha256", "profile_sha256", "target_root", "receipt_path",
    "receipt_sha256", "credential_file", "credential_version", "reasoning_effort", "timeout_seconds", "uid", "gid"})


def argv_digest(argv):
    return hashlib.sha256(json.dumps(argv, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()


def _authorization(path, digest, target):
    if type(digest) is not str or not re.fullmatch("[0-9a-f]{64}", digest):
        raise ValueError()
    data = _read_owned(path, target, 65536)
    document = json.loads(data, object_pairs_hook=_object, parse_constant=_constant)
    if (type(document) is not dict or set(document) != FIELDS
            or type(document["version"]) is not int or document["version"] != 1
            or any(type(document[k]) is not str or not re.fullmatch("[0-9a-f]{64}", document[k])
                   for k in ("argv_sha256", "profile_sha256", "receipt_sha256"))
            or any(type(document[k]) is not int or not 1 <= document[k] < 2**32 - 1 for k in ("uid", "gid"))
            or type(document["timeout_seconds"]) is not int or document["timeout_seconds"] < 1
            or type(document["credential_version"]) is not list or len(document["credential_version"]) != 6
            or any(type(n) is not int for n in document["credential_version"])
            or type(document["reasoning_effort"]) is not str
            or document["reasoning_effort"] not in MindsHubQualificationConfig.REASONING_EFFORTS):
        raise ValueError()
    for name in ("target_root", "receipt_path", "credential_file"):
        value = document[name]
        if type(value) is not str or not Path(value).is_absolute() or str(Path(value).resolve()) != value:
            raise ValueError()
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError()
    return document


def _drop_privileges(uid, gid):
    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)
    if (os.getuid() != uid or os.geteuid() != uid or os.getgid() != gid or os.getegid() != gid
            or os.getgroups() or (hasattr(os, "getresuid") and os.getresuid() != (uid, uid, uid))
            or (hasattr(os, "getresgid") and os.getresgid() != (gid, gid, gid))):
        raise ValueError()


def prepare_context(argv, *, authorization, authorization_sha256):
    if type(argv) not in (list, tuple) or any(type(arg) is not str for arg in argv):
        raise ValueError()
    argv = tuple(argv)
    if os.getuid() != 0 or os.geteuid() != 0 or not argv or argv[0] != "product-deliver":
        raise ValueError()
    args = build_parser().parse_args(argv)
    target = args.target_root.resolve()
    if not args.target_root.is_absolute() or args.target_root != target:
        raise ValueError()
    require_controller_import_path(target)
    # Root validates only external profile/schema/hash. Normal Git origin checks
    # run under the controller identity; no root safe.directory exception.
    profile = ProductProfile.load(args.profile, target_root=target, codex_qualification=args.codex_qualification)
    document = _authorization(authorization, authorization_sha256, target)
    if (document["argv_sha256"] != argv_digest(argv) or document["profile_sha256"] != profile.digest
            or document["target_root"] != str(target)):
        raise ValueError()
    context = resolve_execution_context(profile.document, target_root=target,
        **{key: document[key] for key in ("receipt_path", "receipt_sha256", "credential_file",
            "credential_version", "reasoning_effort", "timeout_seconds")})
    _drop_privileges(document["uid"], document["gid"])
    require_controller_import_path(target)
    return context, argv


def bootstrap(argv, *, authorization, authorization_sha256):
    context, admitted = prepare_context(argv, authorization=authorization, authorization_sha256=authorization_sha256)
    return product_main(list(admitted), inference_context=context)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authorization", type=Path, required=True)
    parser.add_argument("--authorization-sha256", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    try:
        return bootstrap(command, authorization=args.authorization, authorization_sha256=args.authorization_sha256)
    except Exception:
        print("MindsHub bootstrap refused; inspect approved metadata and controller setup", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
