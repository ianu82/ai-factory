"""Trusted VM driver; installed outside source and run as the Docker client user."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from .docker_runner import DockerSandbox, RunnerError
from .source_transfer import pack_source, write_source

WORKSPACE = Path("/work/source")
MAX_MESSAGE_BYTES = 100 * 1024 * 1024


def dispatch(request):
    action = request["action"]
    if action == "upload":
        write_source(WORKSPACE, request["source"])
        return {"uploaded": True}
    if action == "download":
        return {"source": pack_source(WORKSPACE)}
    options = request["sandbox"]
    name = options["name"]
    if not re.fullmatch(r"ai-factory-[0-9a-f]{32}", name):
        raise RunnerError("Invalid container identity")
    sandbox = DockerSandbox(WORKSPACE, options["image"], writable=options["writable"],
        network=options["network"], timeout=options["timeout"], platform=options["platform"])
    sandbox.name = name
    if action == "enter":
        sandbox.__enter__()
        return {"started": True}
    if action == "run":
        return sandbox.run(request["command"], stdin=request.get("stdin"))
    if action == "exit":
        sandbox.__exit__()
        remaining = sandbox._docker("ps", "--all", "--quiet", "--filter", f"name=^/{name}$")
        if remaining.stdout.strip():
            raise RunnerError("Container cleanup was not confirmed")
        return {"removed": True}
    raise RunnerError("Unknown remote operation")


def main():
    try:
        raw = sys.stdin.buffer.read(MAX_MESSAGE_BYTES + 1)
        if len(raw) > MAX_MESSAGE_BYTES:
            raise RunnerError("Remote message exceeds limit")
        response = dispatch(json.loads(raw))
    except Exception as exc:
        # Never return provider bodies, environment variables or exception text.
        response = {"error": type(exc).__name__}
    print(json.dumps(response))


if __name__ == "__main__":
    main()
