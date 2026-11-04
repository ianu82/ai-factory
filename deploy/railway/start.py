"""Persistent single-controller supervisor; activation requires migrated evidence."""
import fcntl
import json
import os
import pwd
import signal
import subprocess
import sys
import time
from pathlib import Path

from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_linear_intake import LinearProductRoute

DATA = Path("/data")
CONFIG = DATA / "config"
TARGET = DATA / "target"
STORE = DATA / "evidence"
stopping = False


def stop(_signal, _frame):
    global stopping
    stopping = True


def run(*command):
    subprocess.run(command, check=True, timeout=120, stdout=subprocess.DEVNULL)


def record(status):
    temporary = DATA / "status.tmp"
    temporary.write_text(json.dumps(status))
    temporary.replace(DATA / "status.json")
    print(json.dumps(status), flush=True)


def main():
    if not DATA.is_mount():
        raise RuntimeError("A persistent volume must be mounted at /data")
    if os.getuid() == 0:
        account = pwd.getpwnam("factory")
        os.chown(DATA, account.pw_uid, account.pw_gid)
        os.setgroups([])
        os.setgid(account.pw_gid)
        os.setuid(account.pw_uid)
    lock = (DATA / "controller.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    CONFIG.mkdir(exist_ok=True)
    STORE.mkdir(exist_ok=True)
    for name in ("profile.json", "route.json"):
        path = CONFIG / name
        if not path.exists():
            path.write_bytes((Path(__file__).parent / name).read_bytes())
    if not TARGET.exists():
        run("git", "init", "--bare", str(TARGET))
        run("git", "-C", str(TARGET), "remote", "add", "origin", "https://github.com/ianu82/ai-factory.git")
    profile = ProductProfile.load(CONFIG / "profile.json", target_root=TARGET)
    route = LinearProductRoute.load(CONFIG / "route.json", target_root=TARGET)
    expected = {"profile_sha": profile.digest, "route_sha": route.digest}
    credential_ready = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        active = DATA / "activation.json"
        enabled = active.exists() and json.loads(active.read_text()) == expected
        status = {"mode": "paused", "at": time.time(), **expected}
        if enabled:
            record({**status, "mode": "running"})
            if not credential_ready:
                run("gh", "auth", "setup-git", "--hostname", "github.com")
                credential_ready = True
            command = [sys.executable, "-m", "auto_mindsdb_factory", "product-linear-worker",
                "--profile", str(CONFIG / "profile.json"), "--route", str(CONFIG / "route.json"),
                "--target-root", str(TARGET), "--store-dir", str(STORE), "--runner", "railway", "--once"]
            # The child owns task/source leases. An interrupted run is reconciled
            # from immutable evidence on restart and never silently rebuilt.
            result = subprocess.run(command, timeout=profile.document["max_run_seconds"] + 300)
            status.update(mode="polling", exit_code=result.returncode, at=time.time())
        record(status)
        for _ in range(60):
            if stopping:
                break
            time.sleep(1)


if __name__ == "__main__":
    main()
