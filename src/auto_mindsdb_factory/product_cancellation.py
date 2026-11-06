"""Explicit direct-run cancellation, retained outside candidate source."""

import os
import threading
import time

from .product_agent import HarnessStopped
from .run_journal import RunJournal


class RunCancellation:
    def __init__(self, journal):
        self.path = journal.root / "cancellation.requested"
        self.requested = False

    def __call__(self):
        if self.requested:
            return True
        try:
            if not self.path.parent.is_dir():
                raise OSError("run evidence unavailable")
            self.requested = self.path.exists() or self.path.is_symlink()
            return self.requested
        except OSError:
            raise HarnessStopped("Cancellation state is unavailable") from None


def request_cancellation(store, run_id):
    existing = RunJournal(store, run_id, read_only=True)
    if not (existing.root / "manifest.json").is_file():
        raise ValueError("Cancellation requires an existing run manifest")
    journal = RunJournal(store, run_id)
    with journal._lock():
        if any(event["kind"] == "run_finished" for event in journal.events()):
            raise ValueError("Terminal run history cannot be cancelled or resumed")
        path = RunCancellation(journal).path
        try:
            with path.open("x") as stream:
                stream.write("Explicit operator cancellation\n")
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            return {"status": "cancellation_requested", "run_id": run_id}
    journal.append("cancellation_requested", source="operator")
    return {"status": "cancellation_requested", "run_id": run_id}


def run_cancellable(sandbox, command, *, cancelled, deadline):
    if cancelled() or time.monotonic() >= deadline:
        raise HarnessStopped("Execution cancelled or its deadline expired")
    finished, interrupted = threading.Event(), threading.Event()
    cleanup_errors = []
    def watch():
        while not finished.wait(0.05):
            try:
                stop = cancelled() or time.monotonic() >= deadline
            except BaseException:
                stop = True
            if stop:
                interrupted.set()
                try:
                    sandbox.abort()
                except BaseException:
                    cleanup_errors.append(True)
                return
    watcher = threading.Thread(target=watch, name="factory-command-cancellation", daemon=True)
    watcher.start()
    try:
        try:
            result = sandbox.run(command)
        except BaseException:
            if interrupted.is_set():
                raise HarnessStopped("Active command was cancelled") from None
            raise
    finally:
        finished.set()
        watcher.join(timeout=95)
        if watcher.is_alive() or cleanup_errors:
            raise HarnessStopped("Cancellation cleanup could not be confirmed")
    if interrupted.is_set() or cancelled():
        raise HarnessStopped("Active command was cancelled")
    return result
