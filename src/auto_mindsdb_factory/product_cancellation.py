"""Explicit direct-run cancellation, retained outside candidate source."""

import os
import sys
import threading
import time
from contextlib import contextmanager

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


@contextmanager
def enter_cancellable(manager, *, cancelled, deadline):
    """Watch sandbox entry only; commands retain their existing sole abort owner.

    Drain entry before yielding or failing. Normal context exit still owns source
    download and cleanup; a cancelled entry must never reach that download path.
    """
    class Entry:
        completed = aborted = False

        def run(self, command):
            result = manager.__enter__()
            self.completed = True
            return result

        def abort(self):
            self.aborted = True
            manager.abort()

    entry = Entry()
    try:
        sandbox = run_cancellable(entry, [], cancelled=cancelled, deadline=deadline)
        # Entry can return just before the watcher next wakes. Admit no body
        # after the deadline, even when that last polling interval did not run.
        if time.monotonic() >= deadline:
            raise HarnessStopped("Sandbox entry exceeded its deadline")
    except BaseException:
        if entry.completed and not entry.aborted:
            try:
                entry.abort()
            except BaseException:
                raise HarnessStopped("Sandbox entry cleanup could not be confirmed") from None
        raise
    try:
        yield sandbox
    except BaseException:
        if not manager.__exit__(*sys.exc_info()):
            raise
    else:
        manager.__exit__(None, None, None)
