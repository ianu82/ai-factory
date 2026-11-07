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
def enter_cancellable(manager, *, cancelled, deadline, finalization_deadline=None, on_finalization_failure=None):
    """Watch entry and finalization; commands retain their sole abort owner.

    Drain each lifecycle operation before proceeding. No watcher spans the body.
    Finalization includes context exit/source download under the supplied phase
    deadline, or the entry deadline when no separate reserve was provided.
    """
    class Entry:
        completed = retired = False

        def __init__(self):
            self.retirement = threading.Lock()

        def run(self, command):
            result = manager.__enter__()
            self.completed = True
            return result

        def abort(self):
            with self.retirement:
                if not self.retired:
                    manager.abort()
                    self.retired = True

    entry = Entry()
    try:
        sandbox = run_cancellable(entry, [], cancelled=cancelled, deadline=deadline)
        # Entry can return just before the watcher next wakes. Admit no body
        # after the deadline, even when that last polling interval did not run.
        if time.monotonic() >= deadline:
            raise HarnessStopped("Sandbox entry exceeded its deadline")
    except BaseException as error:
        if entry.completed and not entry.retired:
            # Removal can race creation and fail before the resource exists.
            # Retry only after entry drains, never treating an attempt as proof.
            try:
                entry.abort()
            except BaseException:
                raise HarnessStopped("Sandbox entry cleanup could not be confirmed") from None
            if isinstance(error, HarnessStopped):
                raise HarnessStopped("Sandbox entry interrupted; late environment retired") from None
        raise

    def exit_context(exception):
        class Finalization:
            interrupted = False
            def run(self, command):
                return manager.__exit__(*exception)
            def abort(self):
                self.interrupted = True
                entry.abort()
        finalizer = Finalization()
        end = deadline if finalization_deadline is None else finalization_deadline
        try:
            result = run_cancellable(finalizer, [], cancelled=cancelled, deadline=end)
            if time.monotonic() >= end:
                raise HarnessStopped("Sandbox finalization exceeded its deadline")
            return result
        except BaseException:
            # A pre-expired call or a return between watcher polls still owns a
            # resource. Never duplicate an abort already attempted by the watcher.
            try:
                if not finalizer.interrupted:
                    try:
                        finalizer.abort()
                    except BaseException:
                        raise HarnessStopped("Sandbox finalization cleanup could not be confirmed") from None
            finally:
                if on_finalization_failure is not None:
                    on_finalization_failure()
            raise
    try:
        yield sandbox
    except BaseException:
        if not exit_context(sys.exc_info()):
            raise
    else:
        exit_context((None, None, None))
