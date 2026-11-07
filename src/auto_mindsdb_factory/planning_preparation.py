"""Fixed, content-free observations of controller-owned planning preparation."""

import time
from contextlib import contextmanager


_OPERATIONS = {"source_export", "source_inventory", "setup_entry", "setup_command",
               "setup_finalization", "setup_validation"}


@contextmanager
def preparation_operation(journal, operation):
    if journal is None:
        yield
        return
    if type(operation) is not str or operation not in _OPERATIONS:
        raise ValueError("Unknown planning preparation operation")
    journal.append("planning_preparation_started", role="planner", operation=operation)
    started, returned = time.monotonic(), False
    try:
        yield
        returned = True
    finally:
        journal.append("planning_preparation_finished", role="planner", operation=operation,
                       returned=returned, duration_seconds=round(time.monotonic() - started, 3))
