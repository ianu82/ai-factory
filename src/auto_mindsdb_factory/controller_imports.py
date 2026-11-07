"""Stdlib-only bootstrap boundary; import this before application dependencies."""

import os
import sys


IMPORT_ERROR = "MindsHub credentials require trusted controller imports outside candidate source"


def _within(path, root):
    try:
        return os.path.commonpath((path, root)) == root
    except ValueError:
        return False


def require_controller_import_path(target_root=None):
    # os/sys and os.path are interpreter-built-in/frozen modules. Do not import
    # pathlib, argparse, jsonschema, SDKs or application modules before this check.
    roots = (os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))),
             os.path.realpath(sys.prefix), os.path.realpath(sys.base_prefix))
    lexical_roots = (roots[0], os.path.abspath(sys.prefix), os.path.abspath(sys.base_prefix))
    # uv may expose the interpreter's own version alias in its stdlib path.
    base_alias = os.path.dirname(os.path.dirname(os.path.abspath(sys._base_executable)))
    if os.path.realpath(base_alias) == roots[2]:
        lexical_roots += (base_alias,)
    cwd = os.path.realpath(os.getcwd())
    target = os.path.realpath(os.fspath(target_root)) if target_root is not None else None
    if target is not None and _within(cwd, target):
        raise ValueError(IMPORT_ERROR)
    for entry in sys.path:
        if type(entry) is not str:
            raise ValueError(IMPORT_ERROR)
        lexical = os.path.abspath(entry or cwd)
        path = os.path.realpath(lexical)
        # Python retains the original entry. Both its lexical location and its
        # destination must be trusted; candidate aliases cannot borrow trust.
        if ((target is not None and (_within(lexical, target) or _within(path, target)))
                or not any(_within(lexical, root) for root in lexical_roots)
                or not any(_within(path, root) for root in roots)):
            raise ValueError(IMPORT_ERROR)
