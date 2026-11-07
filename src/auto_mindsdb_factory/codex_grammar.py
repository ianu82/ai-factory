"""Constrain generation to canonical JSON actions; execution still validates them.

The pinned CLI's SOURCE grammar accepts arbitrary JavaScript. This regular
language emits only run/finish objects with the existing schemas' structure.
Canonical field order constrains generation, not the consumer's acceptance.
"""

_WS = r"[ \t\r\n]*"
_STRING = r'"(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"'
_STATUS = r'"(?:passed|failed|unverified)"'


def _array(item, *, required=False):
    items = item + "(?:" + _WS + "," + _WS + item + ")*"
    return r"\[" + _WS + (items if required else "(?:" + items + ")?") + _WS + r"\]"


def _object(fields):
    return (r"\{" + _WS + (_WS + "," + _WS).join(
        '"' + key + '"' + _WS + ":" + _WS + value for key, value in fields) + _WS + r"\}")


_COMMAND = _object([("command", _array(_STRING, required=True))])
_CRITERION = _object([("id", _STRING), ("status", _STATUS), ("evidence", _STRING)])
_REPORT = _object([("status", _STATUS), ("summary", _STRING),
                   ("criteria", _array(_CRITERION)), ("findings", _array(_STRING))])
_ACTION = (r"text\(await tools\.(?:run\(" + _COMMAND
           + r"\)|finish\(" + _REPORT + r"\))\)")
# Keep the CLI's Lark dialect. One terminal consumes the complete start rule;
# regex document anchors are unnecessary, and delimiter slashes must be escaped.
ACTION_GRAMMAR = "start: /" + _ACTION.replace("/", r"\/") + "/"
