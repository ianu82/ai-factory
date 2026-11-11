"""Pure closed-allowlist extraction: ``extract(changed_paths, before, after)``.

Repo-relative paths + whole UTF-8 snapshots (missing/None = absent) produce
immutable Facts: kind/change/name, before/after metadata, 1-based base/head
locations (removals: base; changes: both). Metadata: route framework/method/path/
handler or env names only, never values/defaults/code/hashes. dataclasses.asdict
serializes results. Match binding/endpoint then unique path edits; ambiguity is
a limitation. Only changed paths are scanned; code is never imported/executed.

Supported syntax (closed allowlist; unsupported forms yield limitations):
* Python module-level FastAPI/APIRouter and Flask/Blueprint bindings, direct or
  qualified imports; literal decorator paths/method lists. FastAPI decorators:
  get/post/put/patch/delete/head/options/trace/route/api_route; Flask decorators:
  get/post/put/patch/delete/route; add_api_route/add_url_rule accept named module
  functions. Mounts require matching frameworks. include_router prefixes compose;
  register_blueprint's literal url_prefix overrides Blueprint's prefix.
  Handler AST edits count; nested routes
  and application factories are unsupported.
* Express express()/express.Router()/Router() single const/let/var declarators,
  lexical blocks (var only at module level), semicolon-terminated statements, HTTP
  methods, route(literal).method(...), use(literal, ...middleware, router).
  Handlers: inline/named functions, function expressions, arrows. Lexical receiver
  identities/mounts use headers, not sibling ordinals. Function/arrow/class/object
  method parameters shadow all names, including destructured names.
  Multi-declarators, generic calls, dynamic mounts, reassigned receivers (every
  assignment/update/destructuring), ambiguous handlers and aliases are unsupported.
* Next app/api/**/route.{js,jsx,ts,tsx}, including src/: named uppercase HTTP
  function or const arrow/function exports (GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS).
  pages/api files, including src/: default function/arrow or named binding.
  Preserve dynamic segments; omit app route groups. Other exports: unsupported.
* Python os.getenv/os.environ.get/os.environ[literal], imported aliases and
  augmented-assignment reads, not plain stores. JS process.env.KEY/['KEY'] only.
  Unsupported: shadowed/mutated process, dynamic names, env aliases/destructuring,
  computed process access, optional chains, property/index/call continuations.
  Comments and literal contents are not code.
* JS lexer: quoted strings, plain templates without ${, comments, identifiers,
  numbers, punctuation. Slash starts regex only after _REGEX_START tokens; it is
  division only after a non-keyword identifier, string, number, regex or ].
  Other contexts (notably after ) or }), unterminated literals and template
  substitutions yield file-level dynamic_route AND dynamic_env limitations,
  suppressing BOTH sides, as do parse errors: never invent additions/removals.

No completeness claim: indirect routing/exports, middleware effects, cross-file
mounts and metaprogramming are unresolved. Retain limitations with facts.
"""
from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
import re
from typing import Literal


@dataclass(frozen=True)
class Location:
    path: str
    line: int
    side: Literal["base", "head"]


@dataclass(frozen=True)
class Fact:
    kind: Literal["route", "env"]
    change: Literal["added", "removed", "changed", "touched"]
    name: str
    before: Mapping[str, str] | None
    after: Mapping[str, str] | None
    locations: tuple[Location, ...]


@dataclass(frozen=True)
class Limitation:
    code: Literal["dynamic_route", "dynamic_env", "parse_error"]
    message: str
    locations: tuple[Location, ...]


@dataclass(frozen=True)
class Extraction:
    facts: tuple[Fact, ...]
    limitations: tuple[Limitation, ...]


@dataclass
class _Item:
    kind: Literal["route", "env"]
    key: str
    name: str
    metadata: dict[str, str]
    fingerprint: str
    location: Location
    match_path: str | None = None


_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "all", "trace"}


class _Scan:
    def __init__(self, path: str, source: str, side: Literal["base", "head"]):
        self.path, self.source, self.side = path, source, side
        self.items: dict[tuple[str, str], _Item] = {}
        self.limitations: list[Limitation] = []
        self.counts: dict[str, int] = {}
        self.opaque = False

    def loc(self, line: int) -> Location:
        return Location(self.path, line, self.side)

    def limit(self, code: Literal["dynamic_route", "dynamic_env", "parse_error"], line: int,
              message: str) -> None:
        value = Limitation(code, message, (self.loc(line),))
        if value not in self.limitations:
            self.limitations.append(value)

    def env(self, name: str, line: int) -> None:
        self.items.setdefault(("env", name), _Item(
            "env", name, name, {"name": name}, "", self.loc(line)))

    def route(self, identity: str, framework: str, method: str, path: str,
              handler: str, body: str, line: int, *, match_path: str | None = None) -> None:
        ordinal = self.counts.get(identity, 0)
        self.counts[identity] = ordinal + 1
        key = f"{identity}:{ordinal}"
        meta = {"framework": framework, "method": method.upper(), "path": path, "handler": handler}
        digest = hashlib.sha256(body.encode()).hexdigest()
        self.items[("route", key)] = _Item(
            "route", key, f"{method.upper()} {path}", meta, digest, self.loc(line), match_path)


def extract(changed_paths: Iterable[str], before: Mapping[str, str | None],
            after: Mapping[str, str | None]) -> Extraction:
    """Compare source snapshots without IO, imports, evaluation or source disclosure."""
    facts: list[Fact] = []
    limitations: list[Limitation] = []
    for path in sorted(set(changed_paths)):
        scans = []
        for snapshots, side in ((before, "base"), (after, "head")):
            source = snapshots.get(path)
            scan = _Scan(path, source or "", side)
            if source is not None:
                if path.endswith(".py"):
                    _python(scan)
                elif re.search(r"\.(?:[cm]?js|jsx|tsx?)$", path):
                    _javascript(scan)
            scans.append(scan)
        base, head = scans
        for old, new in (() if base.opaque or head.opaque else _matches(base, head)):
            if old and new and (old.metadata, old.fingerprint) == (new.metadata, new.fingerprint):
                continue
            item = new or old
            assert item is not None
            facts.append(Fact(item.kind, "changed" if old and new else "added" if new else "removed",
                              item.name, old.metadata if old else None, new.metadata if new else None,
                              tuple(i.location for i in (old, new) if i)))
        limitations.extend(base.limitations)
        limitations.extend(head.limitations)
    return Extraction(tuple(facts), tuple(limitations))


def _matches(base: _Scan, head: _Scan) -> Iterable[tuple[_Item | None, _Item | None]]:
    """Match endpoints before considering path edits; never pair by source order.

    Ordinals only preserve multiple declarations within a snapshot. They are not
    identities across snapshots: inserting a route must not shift its neighbours.
    """
    for key in sorted(base.items.keys() | head.items.keys()):
        if key[0] == "env":
            yield base.items.get(key), head.items.get(key)
    groups: dict[str, tuple[list[_Item], list[_Item]]] = {}
    for side, scan in enumerate((base, head)):
        for (kind, key), item in scan.items.items():
            if kind == "route":
                identity = key.rsplit(":", 1)[0]
                groups.setdefault(identity, ([], []))[side].append(item)
    for identity in sorted(groups):
        old, new = groups[identity]
        for path in sorted({i.match_path or i.metadata["path"] for i in old + new}):
            left = [i for i in old if (i.match_path or i.metadata["path"]) == path]
            right = [i for i in new if (i.match_path or i.metadata["path"]) == path]
            if len(left) > 1 or len(right) > 1:
                # Reused names in anonymous sibling blocks share a stable
                # identity. A distinct mounted URL still identifies an endpoint.
                for url in sorted({i.metadata["path"] for i in left + right}):
                    lurl = [i for i in left if i.metadata["path"] == url]
                    rurl = [i for i in right if i.metadata["path"] == url]
                    if len(lurl) == len(rurl) == 1:
                        yield lurl[0], rurl[0]
                        old.remove(lurl[0])
                        new.remove(rurl[0])
                        left.remove(lurl[0])
                        right.remove(rurl[0])
            if len(left) == len(right) == 1:
                yield left[0], right[0]
                old.remove(left[0])
                new.remove(right[0])
        if len(old) == len(new) == 1:
            yield old[0], new[0]
        elif old and new:
            for scan, items in ((base, old), (head, new)):
                for item in items:
                    scan.limit("dynamic_route", item.location.line,
                               "Ambiguous route correspondence across snapshots")
        else:
            for item in old:
                yield item, None
            for item in new:
                yield None, item


def _literal(node: ast.AST | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _dotted(node.value) + "." + node.attr
    return ""


def _python(s: _Scan) -> None:
    try:
        tree = ast.parse(s.source)
    except (SyntaxError, ValueError, RecursionError):
        s.opaque = True
        s.limit("parse_error", 1, "Python source could not be parsed")
        return
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for a in node.names:
                aliases[a.asname or a.name] = a.name
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"

    def dotted(n: ast.AST) -> str:
        text = _dotted(n)
        first, sep, rest = text.partition(".")
        return aliases.get(first, first) + (sep + rest if sep else "")

    def kw(call: ast.Call, name: str) -> ast.AST | None:
        return next((k.value for k in call.keywords if k.arg == name), None)

    # receiver -> (framework, constructor prefix); None is an unresolved prefix.
    receivers: dict[str, tuple[str, str | None]] = {}
    handlers = {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)) or not isinstance(node.value, ast.Call):
            continue
        target = node.targets[0] if isinstance(node, ast.Assign) and len(node.targets) == 1 else (
            node.target if isinstance(node, ast.AnnAssign) else None)
        if not isinstance(target, ast.Name):
            continue
        call = node.value
        constructor = dotted(call.func)
        if constructor in {"fastapi.FastAPI", "fastapi.APIRouter", "flask.Flask", "flask.Blueprint"}:
            framework = "flask" if constructor.startswith("flask.") else "fastapi"
            prefix_node = kw(call, "url_prefix" if framework == "flask" else "prefix")
            receivers[target.id] = (framework, "" if prefix_node is None else _literal(prefix_node))
    # Invalidation is intentionally file-wide; temporal assignment analysis is not supported.
    for node in ast.walk(tree):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(
            node, (ast.AnnAssign, ast.AugAssign)) else []
        for target in targets:
            if isinstance(target, ast.Name) and target.id in receivers:
                if not (node in tree.body and isinstance(getattr(node, "value", None), ast.Call)
                        and dotted(node.value.func) in {"fastapi.FastAPI", "fastapi.APIRouter", "flask.Flask", "flask.Blueprint"}):
                    framework, _ = receivers[target.id]
                    receivers[target.id] = (framework, None)
    handled: set[int] = set()
    mounts: dict[str, list[tuple[str, str | None, bool]]] = {}
    for node in tree.body:
        call = node.value if isinstance(node, ast.Expr) else None
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
            continue
        if call.func.attr not in {"include_router", "register_blueprint"}:
            continue
        handled.add(id(call))
        parent = _dotted(call.func.value)
        child = _dotted(call.args[0]) if call.args else ""
        framework = "fastapi" if call.func.attr == "include_router" else "flask"
        if (parent not in receivers or child not in receivers
                or any(receivers[name][0] != framework for name in (parent, child))):
            s.limit("dynamic_route", call.lineno, "Unresolved Python router mount")
            if child in receivers:
                receivers[child] = (receivers[child][0], None)
            continue
        prefix_node = kw(call, "url_prefix" if call.func.attr == "register_blueprint" else "prefix")
        prefix = "" if prefix_node is None else _literal(prefix_node)
        override = call.func.attr == "register_blueprint" and prefix_node is not None
        mounts.setdefault(child, []).append((parent, prefix, override))

    def prefixes(receiver: str, seen: frozenset[str] = frozenset()) -> list[str] | None:
        if receiver in seen or receiver not in receivers or receivers[receiver][1] is None:
            return None
        own = receivers[receiver][1] or ""
        result = []
        for parent, extra, override in mounts.get(receiver, []):
            roots = prefixes(parent, seen | {receiver})
            if roots is None or extra is None:
                return None
            result.extend(root + extra + ("" if override else own) for root in roots)
        return result or [own]

    def route(call: ast.Call, handler: ast.AST | None, handler_name: str, ordinal: int) -> None:
        handled.add(id(call))
        assert isinstance(call.func, ast.Attribute)
        receiver, method = _dotted(call.func.value), call.func.attr
        framework = receivers[receiver][0] if receiver in receivers else None
        supported = ({"get", "post", "put", "patch", "delete", "head", "options", "trace",
                      "route", "api_route", "add_api_route"} if framework == "fastapi" else
                     {"get", "post", "put", "patch", "delete", "route", "add_url_rule"})
        if framework is None or method not in supported:
            s.limit("dynamic_route", call.lineno, "Unsupported Python framework route method")
            return
        roots = prefixes(receiver)
        path_node = call.args[0] if call.args else kw(call, "path" if method != "add_url_rule" else "rule")
        path = _literal(path_node)
        methods_node = kw(call, "methods")
        methods = [method.upper()] if method in _METHODS else ["GET"]
        if methods_node is not None:
            methods = [_literal(x) for x in methods_node.elts] if isinstance(
                methods_node, (ast.List, ast.Tuple, ast.Set)) else []
        if roots is None or path is None or not methods or any(m is None for m in methods) or handler is None:
            s.limit("dynamic_route", call.lineno, "Unresolved Python route, prefix, methods or handler")
            return
        framework = receivers[receiver][0]
        for root in roots:
            for m in methods:
                s.route(f"{receiver}:{handler_name}:{ordinal}:{m}", framework, m, root + path,
                        handler_name, ast.dump(handler, include_attributes=False), call.lineno)

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for i, deco in enumerate(node.decorator_list):
                if isinstance(deco, ast.Call) and isinstance(deco.func, ast.Attribute) and deco.func.attr in _METHODS | {"route", "api_route"}:
                    route(deco, node, node.name, i)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr in {"add_api_route", "add_url_rule"}:
                handler = kw(call, "endpoint" if call.func.attr == "add_api_route" else "view_func")
                if handler is None and len(call.args) > (1 if call.func.attr == "add_api_route" else 2):
                    handler = call.args[1 if call.func.attr == "add_api_route" else 2]
                name = _dotted(handler) if handler else ""
                route(call, handlers.get(name), name, 0)

    env_bindings = {"os"} | {name for name, target in aliases.items()
                             if target in {"os", "os.getenv", "os.environ"}}
    invalid_env = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)) and node.id in env_bindings:
            invalid_env.add(node.id)
        elif isinstance(node, ast.arg) and node.arg in env_bindings:
            invalid_env.add(node.arg)
    parents = {id(child): parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Attribute, ast.Name)) and dotted(node) == "os.environ":
            parent = parents.get(id(node))
            if not ((isinstance(parent, ast.Subscript) and parent.value is node)
                    or (isinstance(parent, ast.Attribute) and parent.attr == "get")):
                s.limit("dynamic_env", node.lineno, "Aliased or unsupported Python environment access")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and id(node) not in handled:
            if node.func.attr in _METHODS | {"route", "api_route", "add_api_route", "add_url_rule", "include_router", "register_blueprint"}:
                # Even unknown/local receivers are evidence of unsupported routing syntax.
                if dotted(node.func) != "os.environ.get":
                    s.limit("dynamic_route", node.lineno, "Route-like call outside the module-level allowlist")
        env_node: ast.AST | None = None
        is_env = False
        if isinstance(node, ast.Call) and dotted(node.func) in {"os.getenv", "os.environ.get"}:
            is_env = True
            env_node = node.args[0] if node.args else kw(node, "key")
        elif isinstance(node, ast.Subscript) and dotted(node.value) == "os.environ":
            if isinstance(node.ctx, ast.Load) or any(isinstance(p, ast.AugAssign) and p.target is node for p in ast.walk(tree)):
                is_env, env_node = True, node.slice
        if is_env:
            receiver = node.func if isinstance(node, ast.Call) else node.value
            if _dotted(receiver).split(".")[0] in invalid_env:
                s.limit("dynamic_env", node.lineno, "Shadowed or reassigned Python environment binding")
                continue
            name = _literal(env_node)
            if name is None:
                s.limit("dynamic_env", node.lineno, "Nonliteral Python environment variable name")
            else:
                s.env(name, node.lineno)


@dataclass(frozen=True)
class _Token:
    text: str
    line: int
    string: bool = False


# Strings and regexes are atomic; their contents are never scanned as code.
# A slash is classified from a closed set of preceding tokens, not a greedy
# regex alternative. Ambiguous contexts reject the file before receiver analysis.
_JS_TOKEN = re.compile(
    r"(?P<space>\s+)|(?P<comment>//[^\n]*|/\*[\s\S]*?\*/)|"
    r"(?P<string>'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\")|"
    r"(?P<template>`(?:\\.|[^`\\])*`)|"
    r"(?P<number>0[xX][0-9a-fA-F]+|0[bB][01]+|0[oO][0-7]+|(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?n?)|"
    r"(?P<word>[A-Za-z_$][\w$]*)|"
    r"(?P<op>===|!==|<<=|>>>?=|\*\*=|&&=|\|\|=|\?\?=|=>|==|!=|<=|>=|\+\+|--|[+*/%&|^?-]=|\?\.|.)",
    re.DOTALL,
)
_REGEX_START = {"=", "(", "[", ",", ":", "return", "=>", "!", "?", ";", "{"}
_JS_KEYWORDS = set("""await break case catch class const continue debugger default
    delete do else enum export extends false finally for function if implements
    import in instanceof interface let new null of package private protected public
    return static super switch this throw true try typeof var void while with yield
    async as satisfies""".split())


def _tokens(s: _Scan) -> list[_Token] | None:
    result: list[_Token] = []
    line, position = 1, 0
    previous_kind = ""

    def unsupported(message: str) -> None:
        s.opaque = True
        for code in ("dynamic_route", "dynamic_env"):
            s.limit(code, line, message)

    while position < len(s.source):
        match = _JS_TOKEN.match(s.source, position)
        assert match is not None
        text, kind = match.group(), match.lastgroup
        if kind == "template" and "${" in text:
            unsupported("Template substitutions are outside the lexical allowlist")
            return None
        if (text in {"'", '"', "`"}
                or (s.source.startswith("/*", position) and kind != "comment")):
            unsupported("Unterminated JavaScript literal or comment")
            return None
        if text in {"/", "/="}:
            previous = result[-1].text if result else ""
            if (previous == "return" and len(result) > 1
                    and result[-2].text in {".", "?."}):
                unsupported("Keyword property before slash is outside the lexical allowlist")
                return None
            if not result or previous in _REGEX_START:
                regex = re.match(r"/(?:\\.|\[(?:\\.|[^\]\\])*\]|[^/\\\n])+/[a-z]*", s.source[position:])
                if regex is None:
                    unsupported("Unresolved JavaScript regex literal")
                    return None
                text = regex.group()
                result.append(_Token("<regex>", line))
                previous_kind = "regex"
                position += len(text)
                line += text.count("\n")
                continue
            if not (previous_kind in {"string", "number", "regex"}
                    or previous == "]"
                    or (previous_kind == "word" and previous not in _JS_KEYWORDS)):
                unsupported("Ambiguous JavaScript slash outside the lexical allowlist")
                return None
        if kind not in {"space", "comment"}:
            result.append(_Token(text, line, kind == "string"))
            previous_kind = kind
        line += text.count("\n")
        position = match.end()
    return result


@dataclass
class _Binding:
    name: str
    scope: int
    start: int
    kind: str  # app/router/handler/other
    body: str = ""
    invalid: bool = False


def _javascript(s: _Scan) -> None:
    t = _tokens(s)
    if t is None:
        return
    n = len(t)
    text = [x.text for x in t]
    pairs: dict[int, int] = {}
    stack: list[int] = []
    scopes, parents = [], {0: -1}
    scope_stack = [0]
    for i, token in enumerate(t):
        scopes.append(scope_stack[-1])
        if token.text in {"(", "[", "{"}:
            stack.append(i)
            if token.text == "{":
                parents[i + 1] = scope_stack[-1]
                scope_stack.append(i + 1)
        elif token.text in {")", "]", "}"}:
            if not stack or text[stack[-1]] != {")": "(", "]": "[", "}": "{"}[token.text]:
                s.opaque = True
                s.limit("parse_error", token.line, "Unbalanced JavaScript delimiters")
                return
            opening = stack.pop()
            pairs[opening], pairs[i] = i, opening
            if token.text == "}":
                scope_stack.pop()
    if stack:
        s.opaque = True
        s.limit("parse_error", t[stack[0]].line, "Unbalanced JavaScript delimiters")
        return

    def lit(a: int, b: int) -> str | None:
        if b != a + 1 or not t[a].string:
            return None
        value = text[a][1:-1]
        # Escapes require JS evaluation semantics; do not guess them.
        return value if "\\" not in value else None

    def split(a: int, b: int, separator: str = ",") -> list[tuple[int, int]]:
        result, start = [], a
        while a < b:
            if text[a] in {"(", "[", "{"}:
                a = pairs[a] + 1
                continue
            if text[a] == separator:
                result.append((start, a))
                start = a + 1
            a += 1
        if start < b:
            result.append((start, b))
        return result

    def normalized(a: int, b: int) -> str:
        return repr(text[a:b])

    def end_expr(a: int) -> int:
        while a < n:
            if text[a] in {";", "}"}:
                return a
            if text[a] in {"(", "[", "{"}:
                a = pairs[a] + 1
            else:
                if a > 0 and t[a].line > t[a - 1].line and text[a] in {"export", "const", "let", "var", "function"}:
                    return a
                a += 1
        return n

    bindings: dict[tuple[int, str], _Binding] = {}
    decls: set[int] = set()

    def resolve(name: str, scope: int) -> _Binding | None:
        while scope >= 0:
            if (scope, name) in bindings:
                return bindings[(scope, name)]
            scope = parents[scope]
        return None

    def function_end(a: int) -> int | None:
        # Ordinary function declaration/expression, with optional async/name.
        if a < n and text[a] == "async":
            a += 1
        if a >= n or text[a] != "function":
            return None
        a += 1
        if a < n and re.fullmatch(r"[\w$]+", text[a]):
            a += 1
        if a >= n or text[a] != "(":
            return None
        a = pairs[a] + 1
        if a < n and text[a] == "{":
            return pairs[a] + 1
        return None

    def handler_expr(a: int, b: int) -> bool:
        if function_end(a) == b:
            return True
        cursor = a + 1 if a < b and text[a] == "async" else a
        if cursor < b and text[cursor] == "(":
            cursor = pairs[cursor] + 1
        elif cursor < b and re.fullmatch(r"[A-Za-z_$][\w$]*", text[cursor]):
            cursor += 1
        else:
            return False
        if cursor + 1 >= b or text[cursor] != "=>":
            return False
        return text[cursor + 1] != "{" or pairs[cursor + 1] == b - 1

    for i in range(n):
        if text[i] == "function" and i + 2 < n and text[i + 2] == "(":
            end = function_end(i)
            if end:
                start = (i - 1 if i and text[i - 1] == "async"
                         and t[i - 1].line == t[i].line else i)
                bindings[(scopes[i], text[i + 1])] = _Binding(text[i + 1], scopes[i], i, "handler", normalized(start, end))
                decls.add(i + 1)
        if text[i] not in {"const", "let", "var"} or i + 1 >= n:
            continue
        end = end_expr(i + 1)
        parts = split(i + 1, end)
        multiple = len(parts) != 1
        for a, b in parts:
            if a >= b:
                continue
            if text[a] in {"{", "["}:
                s.limit("dynamic_env", t[a].line, "Destructured bindings are outside the environment allowlist")
                for j in range(a + 1, min(pairs[a], b)):
                    if re.fullmatch(r"[\w$]+", text[j]):
                        bindings[(scopes[i], text[j])] = _Binding(text[j], scopes[i], j, "other", invalid=True)
                continue
            name = text[a]
            kind, body = "other", ""
            if a + 1 < b and text[a + 1] == "=":
                rhs = text[a + 2:b]
                if rhs == ["express", "(", ")"]:
                    kind = "app"
                elif rhs in (["express", ".", "Router", "(", ")"], ["Router", "(", ")"]):
                    kind = "router"
                elif handler_expr(a + 2, b):
                    kind, body = "handler", normalized(a + 2, b)
            unsupported = multiple or (text[i] == "var" and scopes[i] != 0)
            bindings[(scopes[i], name)] = _Binding(name, scopes[i], a, kind, body, unsupported)
            if unsupported and not multiple:
                s.limit("dynamic_route", t[a].line, "Nested var bindings are outside the lexical-scope allowlist")
            decls.add(a)
            if multiple:
                s.limit("dynamic_route", t[a].line, "Multiple declarators are outside the route allowlist")

    def shadow_parameters(scope: int, args: list[tuple[int, int]]) -> None:
        # A pattern can bind receivers through nested destructuring, aliases,
        # rest, defaults or TS annotations. Conservatively shadow every name;
        # unsupported patterns must never fall through to an outer receiver.
        for a, b in args:
            for j in range(a, b):
                if re.fullmatch(r"[A-Za-z_$][\w$]*", text[j]):
                    bindings[(scope, text[j])] = _Binding(text[j], scope, j, "other")

    # Parameter scopes include methods; control-flow parentheses do not bind names.
    expression_shadow: dict[int, set[str]] = {}
    for i in range(n):
        if text[i] == "=>":
            body = i + 1
            if body < n and text[body] == "{":
                args = []
                if i and text[i - 1] == ")":
                    args = split(pairs[i - 1] + 1, i - 1)
                elif i:
                    args = [(i - 1, i)]
                shadow_parameters(body + 1, args)
            elif body < n:
                opening = pairs[i - 1] if i and text[i - 1] == ")" else i - 1
                names = {name for name in text[max(0, opening):i]
                         if re.fullmatch(r"[A-Za-z_$][\w$]*", name)}
                for index in range(body, end_expr(body)):
                    expression_shadow.setdefault(index, set()).update(names)
        if (text[i] == "(" and pairs[i] + 1 < n and text[pairs[i] + 1] == "{"
                and (not i or text[i - 1] not in {"if", "while", "for", "switch", "with"})):
            # Covers ordinary functions, class/object methods (also computed
            # names, getters/setters) and catch bindings without resolving calls.
            shadow_parameters(pairs[i] + 2, split(i + 1, pairs[i]))

    # Imports may shadow global process or receiver identifiers. No alias inference.
    for i in range(n):
        if text[i] == "import":
            end = end_expr(i + 1)
            for j in range(i + 1, end):
                if text[j] == "from" or t[j].string:
                    break
                if re.fullmatch(r"[A-Za-z_$][\w$]*", text[j]):
                    bindings[(scopes[i], text[j])] = _Binding(text[j], scopes[i], j, "other")

    assignments = {"=", "+=", "-=", "*=", "/=", "%=", "**=", "<<=", ">>=", ">>>=", "&=", "|=", "^=", "&&=", "||=", "??=", "++", "--"}
    process_invalid: set[int] = set()
    for i in range(n):
        if not re.fullmatch(r"[A-Za-z_$][\w$]*", text[i]) or i in decls:
            continue
        if i and text[i - 1] in {".", "?."}:
            continue
        assigned = (i + 1 < n and text[i + 1] in assignments) or (i and text[i - 1] in {"++", "--", "delete"})
        cursor = i + 1
        while cursor < n:
            if text[cursor] in {".", "?."} and cursor + 1 < n:
                cursor += 2
            elif text[cursor] == "[":
                cursor = pairs[cursor] + 1
            else:
                break
        if cursor > i + 1 and cursor < n and text[cursor] in assignments:
            assigned = True
        # Destructuring assignment targets: conservatively invalidate every name
        # inside a pattern followed by an assignment operator.
        for opening, closing in pairs.items():
            if opening < i < closing and text[opening] in {"[", "{"} and closing + 1 < n and text[closing + 1] in assignments:
                assigned = True
        if assigned:
            binding = resolve(text[i], scopes[i])
            if binding:
                binding.invalid = True
            elif text[i] == "process":
                process_invalid.add(0)

    for i in range(n - 2):
        if text[i + 1] == "[":
            binding = resolve(text[i], scopes[i])
            if binding and binding.kind in {"app", "router"}:
                s.limit("dynamic_route", t[i].line, "Computed Express receiver access is unsupported")
                binding.invalid = True
        if text[i] == "process" and text[i + 1] in {"[", "?."}:
            s.limit("dynamic_env", t[i].line, "Computed or optional process access is unsupported")

    mounts: dict[int, list[tuple[_Binding, str | None]]] = {}
    route_calls: list[tuple[int, _Binding | None, str, list[tuple[int, int]]]] = []
    chained_receivers: set[int] = set()
    for i in range(n - 3):
        if i in chained_receivers:
            continue
        if i and text[i - 1] in {".", "?."}:
            continue
        if text[i + 1] not in {".", "?."} or text[i + 2] not in _METHODS | {"route", "use"}:
            continue
        method = text[i + 2]
        binding = None if text[i] in expression_shadow.get(i, set()) else resolve(text[i], scopes[i])
        if text[i + 1] == "?.":
            if binding:
                binding.invalid = True
            s.limit("dynamic_route", t[i].line, "Optional route calls are unsupported")
            continue
        if text[i + 3] != "(":
            s.limit("dynamic_route", t[i].line, "Generic or computed route call outside the allowlist")
            continue
        closing = pairs[i + 3]
        args = split(i + 4, closing)
        if method == "use":
            router_args = []
            for a, b in args:
                candidates = [resolve(text[j], scopes[j]) for j in range(a, b) if not t[j].string]
                candidates = [x for x in candidates if x and x.kind == "router"]
                for candidate in candidates:
                    router_args.append(candidate)
                    if b != a + 1:
                        candidate.invalid = True
            if router_args:
                prefix = lit(*args[0]) if args else None
                # app.use(router) is the static empty-prefix form.
                if len(args) == 1 and args[0][1] == args[0][0] + 1:
                    prefix = ""
                for router in router_args:
                    if binding is None or binding.kind not in {"app", "router"} or binding.invalid:
                        router.invalid = True
                    else:
                        mounts.setdefault(router.start, []).append((binding, prefix))
                if prefix is None:
                    s.limit("dynamic_route", t[i].line, "Nonliteral Express mount prefix")
            else:
                s.limit("dynamic_route", t[i].line, "Unresolved Express mount or middleware")
            continue
        if method == "route":
            cursor = closing + 1
            while cursor + 3 < n and text[cursor] == "." and text[cursor + 1] in _METHODS and text[cursor + 2] == "(":
                chained_receivers.add(cursor - 1)
                close = pairs[cursor + 2]
                route_calls.append((i, binding, text[cursor + 1], args + split(cursor + 3, close)))
                cursor = close + 1
            if cursor == closing + 1:
                s.limit("dynamic_route", t[i].line, "Unresolved chained Express route")
        else:
            route_calls.append((i, binding, method, args))

    def prefixes(binding: _Binding, seen: frozenset[int] = frozenset()) -> list[str] | None:
        if binding.invalid or binding.start in seen:
            return None
        result = []
        for parent, prefix in mounts.get(binding.start, []):
            roots = prefixes(parent, seen | {binding.start})
            if roots is None or prefix is None:
                return None
            result.extend(root + prefix for root in roots)
        return result or [""]

    for i, binding, method, args in route_calls:
        roots = prefixes(binding) if binding and binding.kind in {"app", "router"} else None
        path = lit(*args[0]) if args else None
        bodies, names = [], []
        valid = True
        for a, b in args[1:]:
            if b == a + 1:
                handler = resolve(text[a], scopes[i])
                if handler is None or handler.kind != "handler" or handler.invalid:
                    valid = False
                else:
                    bodies.append(handler.body)
                    names.append(handler.name)
            elif handler_expr(a, b):
                bodies.append(normalized(a, b))
                names.append("<inline>")
            else:
                valid = False
        if roots is None or path is None or not bodies or not valid:
            s.limit("dynamic_route", t[i].line, "Unresolved Express receiver, prefix, path or handler")
            continue
        for prefix in roots:
            # Enclosing block headers survive unrelated sibling insertions.
            # Identical anonymous blocks share a matching group: endpoints are
            # matched first, and ambiguous unmatched routes remain limitations.
            lineage = []
            scope = binding.scope
            while scope:
                opening = scope - 1
                start = opening - 1
                while start >= 0 and text[start] not in {";", "{", "}"}:
                    if text[start] in {")", "]"}:
                        start = pairs[start] - 1
                    else:
                        start -= 1
                lineage.append(normalized(start + 1, opening))
                scope = parents[scope]
            s.route(f"express:{lineage}:{binding.name}:{method}:{','.join(names)}", "express", method,
                    prefix + path, ",".join(names), repr(bodies), t[i].line, match_path=path)

    _next(s, t, pairs, bindings, handler_expr, function_end, end_expr, normalized)

    for i in range(n - 2):
        if i and text[i - 1] in {".", "?."}:
            continue
        if text[i:i + 3] != ["process", ".", "env"]:
            continue
        binding = resolve("process", scopes[i])
        if binding is not None or process_invalid or "process" in expression_shadow.get(i, set()):
            s.limit("dynamic_env", t[i].line, "Shadowed or reassigned process binding")
            continue
        a = i + 3
        name = None
        end = a
        if a + 1 < n and text[a] == "." and re.fullmatch(r"[A-Za-z_$][\w$]*", text[a + 1]):
            name, end = text[a + 1], a + 2
        elif a < n and text[a] == "[":
            name, end = lit(a + 1, pairs[a]), pairs[a] + 1
        # Only direct reads are allowlisted, not methods, aliases or deeper access.
        if end < n and (text[end] in {".", "?.", "[", "("} or text[end].startswith("`")):
            name = None
        # Writes alone are not reads; compound assignments are reads.
        if end < n and text[end] == "=":
            continue
        if name is None:
            s.limit("dynamic_env", t[i].line, "Nonliteral or aliased JavaScript environment read")
        else:
            s.env(name, t[i].line)


def _next(s, tokens, pairs, bindings, handler_expr, function_end, end_expr, normalized):
    text = [x.text for x in tokens]
    path = s.path.removeprefix("src/")
    app = re.fullmatch(r"app/api/(?:(.*)/)?route\.(?:js|jsx|ts|tsx)", path)
    pages = re.fullmatch(r"pages/api/(.+)\.(?:js|jsx|ts|tsx)", path)
    if not app and not pages:
        return
    suffix = (app.group(1) or "") if app else pages.group(1)
    if app:
        suffix = "/".join(part for part in suffix.split("/")
                          if not re.fullmatch(r"\([^/]+\)", part))
    if pages:
        suffix = re.sub(r"(?:^|/)index$", "", suffix)
    route_path = "/api" + ("/" + suffix if suffix else "")
    exported = False
    n = len(text)
    for i in range(n):
        if text[i] != "export":
            continue
        a = i + 1
        if pages:
            if a >= n or text[a] != "default":
                continue
            a += 1
            end = function_end(a)
            if end:
                body, name = normalized(a, end), "default"
            else:
                end = end_expr(a)
                if end == a + 1:
                    binding = bindings.get((0, text[a]))
                    body = binding.body if binding and binding.kind == "handler" and not binding.invalid else ""
                else:
                    body = normalized(a, end) if handler_expr(a, end) else ""
                name = "default"
            if body:
                s.route("next:default", "next-pages", "ALL", route_path, name, body, tokens[i].line)
                exported = True
            else:
                s.limit("dynamic_route", tokens[i].line, "Unresolved Next pages default handler")
        else:
            if a < n and text[a] == "async":
                a += 1
            if a + 1 < n and text[a] in {"function", "const", "let", "var"}:
                name = text[a + 1]
                if name not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}:
                    if name.lower() in _METHODS:
                        s.limit("dynamic_route", tokens[i].line, "Unsupported Next app handler export name")
                    continue
                binding = bindings.get((0, name))
                if binding and binding.kind == "handler" and not binding.invalid:
                    s.route(f"next:{name}", "next-app", name, route_path, name, binding.body, tokens[i].line)
                    exported = True
                else:
                    s.limit("dynamic_route", tokens[i].line, "Unresolved Next app handler binding")
            elif a < n and text[a] in {"{", "*"}:
                s.limit("dynamic_route", tokens[i].line, "Next handler re-exports are unsupported")
    if not exported and not any(x.code == "dynamic_route" for x in s.limitations):
        s.limit("dynamic_route", 1, "No statically resolvable Next API handler export")
