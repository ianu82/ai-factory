"""Snapshot fixtures are source data: none of these frameworks are imported."""
from dataclasses import asdict

import pytest

from aifactory.semantic_extract import extract


ROUTES = [
    ("api.py", "from fastapi import FastAPI\napp = FastAPI()\n@app.get('/x')\ndef handler():\n    return 1\n", "fastapi"),
    ("api.py", "from flask import Flask\napp = Flask(__name__)\n@app.route('/x')\ndef handler():\n    return 1\n", "flask"),
    ("api.js", "const app = express();\napp.get('/x', (req, res) => { res.send(1); });", "express"),
    ("app/api/x/route.ts", "export function GET() { return 1; }", "next-app"),
    ("src/app/api/[id]/route.js", "export const GET = function() { return 1; };", "next-app"),
    ("pages/api/x.ts", "export default function handler() { return 1; }", "next-pages"),
    ("src/pages/api/[...slug].js", "const handler = function(req, res) { res.send(1); };\nexport default handler;", "next-pages"),
    ('app/api/[id]/route.ts', 'export const GET = async () => { return 1; };', 'next-app'),
    ('src/app/api/x/route.js', 'const unused = 0; export async function POST() { return 1; }', 'next-app'),
    ('pages/api/index.js', 'export default (req,res) => { res.send(1); };', 'next-pages'),
    ('src/pages/api/x/index.ts', 'const handler = (req,res) => { res.send(1); }; export default handler;', 'next-pages'),
]

@pytest.mark.parametrize("path,source,framework", ROUTES)
@pytest.mark.parametrize("change", ["added", "removed", "changed"])
def test_route_snapshots(path, source, framework, change):
    before = {} if change == "added" else {path: source}
    after = {} if change == "removed" else {path: source.replace("return 1", "return 2").replace("send(1)", "send(2)")}
    result = extract([path], before, after)
    assert len(result.facts) == 1
    fact = result.facts[0]
    assert fact.kind == "route" and fact.change == change
    assert (fact.after or fact.before)["framework"] == framework
    if framework.startswith("next"):
        suffix = "/[...slug]" if "[...slug]" in path else "/[id]" if "[id]" in path else "" if path == "pages/api/index.js" else "/x"
        assert (fact.after or fact.before)["path"] == "/api" + suffix
    assert fact.locations[-1].side == ("base" if change == "removed" else "head")
    for loc in fact.locations:
        snapshot = before if loc.side == "base" else after
        assert loc.path == path and 1 <= loc.line <= len(snapshot[path].splitlines())
    assert extract([path], {path: source}, {path: source}).facts == ()
    comment = "# context\n" if path.endswith(".py") else "// context\n"
    assert not extract([path], {path: source}, {path: comment + source}).facts
    # Facts expose metadata, not code or fingerprints.
    assert "fingerprint" not in str(asdict(result))

@pytest.mark.parametrize("source", [
    "from fastapi import FastAPI\napp=FastAPI()\n@app.get('/x')\ndef f(): return 1",
    "from flask import Flask\napp=Flask(__name__)\n@app.route('/x')\ndef f(): return 1",
    "const app=express(); app.get('/x', function(req,res) { res.send(1); });",
])
def test_changed_route_path(source):
    path = "api.py" if source.startswith("from") else "api.js"
    result = extract([path], {path: source}, {path: source.replace("/x", "/y")})
    assert [(f.change, f.before["path"], f.after["path"]) for f in result.facts] == [("changed", "/x", "/y")]

@pytest.mark.parametrize("source,expected", [
    ("from fastapi import FastAPI, APIRouter\napp=FastAPI()\nr=APIRouter(prefix='/inner')\napp.include_router(r, prefix='/outer')\n@r.get('/x')\ndef f(): return 1", "/outer/inner/x"),
    ("from flask import Flask, Blueprint\napp=Flask(__name__)\nr=Blueprint('r', __name__, url_prefix='/inner')\napp.register_blueprint(r, url_prefix='/outer')\n@r.route('/x', methods=['POST'])\ndef f(): return 1", "/outer/x"),
    ("const app=express(); const r=express.Router(); app.use('/outer', auth, log, r); r.get('/x', (req,res)=>{res.send(1);});", "/outer/x"),
    ("const app=express(); const r=Router(); app.use(r); r.route('/x').get((req,res)=>{res.send(1);});", "/x"),
])
def test_static_prefixes(source, expected):
    path = "api.py" if source.startswith("from") else "api.ts"
    result = extract([path], {}, {path: source})
    assert len(result.facts) == 1
    assert result.facts[0].after["path"] == expected
    assert not result.limitations
    changed = extract([path], {path: source}, {path: source.replace("/outer", "/v2")})
    if "/outer" in source:
        assert len(changed.facts) == 1 and changed.facts[0].change == "changed"

@pytest.mark.parametrize("source", [
    "from fastapi import FastAPI\napp=FastAPI()\ndef f(): return 1\napp.add_api_route('/x', f)",
    "from flask import Flask\napp=Flask(__name__)\ndef f(): return 1\napp.add_url_rule('/x', view_func=f)",
    "const app=express(); const handler=function(req,res){res.send(1);}; app.get('/x', handler);",
    "const app=express(); function handler(req,res){res.send(1);} app.get('/x', handler);",
])
def test_registration_named_handlers(source):
    path = "api.py" if source.startswith("from") else "api.ts"
    result = extract([path], {path: source}, {path: source.replace("return 1", "return 2").replace("send(1)", "send(2)")})
    assert len(result.facts) == 1 and result.facts[0].change == "changed"

@pytest.mark.parametrize("framework,constructor,decorator", [
    ("flask", "Flask(__name__)", "route"),
    ("fastapi", "FastAPI()", "get"),
])
def test_application_factory_is_explicit_limitation(framework, constructor, decorator):
    source = f"from {framework} import {constructor.split('(')[0]}\ndef create_app():\n    app={constructor}\n    @app.{decorator}('/x')\n    def f(): return 1\n    return app\n"
    result = extract(["api.py"], {}, {"api.py": source})
    assert not result.facts
    assert any(l.code == "dynamic_route" and l.locations[0].line == 4 for l in result.limitations)

@pytest.mark.parametrize("call", ["app.get<P>('/x', h)", "app.route<P>('/x').get(h)"])
def test_typed_express_calls_are_not_silently_dropped(call):
    source = "const app=express(); const h=(req,res)=>{res.send(1);}; " + call
    for before, after in [({}, {"api.ts": source}), ({"api.ts": source}, {"api.ts": source.replace("/x", "/y")})]:
        result = extract(["api.ts"], before, after)
        assert not result.facts
        assert any(l.code == "dynamic_route" for l in result.limitations)

@pytest.mark.parametrize("source", [
    "from fastapi import FastAPI\napp=FastAPI()\n@app.get(PATH)\ndef f(): return 1",
    "from flask import Flask, Blueprint\napp=Flask(__name__)\nr=Blueprint('r',__name__,url_prefix=PREFIX)\napp.register_blueprint(r,url_prefix='/v1')\n@r.get('/x')\ndef f(): return 1",
    "from flask import Flask, Blueprint\napp=Flask(__name__)\nr=Blueprint('r',__name__,url_prefix='/r')\napp.register_blueprint(r,url_prefix=PREFIX)\n@r.route('/x')\ndef f(): return 1",
    "const app=express(); const r=Router(); app.use('/api'+version, auth, r); r.get('/x', ()=>{return 1;});",
    "const app=express(); const r=Router(); app.use('/api', flag ? r : other); r.get('/x', ()=>{return 1;});",
    "const app=express(); const r=Router(); app.use('/api', wrap(r)); r.get('/x', ()=>{return 1;});",
    "const app=express(); app.get(PATH, ()=>{return 1;});",
    "const app=express(); app.get('/x', unknown);",
    "export const GET=()=>{return 1;}, POST=()=>{return 2;};",
    "export { GET } from './handler';",
])
def test_unsupported_routes_never_guess(source):
    path = "api.py" if source.startswith("from") else "app/api/x/route.ts" if source.startswith("export") else "api.js"
    result = extract([path], {}, {path: source})
    assert not result.facts
    assert any(l.code == "dynamic_route" for l in result.limitations)

@pytest.mark.parametrize("operator", ["=", "+=", "-=", "*=", "/=", "%=", "**=", "<<=", ">>=", ">>>=", "&=", "|=", "^=", "&&=", "||=", "??=", "++", "--", "destructure"])
@pytest.mark.parametrize("receiver", ["app", "r", "process"])
def test_mutated_receivers_are_unresolved(operator, receiver):
    mutation = f"{receiver} {operator} other;" if operator not in {"++", "--", "destructure"} else f"{receiver}{operator};"
    if operator == "destructure":
        mutation = f"([{receiver}]=other);"
    source = "const app=express(); const r=Router(); " + mutation
    source += "process.env.SECRET;" if receiver == "process" else f"{receiver}.get('/x', ()=>{{return 1;}});"
    result = extract(["api.ts"], {}, {"api.ts": source})
    assert not result.facts
    assert any(l.code == ("dynamic_env" if receiver == "process" else "dynamic_route") for l in result.limitations)

@pytest.mark.parametrize("path,source,code", [
    ("api.js", "const app=express(); const f=app=>app.get('/no', ()=>1);", "dynamic_route"),
    ("api.js", "const router=Router(); const f=router=>router.get('/no', ()=>1);", "dynamic_route"),
    ('api.py', 'import os\nx=os.getenv(name)', 'dynamic_env'),
    ('api.py', "import os\nx=os.environ[prefix+'NAME']", 'dynamic_env'),
    ('api.ts', 'const x=process.env[name];', 'dynamic_env'),
    ('api.ts', "const x=process.env['PREFIX'+suffix];", 'dynamic_env'),
    ('api.ts', 'const x=process.env;', 'dynamic_env'),
    ('api.ts', 'const { NAME }=process.env;', 'dynamic_env'),
    ('api.ts', 'function f(process) { return process.env.NAME; }', 'dynamic_env'),
    ('api.ts', 'const f=(process)=>{return process.env.NAME;};', 'dynamic_env'),
    ('api.py', "import os\nos=other\nx=os.getenv('NO')", 'dynamic_env'),
    ('api.py', "from os import getenv\ngetenv=other\nx=getenv('NO')", 'dynamic_env'),
    ('api.py', "import os\ndef f(os): return os.environ['NO']", 'dynamic_env'),
    ('api.ts', "const app=express(); app['get']('/x', ()=>{return 1;});", 'dynamic_route'),
    ('api.ts', "const app=express(); app?.get('/x', ()=>{return 1;});", 'dynamic_route'),
    ('api.ts', "process['env']['NAME'];", 'dynamic_env'),
    ('api.ts', 'process?.env.NAME;', 'dynamic_env'),
    ('api.ts', "import process from 'other'; process.env.NAME;", 'dynamic_env'),
    ('api.py', 'import os\nenv=os.environ', 'dynamic_env'),
    ('api.py', "import os\nx=os.environ.pop('NAME')", 'dynamic_env'),
    ('api.ts', 'const f=process=>process.env.NAME;', 'dynamic_env'),
    ('api.ts', "const app=express(); app.get('/x', flag ? ()=>1 : other);", 'dynamic_route'),
    ('api.ts', "const app=express(); app.get = other; app.get('/x', ()=>1);", 'dynamic_route'),
    ('api.ts', "const app=express(); app['get'] = other; app.get('/x', ()=>1);", 'dynamic_route'),
    ('api.ts', "const app=express(); delete app; app.get('/x', ()=>1);", 'dynamic_route'),
    ('api.ts', 'const {A, B} = process.env;', 'dynamic_env'),
    ('api.ts', "const {A: alias, B = 'private-default'} = process.env;", 'dynamic_env'),
    ('api.ts', "const {['A']: alias} = process.env;", 'dynamic_env'),
    ('api.ts', 'const {...rest} = process.env;', 'dynamic_env'),
    ('api.ts', 'const env = process.env;', 'dynamic_env'),
    ('api.ts', "process['env'].NAME;", 'dynamic_env'),
    ('api.ts', 'process.env?.NAME;', 'dynamic_env'),
    ('api.ts', "process.env?.['NAME'];", 'dynamic_env'),
    ('api.ts', "process.env['NAME'].trim();", 'dynamic_env'),
    ('api.ts', 'process.env.NAME();', 'dynamic_env'),
    ('api.ts', 'process.env.NAME.other;', 'dynamic_env'),
    ('api.ts', "process.env.NAME['other'];", 'dynamic_env'),
    ('api.ts', 'process.env.NAME`text`;', 'dynamic_env'),
    ('app/api/x/route.ts', 'export function get(){return 1;}', 'dynamic_route'),
    ('app/api/x/route.ts', 'export function GeT(){return 1;}', 'dynamic_route'),
    ('app/api/x/route.ts', 'export function TRACE(){return 1;}', 'dynamic_route'),
    ('app/api/x/route.ts', 'export function all(){return 1;}', 'dynamic_route'),
    ('api.js', 'process.env = other; process.env.NAME;', 'dynamic_env'),
])
def test_unsupported_environment_and_receiver_forms(path, source, code):
    result = extract([path], {}, {path: source})
    assert not result.facts
    assert any(l.code == code for l in result.limitations)
    assert "private-default" not in str(asdict(result))


ENV = [
    ("api.py", "import os\nx=os.getenv('OLD', 'private-default')"),
    ("api.py", "import os\nx=os.environ.get('OLD', 'private-default')"),
    ("api.py", "import os\nx=os.environ['OLD']"),
    ("api.py", "import os\nos.environ['OLD'] += 'private-default'"),
    ("api.py", "from os import getenv as env\nx=env('OLD')"),
    ("api.py", "from os import environ as env\nx=env['OLD']"),
    ("api.ts", "const x=process.env.OLD || 'private-default';"),
    ("api.js", "const x=process.env['OLD'];"),
    ("api.ts", 'const x=process.env["OLD"];'),
]

@pytest.mark.parametrize("path,source", ENV)
def test_environment_add_remove_names_only(path, source):
    result = extract([path], {path: source}, {path: source.replace("OLD", "NEW")})
    assert {(f.change, f.name) for f in result.facts} == {("removed", "OLD"), ("added", "NEW")}
    assert all(f.kind == "env" for f in result.facts)
    assert "private-default" not in str(asdict(result))
    assert next(f for f in result.facts if f.change == "removed").locations[0].side == "base"
    assert not extract([path], {path: source}, {path: source + "\n# comment" if path.endswith("py") else source + "\n// comment"}).facts

@pytest.mark.parametrize("path,source", [
    ("api.py", "# os.getenv('NO')\ntext=\"os.environ['NO']\""),
    ("api.ts", "// process.env.NO\n/* process.env.NO */ const text=\"process.env.NO\";"),
    ("api.py", "import os\nos.environ['NO']='value'"),
    ("api.js", "process.env.NO='value';"),
])
def test_comments_strings_and_writes_are_not_reads(path, source):
    result = extract([path], {}, {path: source})
    assert not result.facts


def test_express_scopes_do_not_share_mounts_or_receiver_mutation():
    source = """const app=express();
{ const r=Router(); app.use('/one', r); r.get('/x', ()=>{return 1;}); }
{ const r=Router(); app.use('/two', auth, log, r); r.get('/y', ()=>{return 2;}); }
"""
    result = extract(["api.ts"], {}, {"api.ts": source})
    assert {f.after["path"] for f in result.facts} == {"/one/x", "/two/y"}
    changed = source.replace("return 2", "return 3")
    result = extract(["api.ts"], {"api.ts": source}, {"api.ts": changed})
    assert len(result.facts) == 1 and result.facts[0].name == "GET /two/y"
    invalid = source.replace("app.use('/one', r);", "r=other; app.use('/one', r);")
    result = extract(["api.ts"], {}, {"api.ts": invalid})
    assert [f.name for f in result.facts] == ["GET /two/y"]
    assert any(l.code == "dynamic_route" for l in result.limitations)


def test_changed_paths_only_and_no_io(tmp_path):
    existing = set(tmp_path.rglob("*"))
    malicious = "raise RuntimeError('must never execute')\nimport os\nx=os.getenv('NAME')"
    result = extract(["api.py"], {}, {"api.py": malicious, "other.py": malicious})
    assert len(result.facts) == 1
    assert set(tmp_path.rglob("*")) == existing

@pytest.mark.parametrize("receiver,preamble", [
    ("app", "const app=express();"),
    ("r", "const app=express(); const r=Router(); app.use('/v1', auth, r);"),
])
@pytest.mark.parametrize("edit", ["delete", "insert", "body", "path", "reorder"])
def test_multiple_express_routes_match_endpoints_not_positions(receiver, preamble, edit):
    lines = [
        preamble,
        f"{receiver}.get('/users', (req,res)=>{{res.send(1);}});",
        f"{receiver}.post('/users', (req,res)=>{{res.send(1);}});",
        f"{receiver}.get('/health', (req,res)=>{{res.send(1);}});",
        f"{receiver}.get('/ready', (req,res)=>{{res.send(1);}});",
    ]
    updated = lines.copy()
    if edit == "delete":
        del updated[1]
    elif edit == "insert":
        updated.insert(1, f"{receiver}.get('/new', (req,res)=>{{res.send(1);}});")
    elif edit == "body":
        updated[1] = updated[1].replace("send(1)", "send(2)")
    elif edit == "path":
        updated[1] = updated[1].replace("/users", "/people")
    else:
        updated[1], updated[3] = updated[3], updated[1]
    result = extract(["api.ts"], {"api.ts": "\n".join(lines)}, {"api.ts": "\n".join(updated)})
    assert not result.limitations
    if edit == "reorder":
        assert not result.facts
        return
    assert len(result.facts) == 1
    fact = result.facts[0]
    prefix = "/v1" if receiver == "r" else ""
    expected_path = prefix + {"insert": "/new", "path": "/people"}.get(edit, "/users")
    assert fact.name == "GET " + expected_path
    assert fact.change == {"insert": "added", "delete": "removed"}.get(edit, "changed")
    assert fact.locations[0].line == 2
    assert fact.locations[0].side == ("head" if edit == "insert" else "base")
    if edit in {"body", "path"}:
        assert fact.before["path"] == prefix + "/users"
        assert fact.after["path"] == expected_path
        assert fact.locations[1].side == "head"


def test_ambiguous_multiple_route_path_edits_are_limitations_not_guesses():
    source = "const app=express();\napp.get('/a', ()=>1);\napp.get('/b', ()=>1);"
    result = extract(["api.js"], {"api.js": source}, {"api.js": source.replace("/a", "/c").replace("/b", "/d")})
    assert not result.facts
    assert len(result.limitations) == 4
    assert {loc.side for limit in result.limitations for loc in limit.locations} == {"base", "head"}
    assert all(limit.code == "dynamic_route" for limit in result.limitations)

@pytest.mark.parametrize("path,expected", [
    ("app/api/fooroute.ts", None),
    ("src/app/api/fooroute.ts", None),
    ("app/api/route.ts", "/api"),
    ("app/api/(admin)/users/route.ts", "/api/users"),
    ("src/app/api/(admin)/[id]/(group)/route.ts", "/api/[id]"),
    ("app/api/(admin)/route.ts", "/api"),
])
def test_next_exact_route_filename_and_route_groups(path, expected):
    source = "export function GET() {return 1;}"
    added = extract([path], {}, {path: source})
    removed = extract([path], {path: source}, {})
    if expected is None:
        assert not added.facts and not removed.facts
        return
    assert not added.limitations and not removed.limitations
    assert added.facts[0].name == "GET " + expected
    assert removed.facts[0].name == "GET " + expected
    assert removed.facts[0].change == "removed"
    assert removed.facts[0].locations[0].side == "base"


def test_chained_express_methods_have_no_spurious_limitations():
    source = "const app=express();\napp.route('/users').get(()=>1).post(()=>2).delete(()=>3);"
    result = extract(["api.js"], {}, {"api.js": source})
    assert not result.limitations
    assert {f.name for f in result.facts} == {"GET /users", "POST /users", "DELETE /users"}
    changed = extract(["api.js"], {"api.js": source}, {"api.js": source.replace("()=>2", "()=>4")})
    assert not changed.limitations
    assert [(f.change, f.name) for f in changed.facts] == [("changed", "POST /users")]


def test_multiple_express_routes_follow_static_mount_prefix_change():
    source = """const app=express(); const r=Router();
app.use('/v1', auth, log, r);
r.get('/users', ()=>1);
r.get('/health', ()=>1);
"""
    result = extract(["api.js"], {"api.js": source}, {"api.js": source.replace("/v1", "/v2")})
    assert not result.limitations
    assert {(f.change, f.before["path"], f.after["path"]) for f in result.facts} == {
        ("changed", "/v1/users", "/v2/users"),
        ("changed", "/v1/health", "/v2/health"),
    }

@pytest.mark.parametrize("expression", [
    "a / b; const k = process.env.OLD / 2",
    "a / b / process.env['OLD']",
    "123 / process.env.OLD",
    "'value' / process.env.OLD",
    "values[0] / process.env.OLD",
    "/literal/ / process.env.OLD",
])
def test_allowlisted_division_preserves_env_reads(expression):
    source = f"const r = {expression};"
    result = extract(["api.ts"], {"api.ts": source}, {"api.ts": source.replace("OLD", "NEW")})
    assert not result.limitations
    assert {(f.change, f.name) for f in result.facts} == {("removed", "OLD"), ("added", "NEW")}

@pytest.mark.parametrize("receiver,preamble,read", [
    ("app", "let app=express();", "app.get('/x', ()=>1);"),
    ("router", "let router=Router();", "router.get('/x', ()=>1);"),
    ("process", "", "process.env.NAME;"),
])
def test_division_cannot_hide_receiver_reassignment(receiver, preamble, read):
    source = f"{preamble} {read} const x = a / b; {receiver} = other; const y = c / d; {read}"
    result = extract(["api.js"], {}, {"api.js": source})
    assert not result.facts
    assert any(l.code == ("dynamic_env" if receiver == "process" else "dynamic_route")
               for l in result.limitations)

@pytest.mark.parametrize("unsupported", [
    "const x = f() / divisor;",
    "const x = obj.return / b; app = other; const y = c / d;",
    "const x = {} / divisor;",
    "if (ok) /process.env.HIDDEN/.test(value);",
    "const x = /unterminated;",
    "const x = 'unterminated;",
    "const x = `unterminated;",
    "/* unterminated comment",
    "const x = `${process.env.HIDDEN}`;",
    "const x = `${app = other}`;",
    "const x = `${router = other}`;",
    "const x = `${process = other}`;",
    "const x = `outer ${`nested ${process.env.HIDDEN}`} value`;",
])
@pytest.mark.parametrize("path", ["api.js", "app/api/x/route.ts", "pages/api/x.ts"])
def test_opaque_lexical_forms_fail_closed_for_both_snapshots(unsupported, path):
    source = "const app=express(); app.get('/x', ()=>1); process.env.NAME;\n"
    if path.startswith("app/"):
        source += "export function GET(){return 1;}\n"
    elif path.startswith("pages/"):
        source += "export default function handler(){return 1;}\n"
    for before, after, side in [(source, source + unsupported, "head"),
                                (source + unsupported, source, "base")]:
        result = extract([path], {path: before}, {path: after})
        assert not result.facts
        assert {l.code for l in result.limitations} == {"dynamic_route", "dynamic_env"}
        for limitation in result.limitations:
            assert limitation.locations[0].path == path
            assert limitation.locations[0].side == side
            assert 1 <= limitation.locations[0].line <= len((before if side == "base" else after).splitlines())
        assert "HIDDEN" not in str(asdict(result))

@pytest.mark.parametrize("source", [
    'const text = `process.env.NO app.get("/no", handler)`;',
    '// `${process.env.NO}`\nconst text="${process.env.NO}";',
    r'const re = /process\.env\.NO[\/]/g;',
    'const re = /=process.env.NO/;',
    "const re = /process.env.NO/; const another = /app.get('x', fn)/;",
    'const re = /["\'`]/;',
])
def test_plain_templates_comments_and_regexes_are_not_executable(source):
    result = extract(["api.js"], {}, {"api.js": source})
    assert not result.facts
    assert not result.limitations

@pytest.mark.parametrize("registration,expected", [
    ("", "/bp/x"),
    (", url_prefix='/v1'", "/v1/x"),
    (", url_prefix=''", "/x"),
])
def test_flask_registration_prefix_replaces_constructor_prefix(registration, expected):
    source = (
        "from flask import Flask, Blueprint\n"
        "app=Flask(__name__)\n"
        "bp=Blueprint('bp', __name__, url_prefix='/bp')\n"
        f"app.register_blueprint(bp{registration})\n"
        "@bp.route('/x')\n"
        "def handler(): return 1\n"
    )
    added = extract(["api.py"], {}, {"api.py": source})
    removed = extract(["api.py"], {"api.py": source}, {})
    assert not added.limitations and not removed.limitations
    assert [(f.change, f.name) for f in added.facts] == [("added", "GET " + expected)]
    assert [(f.change, f.name) for f in removed.facts] == [("removed", "GET " + expected)]
    assert removed.facts[0].locations[0].side == "base"
    updated = source.replace("app.register_blueprint(bp" + registration + ")",
                             "app.register_blueprint(bp, url_prefix='/v2')")
    changed = extract(["api.py"], {"api.py": source}, {"api.py": updated})
    assert not changed.limitations
    assert [(f.change, f.before["path"], f.after["path"]) for f in changed.facts] == [
        ("changed", expected, "/v2/x"),
    ]

@pytest.mark.parametrize("path,source", [
    ("app/api/x/route.ts", "export function GET(){return 1;}"),
    ("src/app/api/x/route.js", "export function GET(){return 1;}"),
    ("pages/api/x.ts", "function handler(){return 1;} export default handler;"),
    ("src/pages/api/x.js", "export default function handler(){return 1;}"),
    ("api.js", "const app=express(); function handler(){return 1;} app.get('/x', handler);"),
])
@pytest.mark.parametrize("remove_async", [False, True])
def test_named_handler_async_modifier_is_a_behaviour_change(path, source, remove_async):
    asynchronous = source.replace("function ", "async function ")
    before, after = (asynchronous, source) if remove_async else (source, asynchronous)
    result = extract([path], {path: before}, {path: after})
    assert not result.limitations
    assert len(result.facts) == 1
    fact = result.facts[0]
    assert fact.kind == "route" and fact.change == "changed"
    assert fact.before == fact.after
    assert [(loc.path, loc.line, loc.side) for loc in fact.locations] == [
        (path, 1, "base"), (path, 1, "head"),
    ]


def test_standalone_async_identifier_is_not_a_handler_modifier():
    source = "function handler(){return 1;} export default handler;"
    path = "pages/api/x.js"
    result = extract([path], {path: source}, {path: "async\n" + source})
    assert not result.facts
    assert not result.limitations

@pytest.mark.parametrize("path,source,broken", [
    ("api.py", "import os\nfrom fastapi import FastAPI\napp=FastAPI()\n@app.get('/x')\ndef f(): return os.getenv('NAME')\n", "def broken(:"),
    ("api.js", "const app=express(); app.get('/x', ()=>1); process.env.NAME;", "("),
    ("api.js", "const app=express(); app.get('/x', ()=>1); process.env.NAME;", ")"),
    ("app/api/x/route.ts", "export function GET(){return process.env.NAME;}", "{"),
    ("pages/api/x.ts", "export default function handler(){return process.env.NAME;}", "}"),
])
@pytest.mark.parametrize("broken_side", ["base", "head"])
def test_parse_error_snapshot_cannot_invent_route_or_env_changes(path, source, broken, broken_side):
    before, after = (source + broken, source) if broken_side == "base" else (source, source + broken)
    result = extract([path], {path: before}, {path: after})
    assert not result.facts
    assert {l.code for l in result.limitations} == {"parse_error"}
    assert all(l.locations[0].side == broken_side for l in result.limitations)
    assert "return process.env" not in str(asdict(result))

@pytest.mark.parametrize("framework,method", [
    ("fastapi", "all"), ("flask", "all"), ("flask", "trace"),
    ("flask", "head"), ("flask", "options"), ("flask", "api_route"),
])
def test_python_decorator_methods_are_framework_specific(framework, method):
    constructor = "FastAPI" if framework == "fastapi" else "Flask"
    args = "" if framework == "fastapi" else "__name__"
    source = f"from {framework} import {constructor}\napp={constructor}({args})\n@app.{method}('/x')\ndef f(): return 1\n"
    result = extract(["api.py"], {}, {"api.py": source})
    assert not result.facts
    assert {l.code for l in result.limitations} == {"dynamic_route"}
    assert result.limitations[0].locations[0].line == 3

@pytest.mark.parametrize("framework,methods", [
    ("fastapi", ["get", "post", "put", "patch", "delete", "head", "options", "trace", "route", "api_route"]),
    ("flask", ["get", "post", "put", "patch", "delete", "route"]),
])
def test_supported_python_decorator_methods(framework, methods):
    constructor = "FastAPI" if framework == "fastapi" else "Flask"
    for method in methods:
        source = f"from {framework} import {constructor}\napp={constructor}()\n@app.{method}('/x')\ndef f(): return 1\n"
        result = extract(["api.py"], {}, {"api.py": source})
        assert not result.limitations
        assert len(result.facts) == 1
        assert result.facts[0].name == ("GET" if method in {"route", "api_route"} else method.upper()) + " /x"

@pytest.mark.parametrize("receiver", ["app", "router"])
@pytest.mark.parametrize("parameter", ["{app}", "{nested: {app}}", "[app]", "{other: app}", "app"])
@pytest.mark.parametrize("function", [
    "function f(PARAM){BODY}",
    "const f=function(PARAM){BODY};",
    "const f=(PARAM)=>{BODY};",
    "const f=(PARAM)=>app.get('/no', ()=>1);",
    "class C { m(PARAM){BODY} }",
    "const object={ m(PARAM){BODY} };",
    "class C { static async m(PARAM){BODY} }",
    "const object={ ['m'](PARAM){BODY} };",
])
def test_parameter_patterns_never_resolve_to_outer_receiver(parameter, function, receiver):
    source = "const app=express(); " + function.replace("PARAM", parameter).replace("BODY", "app.get('/no', ()=>1);")
    source += " app.get('/yes', ()=>1);"
    source = source.replace("app", receiver)
    result = extract(["api.js"], {}, {"api.js": source})
    assert [f.name for f in result.facts] == ["GET /yes"]
    assert any(l.code == "dynamic_route" for l in result.limitations)

@pytest.mark.parametrize("function", [
    "function f({process}){return process.env.NO;}",
    "const f=function({nested: {process}}){return process.env.NO;};",
    "const f=([process])=>{return process.env.NO;};",
    "const f=({process})=>process.env.NO;",
    "class C { m(process){return process.env.NO;} }",
    "const object={ m({process}){return process.env.NO;} };",
])
def test_destructured_parameters_shadow_global_process(function):
    result = extract(["api.ts"], {}, {"api.ts": function + " process.env.YES;"})
    assert [f.name for f in result.facts] == ["YES"]
    assert any(l.code == "dynamic_env" for l in result.limitations)

@pytest.mark.parametrize("wrapper", ["{ BODY }", "if (enabled) { BODY }", "function configure() { BODY }"])
def test_express_scope_identity_survives_unrelated_sibling_insertion(wrapper):
    source = wrapper.replace("BODY", "const app=express(); app.get('/x', ()=>1);")
    sibling = "{ const unrelated=1; }\n"
    unchanged = extract(["api.js"], {"api.js": source}, {"api.js": sibling + source})
    assert not unchanged.facts and not unchanged.limitations
    changed = extract(["api.js"], {"api.js": source}, {"api.js": sibling + source.replace("()=>1", "()=>2")})
    assert not changed.limitations
    assert [(f.change, f.name) for f in changed.facts] == [("changed", "GET /x")]
    assert [loc.side for loc in changed.facts[0].locations] == ["base", "head"]


def test_anonymous_sibling_routers_match_distinct_mounted_urls():
    source = """const app=express();
{ const r=Router(); app.use('/one', r); r.get('/x', ()=>1); }
{ const r=Router(); app.use('/two', r); r.get('/x', ()=>2); }
"""
    for updated, expected in [
        ("{ const unrelated=1; }\n" + source, []),
        (source.replace("()=>2", "()=>3"), [("changed", "GET /two/x")]),
    ]:
        result = extract(["api.js"], {"api.js": source}, {"api.js": updated})
        assert not result.limitations
        assert [(f.change, f.name) for f in result.facts] == expected


def test_next_multiple_exported_handlers():
    path = "app/api/x/route.ts"
    result = extract([path], {}, {path: "export function GET(){return 1;} export const POST=()=>2;"})
    assert not result.limitations
    assert {f.name for f in result.facts} == {"GET /api/x", "POST /api/x"}


def test_dynamic_mount_change_preserves_both_sides_evidence():
    source = "const app=express(); const r=Router(); app.use('/api'+version, auth, r); r.get('/x', ()=>1);"
    result = extract(["api.ts"], {"api.ts": source}, {"api.ts": source.replace("version", "nextVersion")})
    assert not result.facts
    assert {loc.side for limitation in result.limitations for loc in limitation.locations} == {"base", "head"}

@pytest.mark.parametrize('parent,child,mount', [
    ('FastAPI()', "Blueprint('bp', __name__)", 'include_router'),
    ('Flask(__name__)', 'APIRouter()', 'register_blueprint'),
    ('FastAPI()', "Blueprint('bp', __name__)", 'register_blueprint'),
    ('Flask(__name__)', 'APIRouter()', 'include_router'),
    ('FastAPI()', 'APIRouter()', 'register_blueprint'),
    ('Flask(__name__)', "Blueprint('bp', __name__)", 'include_router'),
])
def test_python_mount_api_requires_matching_frameworks(parent, child, mount):
    source = ("from fastapi import FastAPI, APIRouter\nfrom flask import Flask, Blueprint\n"
              f"app={parent}\nr={child}\napp.{mount}(r)\n@r.get('/x')\ndef f(): return 1\n")
    for before, after in [({}, {'api.py': source}), ({'api.py': source}, {}),
                          ({'api.py': source}, {'api.py': source.replace('return 1', 'return 2')})]:
        result = extract(['api.py'], before, after)
        assert not result.facts
        assert {l.code for l in result.limitations} == {'dynamic_route'}
        assert any(l.locations[0].line == 5 for l in result.limitations)
