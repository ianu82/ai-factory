"""Fixtures are intentionally scoped here; the parent isolated_env is unchanged."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from urllib.parse import urlparse
import re
import subprocess
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[2]
VIEWPORTS = ((1440, 900), (375, 812))


def pytest_configure(config):
    config.addinivalue_line("markers", "browser: real built SPA in isolated headless Chromium")


def pytest_collection_modifyitems(items):
    directory = Path(__file__).parent
    for item in items:
        if directory in Path(item.path).parents:
            item.add_marker(pytest.mark.browser)
            if os.environ.get("AIFACTORY_SKIP_BROWSER") == "1":
                item.add_marker(pytest.mark.skip(reason="AIFACTORY_SKIP_BROWSER=1"))


def dependencies_current(web):
    """npm's hidden lock describes the installed tree, including transitive pins."""
    try:
        wanted = json.loads((web / "package-lock.json").read_text())["packages"]
        installed = json.loads((web / "node_modules/.package-lock.json").read_text())["packages"]
        for name, pin in wanted.items():
            if not name or pin.get("optional"):
                continue  # platform-specific optional packages need not be installed
            actual = installed.get(name, {})
            if any(actual.get(key) != pin.get(key) for key in ("version", "integrity", "resolved")):
                return False
            if not (web / name / "package.json").is_file():
                return False
        return (web / "node_modules/.bin/vite").exists()
    except (OSError, ValueError, KeyError):
        return False


@pytest.fixture(scope="session")
def web_build(tmp_path_factory):
    """Always build fresh once: no mtime heuristic or checked-in dist reuse."""
    web = ROOT / "web"
    out = tmp_path_factory.mktemp("browser-web")
    commands = []
    if not dependencies_current(web):
        commands.append(["npm", "ci", "--no-audit", "--no-fund"])
    commands.append(["npm", "run", "build", "--", "--outDir", str(out)])
    for command in commands:
        try:
            result = subprocess.run(command, cwd=web, capture_output=True, text=True, timeout=120, check=True)
        except subprocess.CalledProcessError as exc:
            pytest.fail(f"Browser build failed: {' '.join(command)}\n{exc.stdout}\n{exc.stderr}")
        except (OSError, subprocess.TimeoutExpired) as exc:
            pytest.fail(f"Browser build prerequisite failed: {command}: {exc}")
        (out / "build.log").write_text(result.stdout + result.stderr)
    assert (out / "index.html").is_file(), f"Vite did not produce {out / 'index.html'}"
    return out


@pytest.fixture(scope="session")
def browser_server(web_build, tmp_path_factory):
    from tests.browser.server import BrowserServer

    root = tmp_path_factory.mktemp("browser-server")
    server = BrowserServer(web_build, root / "data", root / "server.log")
    try:
        yield server
    finally:
        server.close()


@pytest.fixture
def browser():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        chromium = playwright.chromium.launch(headless=True)
        try:
            yield chromium
        finally:
            chromium.close()


@pytest.fixture(params=VIEWPORTS, ids=[f"{w}x{h}" for w, h in VIEWPORTS])
def viewport(request):
    w, h = request.param
    return {"width": w, "height": h}


@pytest.fixture
def scenario(browser_server):
    return browser_server.control("create_workspace")


@pytest.fixture
def control(browser_server, scenario):
    """Only current-workspace server commands; never an unauthenticated HTTP API."""
    def command(op, **payload):
        return browser_server.control(op, workspace_id=scenario["workspace_id"], **payload)
    return command


class Diagnostics:
    def __init__(self, node, directory, viewport):
        self.node = node
        self.directory = directory
        self.viewport = f"{viewport['width']}x{viewport['height']}"
        self.context = None
        self.captures = []
        self.errors = []

    def capture(self, label):
        if self.context is None:
            return
        for index, page in enumerate(self.context.pages):
            if page.is_closed():
                continue
            safe = re.sub(r"[^a-zA-Z0-9_.-]+", "-", self.node.nodeid)[-100:]
            digest = hashlib.sha256(self.node.nodeid.encode()).hexdigest()[:10]
            path = self.directory / f"{safe}-{digest}-{self.viewport}-{label}-{index}-{uuid.uuid4().hex[:8]}.png"
            try:
                page.screenshot(path=str(path), full_page=True, timeout=5000)
                self.captures.append((page.url, path))
            except Exception as exc:
                self.errors.append(f"Screenshot failed for {page.url}: {exc}")

    def report(self):
        lines = [f"Node: {self.node.nodeid}", f"Viewport: {self.viewport}"]
        lines.extend(f"URL: {url}\nPNG: {path}" for url, path in self.captures)
        return "\n".join(lines + self.errors)


@pytest.fixture
def diagnostics(request, tmp_path_factory, viewport):
    state = Diagnostics(request.node, tmp_path_factory.mktemp("browser-artifacts"), viewport)
    request.node._browser_diagnostics = state
    return state


@pytest.fixture
def browser_context(browser, browser_server, viewport, diagnostics):
    context = browser.new_context(base_url=browser_server.url, viewport=viewport, reduced_motion="reduce")
    diagnostics.context = context
    context.set_default_timeout(8000)
    context.set_default_navigation_timeout(15000)
    try:
        yield context
    finally:
        # Stage screenshots before closure even on success: a later fixture's
        # teardown can still fail. The teardown report removes them only on success.
        diagnostics.capture("before-close")
        context.close()


@pytest.fixture
def unsigned_page(browser_context):
    return browser_context.new_page()


@pytest.fixture
def signed_in_page(unsigned_page, scenario):
    from playwright.sync_api import expect

    page = unsigned_page
    page.goto("/signin")
    page.get_by_placeholder("Local development email").fill(scenario["email"])
    page.get_by_role("button", name="Dev sign-in", exact=True).click()
    expect(page).to_have_url(re.compile(f"/w/{scenario['workspace_id']}/inbox$"))
    expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
    return page


@pytest.fixture
def checkpoint(diagnostics):
    """Call after each visited page's visible assertions, before navigating away."""
    return diagnostics.capture


@pytest.fixture
def navigate():
    from playwright.sync_api import expect

    def go(page, name):
        menu = page.get_by_role("button", name="Open navigation", exact=True)
        mobile = menu.is_visible()
        if mobile and not page.locator(".shell.nav-open").count():
            menu.click()
        link = page.get_by_role("complementary", name="Main").get_by_role("link", name=name, exact=False)
        expect(link).to_be_visible()
        same_location = urlparse(page.url).path == link.get_attribute("href")
        link.click()
        # A same-location NavLink does not trigger Shell's pathname effect.
        # Close its still-open drawer through the real dismiss surface.
        if mobile and same_location:
            page.locator(".scrim").click(position={"x": page.viewport_size["width"] - 10, "y": 400})
        if mobile:
            expect(page.locator(".shell.nav-open")).to_have_count(0)
    return go


@pytest.fixture
def visible():
    from playwright.sync_api import expect

    def check(locator, text=None):
        expect(locator).to_be_visible()
        if text is not None:
            expect(locator).to_contain_text(text)
    return check


@pytest.fixture
def no_overflow():
    def check(page):
        try:
            page.wait_for_function("document.documentElement.scrollWidth <= window.innerWidth + 1", timeout=3000)
        except Exception as exc:
            sizes = page.evaluate("({content: document.documentElement.scrollWidth, viewport: innerWidth})")
            raise AssertionError(f"Horizontal overflow at {page.url}: {sizes}") from exc
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth + 1"), (
            f"Horizontal overflow at {page.url}: {page.viewport_size}")
    return check


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    state = getattr(item, "_browser_diagnostics", None)
    if state is None:
        return
    if report.failed:
        item._browser_failed = True
        state.capture(report.when)
        report.sections.append(("Browser failure diagnostics", state.report()))
    if report.when == "teardown":
        if getattr(item, "_browser_failed", False):
            if not report.failed:
                report.sections.append(("Browser failure diagnostics", state.report()))
        else:
            for _, path in state.captures:
                path.unlink(missing_ok=True)
