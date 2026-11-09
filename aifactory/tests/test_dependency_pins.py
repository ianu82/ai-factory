"""Pins that must agree across the image and the test environment."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIN = re.compile(r'"playwright==([0-9][0-9.]*)"')


def test_playwright_test_pin_matches_the_browser_the_image_ships():
    # Factory sandboxes share the image's browsers (PLAYWRIGHT_BROWSERS_PATH). A newer Playwright in the test
    # environment looks for a browser build the image does not have, and every screenshot test fails.
    image = PIN.search((ROOT / "Dockerfile").read_text())
    tests = PIN.search((ROOT / "pyproject.toml").read_text())
    assert image and tests, "Playwright must be pinned exactly in both the Dockerfile and the dev dependencies"
    assert tests.group(1) == image.group(1)
