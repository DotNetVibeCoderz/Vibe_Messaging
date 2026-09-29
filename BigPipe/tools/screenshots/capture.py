"""Captures documentation screenshots of the BigPipe Console (and any other web UI).

Uses the locally installed Google Chrome through Playwright, waits until each page shows
real content, and writes PNGs to docs/images.

    pip install playwright
    python tools/screenshots/capture.py [console-url] [out-dir]
"""
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/images")
OUT.mkdir(parents=True, exist_ok=True)

# (file name, path, css selector that proves the page loaded, theme, language, full page?)
SHOTS = [
    ("console-overview.png", "/", ".schematic svg", "light", "en", True),
    ("console-overview-dark.png", "/", ".schematic svg", "dark", "en", False),
    ("console-topics.png", "/topics", "table.grid tbody tr", "light", "en", True),
    ("console-topic-messages.png", "/topics/payments", ".records .rec", "light", "en", False),
    ("console-topic-config.png", "/topics/clickstream", ".readouts", "light", "en", False),
    ("console-groups.png", "/groups", "table.grid tbody tr", "light", "en", True),
    ("console-flows.png", "/flows", "table.grid tbody tr", "light", "en", True),
    ("console-schemas.png", "/schemas", "pre", "light", "en", False),
    ("console-overview-id.png", "/", ".schematic svg", "light", "id", False),
]

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    for name, path, ready, theme, lang, full in SHOTS:
        ctx = browser.new_context(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
        ctx.add_cookies([
            {"name": "bp_theme", "value": theme, "url": BASE},
            {"name": "bp_lang", "value": lang, "url": BASE},
        ])
        page = ctx.new_page()
        page.goto(BASE + path, wait_until="networkidle")
        page.wait_for_selector(ready, timeout=20000)
        if name == "console-topic-config.png":
            page.get_by_role("tab", name="Configuration").click()
            page.wait_for_selector(".segmented", timeout=10000)
        page.wait_for_timeout(2500)  # let live readouts and sparklines fill in
        page.screenshot(path=str(OUT / name), full_page=full)
        print("saved", OUT / name)
        ctx.close()
    browser.close()
