"""Validate the sidebar->filter-bar plan: capture the current legislation page
so we can confirm the sidebar identity band duplicates the top-nav + hero.

Desktop clip shows top-nav + sidebar + hero together. Mobile snap checks the
current sidebar behaviour on a narrow viewport.
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
SLUG = "rankings-legislation"
OUT_DIR = Path(__file__).resolve().parent
DESKTOP = OUT_DIR / "_filterbar_legislation_desktop.png"
MOBILE = OUT_DIR / "_filterbar_legislation_mobile.png"


def _dismiss_rerun(page) -> None:
    try:
        ar = page.get_by_role("button", name="Always rerun")
        if ar.is_visible(timeout=1500):
            ar.click()
            time.sleep(2.0)
    except Exception:
        pass


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        # ── Desktop: top-nav + sidebar + hero in one frame ──────────────
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        page.goto(f"{BASE}/{SLUG}", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        _dismiss_rerun(page)
        page.mouse.move(0, 0)
        time.sleep(0.5)

        survey = page.evaluate(
            """() => {
                const sb = document.querySelector('[data-testid="stSidebar"]');
                const txt = sb ? sb.innerText.split('\\n').filter(Boolean).slice(0, 10) : [];
                const h1 = document.querySelector('h1');
                return {
                    sidebarWidth: sb ? Math.round(sb.getBoundingClientRect().width) : 0,
                    sidebarLines: txt,
                    mainHero: h1 ? h1.innerText : null,
                    topNavLinks: document.querySelectorAll('[data-testid="stTopNavLink"]').length,
                };
            }"""
        )
        print("DESKTOP survey:")
        print("  sidebar width :", survey["sidebarWidth"])
        print("  sidebar text  :", survey["sidebarLines"])
        print("  main hero h1  :", survey["mainHero"])
        print("  top-nav links :", survey["topNavLinks"])

        page.screenshot(path=str(DESKTOP), clip={"x": 0, "y": 0, "width": 1440, "height": 760})
        print(f"saved {DESKTOP}")
        ctx.close()

        # ── Mobile: how does the sidebar behave at phone width? ─────────
        ctx2 = browser.new_context(viewport={"width": 390, "height": 844})
        page2 = ctx2.new_page()
        page2.goto(f"{BASE}/{SLUG}", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        _dismiss_rerun(page2)
        page2.mouse.move(0, 0)
        time.sleep(0.5)
        page2.screenshot(path=str(MOBILE), clip={"x": 0, "y": 0, "width": 390, "height": 760})
        print(f"saved {MOBILE}")
        ctx2.close()

        browser.close()


if __name__ == "__main__":
    main()
