"""Capture settled masthead state from the FRESH 8599 instance (working-tree code)."""
from __future__ import annotations
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8599"
OUT = Path(__file__).resolve().parent / "_masthead_fresh8599.png"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(viewport={"width": 1440, "height": 900}).new_page()
        page.goto(f"{BASE}/rankings-legislation", wait_until="networkidle", timeout=30000)
        time.sleep(4)
        try:
            ar = page.get_by_role("button", name="Always rerun")
            if ar.is_visible(timeout=1500):
                ar.click(); time.sleep(2.5)
        except Exception:
            pass
        try:
            close_btn = page.locator('div[role="dialog"] button[aria-label="Close"]').first
            if close_btn.is_visible(timeout=1500):
                close_btn.click(); time.sleep(0.5)
        except Exception:
            pass
        page.mouse.move(0, 0)
        time.sleep(1.5)
        counts = page.evaluate(
            """() => ({
                siteBanner: document.querySelectorAll('.site-banner').length,
                stHeader: document.querySelectorAll('header[data-testid=\\"stHeader\\"]').length,
                topNavLinks: document.querySelectorAll('[data-testid=\\"stTopNavLink\\"]').length,
                sidebarPresent: !!document.querySelector('[data-testid=\\"stSidebar\\"]'),
                bannerRects: [...document.querySelectorAll('.site-banner')].map(e => {
                    const r = e.getBoundingClientRect();
                    return {top: Math.round(r.top), height: Math.round(r.height)};
                }),
                headerRect: (() => {
                    const h = document.querySelector('header[data-testid=\\"stHeader\\"]');
                    if (!h) return null;
                    const r = h.getBoundingClientRect();
                    return {top: Math.round(r.top), height: Math.round(r.height), bg: getComputedStyle(h).backgroundColor};
                })(),
                navLabels: [...document.querySelectorAll('[data-testid=\\"stTopNavLink\\"] p')].map(e => e.textContent),
            })"""
        )
        print("FRESH 8599 MASTHEAD STATE")
        for k, v in counts.items():
            print(f"  {k}: {v}")
        page.screenshot(path=str(OUT), clip={"x": 0, "y": 0, "width": 1440, "height": 420})
        print(f"saved {OUT}")
        browser.close()


if __name__ == "__main__":
    main()
