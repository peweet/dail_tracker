"""Scroll deep on Votes, navigate via the grouped top-nav, measure landing scroll."""

from __future__ import annotations

import time

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8631"

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 900}).new_page()
    pg.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded", timeout=60000)
    time.sleep(8)
    pg.evaluate("() => { const m = document.querySelector('section.stMain'); if (m) m.scrollTop = 2500; }")
    time.sleep(1)
    print("votes scrollTop:", pg.evaluate("() => document.querySelector('section.stMain').scrollTop"))

    trig = pg.locator("header div", has_text="Members & Parliament").last
    trig.click()
    time.sleep(1)
    link = pg.get_by_test_id("stTopNavDropdownLink").filter(has_text="Attendance").first
    if not link.is_visible():
        trig.hover()
        time.sleep(1)
    link.click(timeout=8000)
    time.sleep(6)
    res = pg.evaluate(
        """() => {
            const m = document.querySelector('section.stMain');
            const h1 = document.querySelector('[data-testid="stMainBlockContainer"] h1');
            const r = h1 ? h1.getBoundingClientRect() : null;
            return {url: location.pathname, scrollTop: m ? m.scrollTop : null,
                    h1Top: r ? r.top : null, h1Text: h1 ? h1.textContent.slice(0, 40) : null};
        }"""
    )
    print("after nav:", res)
    pg.screenshot(path="audit_screenshots/_after_nav.png")
    b.close()
