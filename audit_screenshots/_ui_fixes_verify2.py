"""Round 2: verify votes division-panel link and lobbying return-card header link."""

from __future__ import annotations

import time

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8631"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()

    # ── votes: open a division via its ?vote= link ──
    page.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded", timeout=60000)
    time.sleep(9)
    href = page.evaluate(
        """() => { const a = [...document.querySelectorAll('a')].find(a => (a.getAttribute('href')||'').includes('?vote=')); return a ? a.getAttribute('href') : null; }"""
    )
    print("division href:", href)
    if href:
        page.goto(f"{BASE}/rankings-votes{href[href.index('?'):]}" if not href.startswith("http") else href,
                  wait_until="domcontentloaded", timeout=60000)
        time.sleep(9)
        link = page.evaluate(
            """() => { const a = document.querySelector('.vt-division-header .dt-source-link'); return a ? a.getAttribute('href') : null; }"""
        )
        present = page.evaluate("() => !!document.querySelector('.vt-division-header')")
        print(f"votes panel: header_present={present} link={link} "
              f"({'PASS' if link and 'oireachtas.ie' in link else 'CHECK'})")
        page.screenshot(path="audit_screenshots/_verify_votes_panel2.png")

    # ── lobbying: area stage-2 return cards ──
    page.goto(f"{BASE}/rankings-lobbying?lp3_area=Health", wait_until="domcontentloaded", timeout=60000)
    time.sleep(10)
    card = page.evaluate(
        """() => {
            const c = document.querySelector('.lp3-return-card');
            if (!c) return {found: false, bodySnippet: document.body.innerText.slice(0, 150)};
            const a = c.querySelector('.lp3-return-head .dt-source-link');
            return {found: true, headerLink: a ? a.getAttribute('href') : null,
                    width: Math.round(c.getBoundingClientRect().width)};
        }"""
    )
    print("lobbying area card:", card)
    page.screenshot(path="audit_screenshots/_verify_lobbying2.png")
    browser.close()
