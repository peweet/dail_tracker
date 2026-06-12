"""Does the fixed masthead overlap the page content at intermediate widths?"""

from __future__ import annotations

import time

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8631"

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    for w in (1440, 1100, 1000, 900, 820, 768, 700, 600, 480, 390):
        ctx = b.new_context(viewport={"width": w, "height": 900})
        pg = ctx.new_page()
        pg.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded", timeout=60000)
        time.sleep(7)
        res = pg.evaluate(
            """() => {
                const hdr = document.querySelector('header[data-testid="stHeader"]');
                const ban = document.querySelector('.site-banner');
                const main = document.querySelector('[data-testid="stMainBlockContainer"]');
                const first = main ? main.querySelector(':scope > div') : null;
                const h1 = document.querySelector('[data-testid="stMainBlockContainer"] h1, [data-testid="stMainBlockContainer"] .dt-hero, [data-testid="stMainBlockContainer"] .stSegmentedControl');
                const hb = hdr ? hdr.getBoundingClientRect().bottom : null;
                const bb = ban ? ban.getBoundingClientRect().bottom : null;
                const mastheadBottom = Math.max(hb || 0, bb || 0);
                const ft = first ? first.getBoundingClientRect().top : null;
                const ht = h1 ? h1.getBoundingClientRect().top : null;
                return {mastheadBottom, firstContentTop: ft, firstKeyElTop: ht,
                        overlapped: ht !== null ? ht < mastheadBottom : null};
            }"""
        )
        print(f"w={w}:", res)
        ctx.close()
    b.close()
