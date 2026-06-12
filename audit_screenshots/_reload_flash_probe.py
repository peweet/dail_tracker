"""Capture what the top of the page looks like during the first seconds after reload."""

from __future__ import annotations

import time

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8631"

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 900}).new_page()
    pg.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded", timeout=60000)
    time.sleep(10)  # settle fully

    pg.reload(wait_until="commit")
    for t in (0.5, 1.5, 3.0, 5.0, 8.0):
        time.sleep(t - (0 if t == 0.5 else [0.5, 1.5, 3.0, 5.0][[1.5, 3.0, 5.0, 8.0].index(t)]))
        pg.screenshot(path=f"audit_screenshots/_reload_t{str(t).replace('.', '_')}.png",
                      clip={"x": 0, "y": 0, "width": 1440, "height": 260})
        state = pg.evaluate(
            """() => ({
                banner: !!document.querySelector('.site-banner'),
                headerBg: (() => { const h = document.querySelector('header'); return h ? getComputedStyle(h).backgroundColor : null; })(),
                navText: (() => { const h = document.querySelector('header'); return h ? (h.innerText||'').replace(/\\n/g,' | ').slice(0,80) : null; })(),
                bodyBg: getComputedStyle(document.body).backgroundColor,
            })"""
        )
        print(f"t={t}s:", state)
    b.close()
