"""Verify the audit fixes: no sidebar chevron on appointments/corporate, mobile header tagline gone."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8536"


def _wait(pg, ms=3000):
    try:
        pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        pg.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    pg.wait_for_timeout(ms)


def _chevron_visible(pg) -> bool:
    # Streamlit's collapsed-sidebar expand control
    return pg.evaluate(
        """() => {
            const el = document.querySelector('[data-testid="stSidebarCollapsedControl"], [data-testid="collapsedControl"]');
            if (!el) return false;
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && getComputedStyle(el).display !== 'none';
        }"""
    )


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 940}, device_scale_factor=1).new_page()

    for name, route in [("appointments", "rankings-appointments"), ("corporate", "rankings-corporate")]:
        pg.goto(f"{BASE}/{route}", wait_until="domcontentloaded")
        _wait(pg)
        print(f"{name}: sidebar chevron visible = {_chevron_visible(pg)}")
        pg.screenshot(path=str(OUT / f"_fix_{name}.png"))

    # mobile header — tagline should be gone
    pg.set_viewport_size({"width": 390, "height": 844})
    pg.goto(f"{BASE}/member-overview", wait_until="domcontentloaded")
    _wait(pg)
    sub_visible = pg.evaluate(
        """() => { const e=document.querySelector('.site-banner-sub'); if(!e) return 'absent'; return getComputedStyle(e).display; }"""
    )
    print("mobile tagline display:", sub_visible)
    pg.screenshot(path=str(OUT / "_fix_mobile_header.png"))
    b.close()
print("DONE")
