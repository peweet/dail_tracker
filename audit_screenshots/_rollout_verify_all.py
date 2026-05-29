"""Full rollout verification: every page should have NO sidebar and NO exception."""
from __future__ import annotations
import time
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8599"
ROUTES = {
    "member-overview": "member-overview",
    "attendance": "rankings-attendance",
    "votes": "rankings-votes",
    "interests": "rankings-interests",
    "payments": "rankings-payments",
    "lobbying": "rankings-lobbying",
    "legislation": "rankings-legislation",
    "statutory-instruments": "rankings-statutory-instruments",
    "committees": "rankings-committees",
    "glossary": "glossary",
}


def _settle(page):
    time.sleep(2.5)
    try:
        ar = page.get_by_role("button", name="Always rerun")
        if ar.is_visible(timeout=1000):
            ar.click(); time.sleep(1.8)
    except Exception:
        pass
    page.mouse.move(0, 0)
    time.sleep(0.6)


def main() -> None:
    fails = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for label, slug in ROUTES.items():
            ctx = browser.new_context(viewport={"width": 1440, "height": 900})
            page = ctx.new_page()
            try:
                page.goto(f"{BASE}/{slug}", wait_until="networkidle", timeout=30000)
                _settle(page)
                r = page.evaluate(
                    """() => {
                        const sb = document.querySelector('[data-testid="stSidebar"]');
                        const vis = sb ? (getComputedStyle(sb).display !== 'none' && sb.getBoundingClientRect().width > 5) : false;
                        return {
                            sidebar: vis,
                            exc: document.querySelectorAll('[data-testid="stException"]').length,
                            h1: ((document.querySelector('h1')||{}).innerText || '').slice(0, 40),
                            filterbar: document.querySelectorAll('.dt-filterbar-marker').length,
                        };
                    }"""
                )
                flag = "" if (not r["sidebar"] and r["exc"] == 0) else "   <<< FAIL"
                if flag:
                    fails += 1
                print(f"{label:24s} sidebar={r['sidebar']!s:5s} exc={r['exc']} bar={r['filterbar']} h1={r['h1']!r}{flag}")
            except Exception as e:
                fails += 1
                print(f"{label:24s} ERROR: {e}")
            ctx.close()
        browser.close()
    print("\nRESULT:", "ALL PASS" if fails == 0 else f"{fails} FAIL(S)")


if __name__ == "__main__":
    main()
