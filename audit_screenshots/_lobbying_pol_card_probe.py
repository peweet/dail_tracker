"""Probe: where do politician name-cards go on the lobbying page?

Visits org-detail, area-detail, and DPO-detail routes and prints out the
hrefs of every politician card. CONTEXT-LOSS = card jumps to /member-overview
rather than staying contextual (?lp3_org=X..., ?lp3_area=X&lp3_result_pol=Y,
?lp3_dpo=X..., etc.).
"""
from __future__ import annotations

import urllib.parse

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-lobbying"
PAGE_WAIT = 6000


def _settle(page, ms=2500):
    page.wait_for_timeout(ms)


def _hero(page) -> str:
    h = page.eval_on_selector_all("h1", "ns => ns.map(n => n.textContent.trim())")
    return " | ".join(h)[:160]


def _list_cards(page, label: str, aria_prefix: str) -> None:
    sel = f'a[aria-label^="{aria_prefix}"]'
    cards = page.eval_on_selector_all(
        sel,
        """nodes => nodes.map(n => ({
            href: n.getAttribute('href'),
            label: n.getAttribute('aria-label')
        }))""",
    )
    bad = sum(1 for c in cards if "member-overview" in (c.get("href") or "")
              or "rankings-members" in (c.get("href") or ""))
    print(f"\n=== {label} ===")
    print(f"  hero: {_hero(page)}")
    print(f"  selector: {sel}  -> {len(cards)} cards, {bad} jump to /member-overview")
    for c in cards[:6]:
        href = (c.get("href") or "")[:140]
        verdict = "GENERIC" if ("member-overview" in href or "rankings-members" in href) else "OK"
        # avoid console unicode crash on Windows CP1252
        lbl = (c.get("label") or "").encode("ascii", "ignore").decode("ascii")
        print(f"   [{verdict}] {lbl}")
        print(f"           -> {href}")


def fresh(p, viewport=None):
    viewport = viewport or {"width": 1440, "height": 900}
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=viewport)
    page = ctx.new_page()
    return browser, ctx, page


def main() -> None:
    with sync_playwright() as p:
        # 1. Org detail — Ibec.   Bug claim: politician cards go to /member-overview.
        b, c, page = fresh(p)
        page.goto(f"{BASE}{ROUTE}?lp3_org={urllib.parse.quote('Ibec')}",
                  wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        _list_cards(page, "Org=Ibec: Politicians targeted", "View ")
        b.close()

        # 2. Area detail — pick the first registered area from the page itself
        b, c, page = fresh(p)
        page.goto(f"{BASE}{ROUTE}?lp3_orgindex=1", wait_until="domcontentloaded")
        _settle(page, 3000)
        # Just go to landing to read an area from the area selectbox isn't easy.
        # Instead navigate to a known-good area; fetch from the URL on the
        # 'Browse by policy area' tile on the landing.
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        tile_href = page.eval_on_selector_all(
            'a[aria-label="Browse lobbying by policy area"]',
            "ns => ns.map(n => n.getAttribute('href'))",
        )
        print(f"\n> tile_href: {tile_href}")
        area_url = tile_href[0] if tile_href else "?lp3_area=Industrial%20Relations"
        page.goto(f"{BASE}{ROUTE}{area_url}", wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        _list_cards(page, f"Area detail {area_url}: Most-targeted politicians", "View every return")
        b.close()

        # 3. RD index — first DPO card.   Then DPO detail.
        b, c, page = fresh(p)
        page.goto(f"{BASE}{ROUTE}?lp3_rd=1", wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        rd = page.eval_on_selector_all(
            'a[aria-label^="View revolving-door profile for"]',
            "ns => ns.map(n => ({href: n.getAttribute('href'), label: n.getAttribute('aria-label')}))",
        )
        print(f"\n> RD index found {len(rd)} DPO cards")
        if rd:
            href = rd[0]["href"]
            print(f"  first DPO link: {href}  label: {rd[0]['label'][:80]}")
            page.goto(f"{BASE}{ROUTE}{href}", wait_until="domcontentloaded")
            _settle(page, PAGE_WAIT)
            _list_cards(page, "DPO detail: Politicians targeted", "Open profile for ")
        b.close()


if __name__ == "__main__":
    main()
