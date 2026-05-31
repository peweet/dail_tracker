"""Probe: where do politician name-cards go on the lobbying page?

Visits landing, org-detail, area-detail, DPO-detail AND the new Stage-3
routes. Prints out the hrefs of every politician card and whether each
stays inside the lobbying page (OK) or jumps to /member-overview (GENERIC).
"""
from __future__ import annotations

import urllib.parse

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-lobbying"
PAGE_WAIT = 7000


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
    bad = sum(1 for c in cards
              if "member-overview" in (c.get("href") or "")
              or "rankings-members" in (c.get("href") or ""))
    print(f"\n=== {label} ===")
    print(f"  hero: {_hero(page)}")
    print(f"  selector: {sel}  -> {len(cards)} cards, {bad} jump to /member-overview")
    for c in cards[:5]:
        href = (c.get("href") or "")[:140]
        verdict = "GENERIC" if ("member-overview" in href or "rankings-members" in href) else "OK"
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
        # 1. Landing -- Most-lobbied politicians cards
        b, c, page = fresh(p)
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        _list_cards(page, "Landing: Most-lobbied politicians", "View lobbying record for")
        b.close()

        # 2. Org detail -- Ibec
        b, c, page = fresh(p)
        page.goto(f"{BASE}{ROUTE}?lp3_org={urllib.parse.quote('Ibec')}",
                  wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        _list_cards(page, "Org=Ibec: Politicians targeted",
                    "View every return from Ibec targeting")
        b.close()

        # 3. Org x Politician Stage 3 (sanity check the new route)
        b, c, page = fresh(p)
        url = f"{BASE}{ROUTE}?lp3_org=Ibec&lp3_result_pol=Paschal%20Donohoe"
        page.goto(url, wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        print(f"\n=== Stage 3 org x pol: Ibec x Paschal Donohoe ===")
        print(f"  hero: {_hero(page)}")
        return_cards = page.eval_on_selector_all(
            'article.lp3-return-card', 'ns => ns.length',
        )
        print(f"  lp3-return-card count: {return_cards}")

        # 4. DPO detail -- first DPO
        page.goto(f"{BASE}{ROUTE}?lp3_rd=1", wait_until="domcontentloaded")
        _settle(page, PAGE_WAIT)
        rd = page.eval_on_selector_all(
            'a[aria-label^="View revolving-door profile for"]',
            "ns => ns.map(n => ({href: n.getAttribute('href'), label: n.getAttribute('aria-label')}))",
        )
        if rd:
            href = rd[0]["href"]
            print(f"\n> first DPO: {rd[0]['label'][:80]}  href: {href}")
            page.goto(f"{BASE}{ROUTE}{href}", wait_until="domcontentloaded")
            _settle(page, PAGE_WAIT)
            # Compose the dynamic aria-prefix for the DPO targeted cards
            indiv = urllib.parse.unquote(href.split("=", 1)[1])
            _list_cards(page,
                        f"DPO={indiv}: Politicians targeted",
                        f"View every return from {indiv} targeting")
        b.close()


if __name__ == "__main__":
    main()
