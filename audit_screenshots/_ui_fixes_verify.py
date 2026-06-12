"""Verify this session's UI fixes against a freshly started server.

1. member-overview: party pills are multi-select; helper dropdown removed.
2. votes: division detail panel carries a View-on-oireachtas.ie link.
3. public payments: council profile shows the AFS spend-by-service breakdown.
4. judiciary: vacancy-lifecycle links are per-nominee gov.ie search URLs.
5. lobbying: return card carries the lobbying.ie link in its header row.
"""

from __future__ import annotations

import time

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8631"


def main() -> None:
    results: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # ── 1. member overview: multi-select pills + no dropdown ──
        page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded", timeout=60000)
        time.sleep(9)
        n_select = page.evaluate(
            "() => document.querySelectorAll('[data-testid=\"stMainBlockContainer\"] [data-testid=\"stSelectbox\"]').length"
        )
        baseline = page.evaluate("() => (document.body.innerText.match(/(\\d+) TDs/) || [])[1] || ''")
        page.get_by_text("Fianna Fáil", exact=True).first.click()
        time.sleep(2.5)
        ff = page.evaluate("() => (document.body.innerText.match(/(\\d+) TDs/) || [])[1] || ''")
        page.get_by_text("Fine Gael", exact=True).first.click()
        time.sleep(2.5)
        ff_fg = page.evaluate("() => (document.body.innerText.match(/(\\d+) TDs/) || [])[1] || ''")
        page.screenshot(path="audit_screenshots/_verify_mo_pills.png")
        results.append(
            f"1. member-overview: dropdowns_in_main={n_select} (want 0) | counts all={baseline} FF={ff} FF+FG={ff_fg} "
            f"({'PASS' if n_select == 0 and ff and ff_fg and int(ff_fg) > int(ff) else 'CHECK'})"
        )

        # ── 2. votes: division panel oireachtas link ──
        page.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded", timeout=60000)
        time.sleep(9)
        # open the first division (cards are links or buttons; try common patterns)
        opened = False
        for sel in ('a[href*="vote_id="]', ".vt-card a", '[data-testid="stMainBlockContainer"] a.dt-card-link'):
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click()
                opened = True
                break
        time.sleep(5)
        link = page.evaluate(
            """() => {
                const a = document.querySelector('.vt-division-header .dt-source-link');
                return a ? a.getAttribute('href') : null;
            }"""
        )
        page.screenshot(path="audit_screenshots/_verify_votes_panel.png")
        results.append(
            f"2. votes: opened_division={opened} header_link={link} "
            f"({'PASS' if link and 'oireachtas.ie' in link else 'CHECK'})"
        )

        # ── 3. public payments: council AFS breakdown ──
        page.goto(f"{BASE}/rankings-public-payments?publisher=ie_la_south_dublin", wait_until="domcontentloaded", timeout=60000)
        time.sleep(10)
        afs = page.evaluate(
            """() => ({
                head: !!document.querySelector('.pr-afs-head'),
                divisions: [...document.querySelectorAll('.pr-grid .pr-card .pr-name')].slice(0, 6).map(e => e.textContent.trim()),
                text: (document.body.innerText.match(/Council accounts[^\\n]*/) || [null])[0],
            })"""
        )
        page.screenshot(path="audit_screenshots/_verify_pp_council.png", full_page=False)
        results.append(f"3. public-payments council AFS: {afs} ({'PASS' if afs['head'] else 'CHECK'})")

        # ── 4. judiciary: per-nominee gov.ie links in vacancy lifecycle ──
        page.goto(f"{BASE}/rankings-judiciary", wait_until="domcontentloaded", timeout=60000)
        time.sleep(9)
        # navigate to the appointments view (segmented control / tabs)
        try:
            page.get_by_text("Appointments & Government", exact=False).first.click()
            time.sleep(4)
        except Exception:
            pass
        govie = page.evaluate(
            """() => [...document.querySelectorAll('.jud-vac a')].slice(0, 3).map(a => a.getAttribute('href'))"""
        )
        page.screenshot(path="audit_screenshots/_verify_judiciary.png")
        ok4 = bool(govie) and all(h and "gov.ie/en/search" in h for h in govie)
        results.append(f"4. judiciary vacancy links: {govie} ({'PASS' if ok4 else 'CHECK'})")

        # ── 5. lobbying: return-card header link ──
        page.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded", timeout=60000)
        time.sleep(9)
        card = page.evaluate(
            """() => {
                const c = document.querySelector('.lp3-return-card');
                if (!c) return {found: false};
                const a = c.querySelector('.lp3-return-head .dt-source-link');
                const w = c.getBoundingClientRect().width;
                return {found: true, headerLink: a ? a.getAttribute('href') : null, width: Math.round(w)};
            }"""
        )
        results.append(f"5. lobbying card (landing): {card}")
        # topic stage-2 cards live deeper; try a topic tile if no card on landing
        if not card.get("found"):
            try:
                page.locator(".lp3-topic-tile, a[href*='lp3']").first.click()
                time.sleep(6)
                card = page.evaluate(
                    """() => {
                        const c = document.querySelector('.lp3-return-card');
                        if (!c) return {found: false};
                        const a = c.querySelector('.lp3-return-head .dt-source-link');
                        return {found: true, headerLink: a ? a.getAttribute('href') : null,
                                width: Math.round(c.getBoundingClientRect().width)};
                    }"""
                )
                results.append(f"5b. lobbying card (topic page): {card}")
            except Exception as e:  # noqa: BLE001
                results.append(f"5b. could not reach topic cards: {type(e).__name__}")
        page.screenshot(path="audit_screenshots/_verify_lobbying.png")

        browser.close()

    print("\n".join(results))


if __name__ == "__main__":
    main()
