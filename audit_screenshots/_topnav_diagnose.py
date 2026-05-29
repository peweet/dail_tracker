"""Inspect Streamlit's native top-nav DOM after switching to
st.navigation(position="top"). Goal: capture the exact data-testid
and class names so we can write CSS overrides that match the dark
banner aesthetic.

Run with the dev server already running at localhost:8501.
"""

from __future__ import annotations

import time

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()

        popups: list[str] = []
        context.on("page", lambda new_page: popups.append(new_page.url or "<no-url>"))

        page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)

        # Always-rerun toast.
        try:
            ar = page.get_by_role("button", name="Always rerun")
            if ar.is_visible(timeout=1000):
                ar.click()
                time.sleep(2.0)
        except Exception:
            pass

        # ── PART 1: dump every element with a stTopNav* data-testid ──────
        print("=" * 70)
        print("PART 1: Streamlit native top-nav DOM")
        print("=" * 70)
        nodes = page.evaluate(
            """() => {
                const out = [];
                document.querySelectorAll('[data-testid^="stTopNav"]').forEach(el => {
                    out.push({
                        tag: el.tagName.toLowerCase(),
                        testid: el.getAttribute('data-testid'),
                        cls: el.className,
                        href: el.getAttribute('href'),
                        target: el.getAttribute('target'),
                        text: (el.textContent || '').trim().slice(0, 60),
                    });
                });
                return out;
            }"""
        )
        if not nodes:
            print("NO stTopNav* elements found. Streamlit may have rendered nothing.")
            print("Header look:")
            print(page.evaluate("() => document.querySelector('header')?.outerHTML?.slice(0, 1200) || '(no header)'"))
        else:
            for n in nodes[:40]:
                print(f"  <{n['tag']}> testid={n['testid']!r}")
                print(f"     class = {n['cls']!r}")
                if n["href"] is not None:
                    print(f"     href   = {n['href']!r}")
                    print(f"     target = {n['target']!r}")
                if n["text"]:
                    print(f"     text   = {n['text']!r}")
                print()

        # ── PART 2: click each nav link, observe behavior ────────────────
        print("=" * 70)
        print("PART 2: Click each nav link, observe in-tab vs popup")
        print("=" * 70)
        labels = ["Member Overview", "Attendance", "Votes", "Interests", "Payments",
                  "Lobbying", "Lobbying (PoC)", "Legislation", "Statutory Instruments",
                  "Committees", "Glossary"]
        for label in labels:
            url_before = page.url
            popup_count_before = len(popups)
            try:
                page.locator(f'[data-testid^="stTopNav"]:has-text("{label}")').first.click(timeout=3000)
            except Exception as exc:
                print(f"  [{label}] CLICK FAILED: {exc}")
                continue
            time.sleep(2.0)
            url_after = page.url
            new_popups = popups[popup_count_before:]
            if new_popups:
                print(f"  [{label}] POPUP -- {len(new_popups)} new tab(s): {new_popups}")
                for pp in list(context.pages):
                    if pp is not page:
                        pp.close()
            elif url_after != url_before:
                print(f"  [{label}] in-tab: {url_after.split('localhost:8501')[-1]}")
            else:
                print(f"  [{label}] no nav (still at {url_after.split('localhost:8501')[-1]})")

        print()
        print(f"TOTAL POPUPS: {len(popups)}")
        browser.close()


if __name__ == "__main__":
    main()
