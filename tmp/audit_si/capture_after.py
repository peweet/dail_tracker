"""Post-fix recapture — same comprehensive sweep as capture.py, output
suffix '_after' so we can diff against the pre-fix shots.

Steps the original missed:
- click Streamlit main menu → 'Clear cache' once at the start so the
  server-side @st.cache_data(ttl=300) wrapping fetch_si_entity_index is
  evicted and the page reloads against the freshly rewritten parquet.
- dismiss the 'source changed → Rerun?' header toast on every navigation.
"""
from __future__ import annotations
import io, sys, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from pathlib import Path
from playwright.sync_api import sync_playwright, Page

BASE = "http://localhost:8501"
ROUTE = "/rankings-statutory-instruments"
OUT = Path(__file__).resolve().parent
DESKTOP = {"width": 1440, "height": 900}
MOBILE  = {"width": 390,  "height": 844}
PAGE_LOAD_WAIT = 5500
RERUN_WAIT = 2400


def shot(page, name, full_page=True):
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=full_page)
    print(f"  -> {p.name}")


def wait(ms): time.sleep(ms / 1000)


def dismiss_rerun(page):
    for label in ("Always rerun", "Rerun"):
        b = page.get_by_role("button", name=label)
        try:
            if b.count() > 0:
                b.first.click(timeout=2000)
                wait(1500)
                return
        except Exception:
            pass


def clear_cache(page):
    """Open the main menu (kebab top-right) → 'Clear cache' → confirm."""
    print("  > clearing Streamlit cache via menu")
    menu = page.locator('[data-testid="stMainMenuButton"]')
    if menu.count() == 0:
        print("     menu not found — skipping")
        return
    try:
        menu.first.click(timeout=3000)
        wait(800)
        # The 'Clear cache' item is a menu item with that text.
        clear = page.locator('text="Clear cache"').first
        if clear.count() > 0:
            clear.click(timeout=3000)
            wait(1000)
            # Confirm dialog uses 'Clear caches' or 'Clear all'
            for label in ("Clear caches", "Clear all", "Clear"):
                btn = page.get_by_role("button", name=label)
                if btn.count() > 0:
                    btn.first.click(timeout=2000)
                    print(f"     confirmed via '{label}'")
                    wait(2000)
                    break
            else:
                # Press Enter to confirm if no button matched
                page.keyboard.press("Enter")
                wait(2000)
        else:
            print("     'Clear cache' menu item not found")
            page.keyboard.press("Escape")
    except Exception as e:
        print(f"     clear_cache failed: {e}")
        page.keyboard.press("Escape")


def goto(page, path=ROUTE):
    print(f"  > GOTO {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)
    dismiss_rerun(page)
    wait(2500)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()

        # One-time: clear server-side cache so we see the new parquet
        goto(page)
        clear_cache(page)
        # Reload after cache clear
        goto(page)

        # ── DESKTOP ────────────────────────────────────────────────────
        print("\n== DESKTOP AFTER ==")
        shot(page, "after_A01_landing_full_desktop")
        shot(page, "after_A02_landing_above_fold", full_page=False)
        page.mouse.wheel(0, 1500); wait(600)
        shot(page, "after_A03_mid", full_page=False)
        page.mouse.wheel(0, 1500); wait(600)
        shot(page, "after_A04_cards", full_page=False)
        page.mouse.wheel(0, 4000); wait(600)
        shot(page, "after_A05_bottom", full_page=False)

        # ── SI DETAIL — same 2026-117 / 2026-116 / 2026-114 as before ──
        print("\n== DESKTOP — DETAIL PANELS ==")
        for sid in ("2026-117", "2026-116", "2026-114", "2026-071"):
            print(f"  > deep-link {sid}")
            page.goto(f"{BASE}{ROUTE}?si={sid}", wait_until="domcontentloaded")
            wait(PAGE_LOAD_WAIT)
            dismiss_rerun(page)
            wait(2000)
            shot(page, f"after_G_detail_{sid}_full")
            shot(page, f"after_G_detail_{sid}_above_fold", full_page=False)

        # ── PAGINATION FOOTER ─────────────────────────────────────────
        print("\n== DESKTOP — PAGINATION/CARDS ==")
        goto(page)
        page.mouse.wheel(0, 12000); wait(700)
        shot(page, "after_F01_pagination_footer", full_page=False)
        page.mouse.wheel(0, 2000); wait(400)
        shot(page, "after_F02_pagination_cards", full_page=False)

        # ── MOBILE ────────────────────────────────────────────────────
        print("\n== MOBILE AFTER ==")
        page.set_viewport_size(MOBILE)
        goto(page)
        shot(page, "after_I05_mobile_landing_full")
        shot(page, "after_I06_mobile_above_fold", full_page=False)
        page.mouse.wheel(0, 2400); wait(500)
        shot(page, "after_I09_mobile_cards", full_page=False)

        ctx.close()
        browser.close()
    print("\nDONE")


if __name__ == "__main__":
    main()
