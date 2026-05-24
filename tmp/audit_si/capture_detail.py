"""SI detail capture v3 — verbose, with timeouts, single browser context."""
from __future__ import annotations
import io, sys, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "http://localhost:8501"
ROUTE = "/rankings-statutory-instruments"
OUT = Path(__file__).resolve().parent
DESKTOP = {"width": 1440, "height": 900}
PAGE_LOAD_WAIT = 5500
RERUN_WAIT = 2400


def shot(page, name, full_page=True):
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=full_page)
    print(f"  -> {p.name} ({p.stat().st_size // 1024} KB)")


def wait(ms): time.sleep(ms / 1000)


def dismiss_rerun(page):
    for label in ("Always rerun", "Rerun"):
        b = page.get_by_role("button", name=label)
        try:
            n = b.count()
        except Exception:
            n = 0
        if n > 0:
            try:
                b.first.click(timeout=3000)
                print(f"     dismissed: clicked '{label}'")
                wait(2000)
                return
            except Exception as e:
                print(f"     dismiss '{label}' failed: {e}")


def goto(page, path=ROUTE):
    print(f"  > GOTO {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)
    dismiss_rerun(page)
    wait(2500)


def click_view_detail(page):
    print("     [click_view_detail] start")
    # Scroll down so cards render in the viewport (Streamlit doesn't virtualise
    # but actionability checks need the target visible).
    page.mouse.wheel(0, 4000); wait(800)
    page.mouse.wheel(0, 2000); wait(800)
    # 'Clear all' is also a tertiary button — filter by text.
    all_btns = page.locator('[data-testid="stBaseButton-tertiary"]:has-text("View detail")')
    n_all = all_btns.count()
    print(f"     View-detail tertiary buttons total: {n_all}")
    if n_all == 0:
        # Fallback: any button containing 'View detail' as text
        all_btns = page.locator('button:has-text("View detail")')
        n_all = all_btns.count()
        print(f"     fallback button:has-text count: {n_all}")
        if n_all == 0:
            return None
    btn = all_btns.first
    try:
        btn.scroll_into_view_if_needed(timeout=5000)
        wait(400)
        btn.click(timeout=5000, force=True)
        print("     clicked (force=True)")
    except PWTimeout as e:
        print(f"     click timed out: {e}")
        return None
    except Exception as e:
        print(f"     click failed: {e}")
        return None
    wait(4500)
    # Confirm we're now on a detail view by looking for the back button.
    back = page.locator('button:has-text("Back to SI Index")')
    on_detail = back.count() > 0
    url = page.evaluate("() => window.location.href")
    print(f"     on detail? {on_detail}   url={url}")
    if not on_detail:
        return None
    return url.split("si=")[-1].split("&")[0] if "si=" in url else "UNKNOWN"


def click_text_button(page, text):
    """Click any button whose visible text matches exactly."""
    b = page.locator(f'button:has-text("{text}")').first
    try:
        if b.count() == 0:
            return False
        b.scroll_into_view_if_needed(timeout=3000)
        wait(300)
        b.click(timeout=5000)
        wait(RERUN_WAIT)
        return True
    except Exception as e:
        print(f"     click_text_button('{text}') failed: {e}")
        return False


def click_tab(page, label_starts):
    tabs = page.locator('[role="tab"]')
    for i in range(tabs.count()):
        try:
            txt = tabs.nth(i).inner_text().strip()
            if txt.startswith(label_starts):
                tabs.nth(i).click(timeout=3000)
                wait(1500)
                return True
        except Exception:
            continue
    return False


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()

        # G1
        print("\n[G1] first card on default landing")
        goto(page)
        sid = click_view_detail(page)
        if sid:
            print(f"     opened SI {sid}")
            shot(page, "G01_detail_default_full")
            shot(page, "G02_detail_default_above_fold", full_page=False)
            page.mouse.wheel(0, 1200); wait(500)
            shot(page, "G03_detail_default_mid", full_page=False)
            page.mouse.wheel(0, 1500); wait(500)
            shot(page, "G04_detail_default_bottom", full_page=False)

        # G2
        print("\n[G2] EU-derived recent via callout")
        goto(page)
        if click_text_button(page, "Show these"):
            print("     clicked Show-these")
            wait(1000)
        sid_eu = click_view_detail(page)
        if sid_eu:
            print(f"     opened EU SI {sid_eu}")
            shot(page, "G05_detail_eu_minister_full")
            shot(page, "G06_detail_eu_minister_above_fold", full_page=False)
            page.mouse.wheel(0, 1500); wait(500)
            shot(page, "G07_detail_eu_minister_mid", full_page=False)

        # G3 — deep link cold
        if sid:
            print("\n[G3] cold-load deep link")
            goto(page, f"{ROUTE}?si={sid}")
            shot(page, "G08_detail_deeplink_cold_load")
            if click_text_button(page, "Back to SI Index"):
                shot(page, "G09_after_back_button")

        # G4 — Health dept first card (probably unmatched parent legislation)
        print("\n[G4] Health dept first card")
        goto(page)
        if click_tab(page, "Department"):
            click_text_button(page, "Health · 22")
        sid_h = click_view_detail(page)
        if sid_h:
            shot(page, "G10_detail_health_dept_full")
            page.mouse.wheel(0, 1500); wait(500)
            shot(page, "G11_detail_health_dept_billlink", full_page=False)

        # G5 — substantive-or-base
        print("\n[G5] substantive-or-base operation first card")
        goto(page)
        if click_tab(page, "What it does"):
            click_text_button(page, "Substantive or base")
        sid_s = click_view_detail(page)
        if sid_s:
            shot(page, "G12_detail_substantive_full")
            page.mouse.wheel(0, 1200); wait(500)
            shot(page, "G13_detail_substantive_mid", full_page=False)

        # G6 — pagination last page (132)
        print("\n[G6] pagination last page")
        goto(page)
        if click_text_button(page, "132"):
            wait(1500)
            page.mouse.wheel(0, 8000); wait(800)
            shot(page, "F03_pagination_last_page", full_page=False)

        ctx.close()
        browser.close()
    print("\nDONE")


if __name__ == "__main__":
    main()
