"""Single-step Playwright runner — open a URL (optionally click something),
screenshot, exit. Drive each step from the command line so I can look at
each screenshot before deciding the next action.

Usage:
    python _manual_step.py <name> <url-path> [click_text]

Click target: literal text to click after page settles (optional).
"""
from __future__ import annotations
import sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent / "_manual"
OUT.mkdir(parents=True, exist_ok=True)
BASE = "http://localhost:8501"
SETTLE = 7


def main():
    name = sys.argv[1]
    path = sys.argv[2]
    click_target = sys.argv[3] if len(sys.argv) > 3 else None
    fill_target = sys.argv[4] if len(sys.argv) > 4 else None  # text to type
    press_enter = "--enter" in sys.argv
    # Optional --input N to target the Nth text input on the page (0-indexed)
    input_idx = 0
    for i, a in enumerate(sys.argv):
        if a == "--input" and i + 1 < len(sys.argv):
            input_idx = int(sys.argv[i + 1])
    # Optional --placeholder "..." to target an input whose placeholder matches
    placeholder = None
    for i, a in enumerate(sys.argv):
        if a == "--placeholder" and i + 1 < len(sys.argv):
            placeholder = sys.argv[i + 1]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 1400})
        page = ctx.new_page()
        page.set_default_timeout(20000)
        page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
        time.sleep(SETTLE)
        # dismiss modal
        try:
            btn = page.locator('[data-testid="stDialog"] button').first
            if btn.is_visible(timeout=300):
                btn.click(); time.sleep(0.3)
        except Exception:
            pass

        # Click target first so e.g. an expander/chip opens before we look for inputs
        if click_target:
            try:
                page.get_by_text(click_target, exact=False).first.click(timeout=4000)
                time.sleep(3)
            except Exception as e:
                print(f"click failed: {e}")

        if fill_target is not None:
            try:
                if placeholder:
                    inp = page.locator(f'input[placeholder*="{placeholder}" i]').first
                else:
                    inputs = page.locator('input[type="text"], input[role="combobox"]').all()
                    print(f"text inputs visible on page: {len(inputs)}")
                    for k, ip in enumerate(inputs[:8]):
                        try:
                            ph = ip.get_attribute("placeholder") or ""
                            print(f"  [{k}] placeholder={ph!r}")
                        except Exception:
                            pass
                    inp = inputs[input_idx]
                inp.click()
                inp.fill(fill_target)
                time.sleep(2)
                if press_enter:
                    inp.press("Enter")
                    time.sleep(3)
            except Exception as e:
                print(f"fill failed: {e}")
        click_target = None  # already handled above

        if click_target:
            try:
                page.get_by_text(click_target, exact=False).first.click(timeout=4000)
                time.sleep(3)
            except Exception as e:
                print(f"click failed: {e}")

        path_png = OUT / f"{name}.png"
        page.screenshot(path=str(path_png), full_page=True)
        print(f"screenshot: {path_png}")
        ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
