"""Debug + dismiss the 'source changed' Streamlit toast."""
import io, sys, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-statutory-instruments"


def dismiss_rerun_toast(page):
    """Click 'Always rerun' in the header if the source-changed prompt is up."""
    for label in ("Always rerun", "Rerun"):
        b = page.get_by_role("button", name=label)
        if b.count() > 0:
            try:
                b.first.click()
                print(f"  dismissed: clicked '{label}'")
                time.sleep(2)
                return True
            except Exception as e:
                print(f"  '{label}' click failed: {e}")
    return False


def main():
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        time.sleep(6)
        dismiss_rerun_toast(page)
        time.sleep(6)
        page.mouse.wheel(0, 3000); time.sleep(2)
        page.mouse.wheel(0, 2000); time.sleep(2)

        buttons = page.locator("button")
        n = buttons.count()
        print(f"\n{n} buttons total. Showing 0..100:\n")
        for i in range(min(n, 100)):
            try:
                t = (buttons.nth(i).inner_text() or "").strip().replace("\n", "  ")[:90]
                testid = buttons.nth(i).get_attribute("data-testid") or ""
                kind = buttons.nth(i).get_attribute("kind") or ""
                print(f"  [{i:3d}] testid='{testid}'  kind='{kind}'  text='{t}'")
            except Exception as e:
                print(f"  [{i:3d}] FAIL: {e}")
        ctx.close()
        b.close()


if __name__ == "__main__":
    main()
