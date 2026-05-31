"""Open a profile, open all sections (click each expander), dump full body."""
from __future__ import annotations
import sys, time
from pathlib import Path
from urllib.parse import quote
from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent / "_manual"
OUT.mkdir(parents=True, exist_ok=True)
BASE = "http://localhost:8501"

def main():
    name, code = sys.argv[1], sys.argv[2]
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(viewport={"width": 1440, "height": 1400})
        page = ctx.new_page()
        page.set_default_timeout(20000)
        page.goto(f"{BASE}/member-overview?member={quote(code, safe='')}", wait_until="domcontentloaded")
        time.sleep(8)
        # click every closed details > summary
        try:
            for s in page.locator('details:not([open]) > summary').all():
                try: s.click(); time.sleep(0.5)
                except: pass
            time.sleep(3)
        except: pass
        body = page.locator('[data-testid="stApp"]').inner_text()
        (OUT / f"{name}_body.txt").write_text(body, encoding="utf-8")
        page.screenshot(path=str(OUT / f"{name}.png"), full_page=True)
        # section anchors present?
        anchors = page.locator('[id^="mo-section-"]').all()
        print(f"anchors found: {len(anchors)}")
        for a in anchors[:20]:
            print(" ", a.get_attribute("id"))
        ctx.close(); b.close()

if __name__ == "__main__":
    main()
