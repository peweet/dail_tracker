"""Probe — drive CBI news listing with Playwright to find the URL pattern for
enforcement-action press releases."""
from playwright.sync_api import sync_playwright
import json

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent="Mozilla/5.0", viewport={"width":1400,"height":1800})
    pg = ctx.new_page()

    # Probe the actual landing page for navigation hints
    for url in [
        "https://www.centralbank.ie/news",
        "https://www.centralbank.ie/news-media/legal-notices/enforcement-actions",
        "https://www.centralbank.ie/news-media/news?searchterm=enforcement",
    ]:
        try:
            pg.goto(url, wait_until="domcontentloaded", timeout=30000)
            pg.wait_for_timeout(2500)
            title = pg.title()
            final = pg.url
            # Pull all anchor hrefs containing 'enforcement'
            hrefs = pg.evaluate("""() => Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => h && h.toLowerCase().includes('enforcement'))""")
            print(f"\n{url}")
            print(f"  final: {final}")
            print(f"  title: {title!r}")
            print(f"  enforcement-flavour anchors: {len(hrefs)}")
            for h in hrefs[:20]:
                print(f"    {h}")
        except Exception as e:
            print(f"\n{url}\n  ERR: {e}")

    browser.close()
