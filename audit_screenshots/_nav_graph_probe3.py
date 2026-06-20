"""Confirm Public Payments suppliers resolve to real /company dossiers,
proving the missing PublicPayments->Company link is a true miss."""

from __future__ import annotations

import sys
import time
from urllib.parse import quote

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"
SUPPLIERS = ["ERNST YOUNG", "ROADSTONE", "LAGAN ASPHALT"]


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 2600})
        page = ctx.new_page()
        for s in SUPPLIERS:
            page.goto(f"{BASE}/company?supplier=" + quote(s),
                      wait_until="domcontentloaded", timeout=60000)
            time.sleep(9)
            # a real dossier renders the supplier name as an H1/hero and has
            # award/payment content; a not-found shows the landing search.
            body = page.inner_text("body")
            not_found = "not found" in body.lower() or "no supplier" in body.lower()
            # heuristic: dossier shows the supplier name prominently + sections
            has_name = s.split()[0].upper() in body.upper()
            n_chars = len(body)
            print(f"[{s}] resolves={'NO' if not_found else 'YES'} "
                  f"name_present={has_name} body_len={n_chars} url={page.url}")
        browser.close()


if __name__ == "__main__":
    main()
