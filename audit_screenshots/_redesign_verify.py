"""Verify the 2026-06-10 redesign slice: procurement two-question page + /company dossier."""

from __future__ import annotations

from pathlib import Path

import duckdb
from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8534"
DESK = {"width": 1440, "height": 1600}

# Top supplier_norm straight off the gold parquet so the dossier shot uses a real key.
AW = str(Path(__file__).resolve().parents[1] / "data/gold/parquet/procurement_awards.parquet").replace("\\", "/")
top_norm = duckdb.sql(
    f"SELECT supplier_norm FROM '{AW}' WHERE supplier_class='company' "
    "GROUP BY 1 ORDER BY count(*) DESC LIMIT 1"
).fetchone()[0]
print("dossier key:", top_norm)

ROUTES = [
    ("rv_procurement", "rankings-procurement"),
    ("rv_company_landing", "company"),
    ("rv_company_dossier", f"company?supplier={top_norm}"),
]


def _wait(pg, ms=3500):
    try:
        pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        pg.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    pg.wait_for_timeout(ms)


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport=DESK, device_scale_factor=1)
    pg = ctx.new_page()
    for name, route in ROUTES:
        pg.goto(f"{BASE}/{route}", wait_until="domcontentloaded")
        _wait(pg)
        pg.screenshot(path=str(OUT / f"{name}.png"))
        print("saved", name)
    b.close()
print("DONE")
