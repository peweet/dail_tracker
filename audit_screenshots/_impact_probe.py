"""Ground the risk assessment: does the SI->legislation bill id resolve, and how
big are the dataframes we'd convert to cards (UX-risk sizing)?"""
from __future__ import annotations
import sys, time
from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"

def settle(pg, t=6.0):
    try: pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception: pass
    time.sleep(t)

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width":1440,"height":2600}).new_page()

    # 1) Would simply fixing the slug be enough? Load the target the SI link
    #    WOULD point to once corrected, and see if the bill resolves.
    pg.goto(f"{BASE}/rankings-legislation?bill=2026_6", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
    txt = pg.evaluate("()=>document.body.innerText")
    notfound = any(s in txt.lower() for s in ("not found","isn't in","not in the index","no bill","unknown"))
    # crude: does a bill-detail heading / back button render?
    has_back = "back" in txt.lower()
    print(f"SI-fix end-to-end: /rankings-legislation?bill=2026_6 -> notfound_marker={notfound}, has_back={has_back}, chars={len(txt)}")
    print("   first 240 chars:", " ".join(txt.split())[:240])

    # 2) Size the dataframe->card conversions (rows = UX risk if huge)
    def df_rows(slug, label, prep=None):
        pg.goto(f"{BASE}{slug}", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
        if prep: prep()
        # stDataFrame uses a canvas/grid; count rendered row testids if present, else report element count
        n_df = pg.evaluate("()=>document.querySelectorAll('[data-testid=\"stDataFrame\"]').length")
        # try to read the row-count text the grid exposes via aria or the glide grid row count
        rowinfo = pg.evaluate(r"""()=>{
            const g=document.querySelector('[data-testid=\"stDataFrame\"]');
            if(!g) return null;
            const aria=g.querySelector('[role=\"grid\"]');
            return aria? aria.getAttribute('aria-rowcount'): 'no-aria';
        }""")
        print(f"{label}: stDataFrames={n_df}, aria-rowcount={rowinfo}")

    def to_county():
        for lab in ("By county",):
            try:
                el = pg.get_by_text(lab, exact=False).first
                if el and el.is_visible(): el.click(); settle(pg,5)
            except Exception: pass
    df_rows("/housing","housing By-county", to_county)
    df_rows("/accommodation-spend","accommodation-spend providers")
    pg.goto(f"{BASE}/rankings-lobbying?lp3_pol=Paschal%20Donohoe", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
    n_df = pg.evaluate("()=>document.querySelectorAll('[data-testid=\"stDataFrame\"]').length")
    rc = pg.evaluate(r"""()=>[...document.querySelectorAll('[data-testid=\"stDataFrame\"] [role=\"grid\"]')].map(g=>g.getAttribute('aria-rowcount'))""")
    print(f"lobbying ?lp3_pol Donohoe: stDataFrames={n_df}, aria-rowcounts={rc}")
    b.close()
