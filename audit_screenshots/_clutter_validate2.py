"""Ground the two unresolved checks from _clutter_validate.py."""
from __future__ import annotations
import sys, time, json
from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"

def settle(page, t=6.0):
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except Exception: pass
    time.sleep(t)

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width":1440,"height":2600}).new_page()

    # diaries: enumerate ALL onward (non-state, non-chrome) hrefs on landing + an org drill
    pg.goto(f"{BASE}/rankings-ministerial-diaries", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
    hs = pg.evaluate("""()=>[...document.querySelectorAll('a')].map(a=>({href:a.getAttribute('href')||'',text:(a.textContent||'').trim().slice(0,40)}))""")
    interesting = [a for a in hs if any(k in a["href"] for k in ("org=","member","lp3_org","company?supplier","rankings-corporate","minister="))]
    print("DIARIES landing onward-ish links:")
    for a in interesting[:25]: print(f"   {a['href']!r}  <- {a['text']!r}")
    # follow first org= drill and look for onward
    orgd = [a["href"] for a in hs if "org=" in a["href"]]
    if orgd:
        d = orgd[0]; d = d if d.startswith("http") else f"{BASE}{d if d.startswith('/') else '/rankings-ministerial-diaries'+d}"
        pg.goto(d, wait_until="domcontentloaded", timeout=60000); settle(pg,6)
        hs2 = pg.evaluate("""()=>[...document.querySelectorAll('a')].map(a=>a.getAttribute('href')||'')""")
        onward = [h for h in hs2 if any(k in h for k in ("lp3_org","company?supplier","rankings-corporate","member="))]
        print(f"DIARIES org-drill {d.split(BASE)[-1]!r}: onward entity links = {len(onward)} -> {onward[:8]}")

    # election-2024: count several candidate caveat phrasings
    pg.goto(f"{BASE}/election-2024", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
    txt = pg.evaluate("()=>document.body.innerText").lower()
    print("\nELECTION-2024 page loaded chars:", len(txt))
    for ph in ("never","add together","do not add","separate","different record","three","sum","grain"):
        print(f"   '{ph}': {txt.count(ph)}x")
    b.close()
