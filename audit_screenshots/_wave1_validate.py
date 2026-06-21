"""Validate Wave-1 dead-link fixes on a FRESH server.
  1. SI enabling-Act link now points at /rankings-legislation?bill= and resolves;
     pre-2014 (act_) SIs show NO broken 'View ... detail' link.
  2. follow-the-money group node no longer contains 'sum sum'.
  3. constituency council-spending link is well-formed (/rankings-council-spending?paid_publisher=).
Run: python _wave1_validate.py http://127.0.0.1:8645
"""
from __future__ import annotations
import sys, time
from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"

def settle(pg, t=6.0):
    try: pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception: pass
    time.sleep(t)
def hrefs(pg):
    return pg.evaluate("""()=>[...document.querySelectorAll('a')].map(a=>({href:a.getAttribute('href')||'',text:(a.textContent||'').trim().slice(0,40)}))""")

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width":1440,"height":2600}).new_page()

    # 1. SI: scan several SI details for a 'View ... detail' link, assert slug.
    pg.goto(f"{BASE}/rankings-statutory-instruments", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
    si = list(dict.fromkeys(a["href"] for a in hrefs(pg) if "si=" in a["href"]))[:14]
    post2014_ok = pre2014_clean = None
    bad_legacy = []
    checked = 0
    for h in si:
        d = h if h.startswith("http") else f"{BASE}{h if h.startswith('/') else '/rankings-statutory-instruments'+h}"
        pg.goto(d, wait_until="domcontentloaded", timeout=60000); settle(pg,4)
        view = [a for a in hrefs(pg) if "View " in a["text"] and "detail" in a["text"]]
        legacy = [a for a in hrefs(pg) if a["href"].startswith("/legislation?")]
        if legacy: bad_legacy.append((h, [a["href"] for a in legacy]))
        for a in view:
            checked += 1
            slug = a["href"].lstrip("/").split("?")[0]
            if slug == "rankings-legislation": post2014_ok = a["href"]
        if post2014_ok and checked >= 3: break
    print(f"SI: checked {checked} 'View detail' links; resolves-slug-example={post2014_ok!r}; "
          f"legacy /legislation? still present={bad_legacy if bad_legacy else 'NONE'}")
    if post2014_ok:
        pg.goto(f"{BASE}{post2014_ok}", wait_until="domcontentloaded", timeout=60000); settle(pg,6)
        t = pg.evaluate("()=>document.body.innerText")
        nf = any(s in t.lower() for s in ("not found","isn't in","not in the index"))
        print(f"SI link end-to-end: {post2014_ok} -> notfound={nf}, chars={len(t)}")

    # 2. follow-the-money group node text
    pg.goto(f"{BASE}/follow-the-money", wait_until="domcontentloaded", timeout=60000); settle(pg,6)
    grp = [a["href"] for a in hrefs(pg) if "group" in a["href"].lower()]
    txt = pg.evaluate("()=>document.body.innerText")
    if grp and "sum sum" not in txt.lower():
        d = grp[0]; d = d if d.startswith("http") else f"{BASE}{d if d.startswith('/') else '/follow-the-money'+d}"
        pg.goto(d, wait_until="domcontentloaded", timeout=60000); settle(pg,6)
        txt = pg.evaluate("()=>document.body.innerText")
    print(f"FTM: 'sum sum' present={'sum sum' in txt.lower()} (followed group={bool(grp)}); "
          f"'sum-safe euros totalled' present={'sum-safe euros totalled' in txt.lower()}")

    # 3. constituency council-spending link well-formed
    pg.goto(f"{BASE}/constituencies", wait_until="domcontentloaded", timeout=60000); settle(pg,7)
    con = [a["href"] for a in hrefs(pg) if "constituency=" in a["href"]]
    cs_links = []
    if con:
        d = con[0]; d = d if d.startswith("http") else f"{BASE}{d if d.startswith('/') else '/constituencies'+d}"
        pg.goto(d, wait_until="domcontentloaded", timeout=60000); settle(pg,7)
        cs_links = [a["href"] for a in hrefs(pg) if "rankings-council-spending?paid_publisher=" in a["href"]]
    print(f"CONSTITUENCY: council-spending links found={len(cs_links)}; example={cs_links[0] if cs_links else None}")
    b.close()
print("DONE")
