"""Comprehensive dead-link sweep across the LIVE app.

Crawls every registered page, drills into the first detail/profile view per page
(following the first entity-bearing href), and flags any anchor whose internal
slug is NOT in the registered set (true dead link -> Streamlit 'page not found').

Run with a fresh server:  python _dead_link_sweep.py http://127.0.0.1:8645
"""
from __future__ import annotations
import sys, time, json
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"

# Registered url_path slugs (from utility/app.py st.Page(url_path=...)).
SLUGS = {
    "", "home", "what-they-own", "constituencies", "local-government",
    "rankings-council-spending", "housing", "member-overview",
    "rankings-attendance", "rankings-votes", "rankings-interests",
    "rankings-committees", "in-the-news", "rankings-payments",
    "rankings-election-spending", "rankings-procurement", "follow-the-money",
    "accommodation-spend", "rankings-public-payments", "company",
    "rankings-legislation", "rankings-statutory-instruments", "rankings-corporate",
    "rankings-judiciary", "rankings-lobbying", "rankings-ministerial-diaries",
    "rankings-appointments", "glossary", "election-2024",
}

PAGES = [
    "what-they-own","constituencies","local-government","rankings-council-spending",
    "housing","member-overview","rankings-attendance","rankings-votes",
    "rankings-committees","in-the-news","rankings-payments",
    "rankings-election-spending","rankings-procurement","follow-the-money",
    "accommodation-spend","rankings-public-payments","company","rankings-legislation",
    "rankings-statutory-instruments","rankings-corporate","rankings-judiciary",
    "rankings-lobbying","rankings-ministerial-diaries","rankings-appointments",
    "glossary","election-2024",
]
ENTITY_PARAMS = ("member=","supplier=","vote=","bill=","si=","committee=","fund=",
                 "firm=","ref=","lp3_org=","lp3_pol=","lp3_dpo=","paid_publisher=",
                 "paid_supplier=","authority=","la=","constituency=","council=",
                 "judge=","court=","cand=","dparty=","eparty=","org=","minister=")

def settle(pg, t=5.0):
    try: pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception: pass
    time.sleep(t)

def harvest(pg):
    return pg.evaluate("""()=>[...document.querySelectorAll('a')].map(a=>({href:a.getAttribute('href')||'',text:(a.textContent||'').trim().slice(0,50)}))""")

def check_hrefs(hrefs, page_label, dead, where):
    for a in hrefs:
        h = a["href"]
        if not h or h in ("#",) or h.startswith(("http","https","mailto:","?")):
            if h and ("None" in h or "/nan" in h or h.endswith("=nan") or "=None" in h):
                dead.append((page_label, h, a["text"], "null-ish"))
            continue
        if "None" in h or "/nan" in h or h.endswith("=nan") or "=None" in h:
            dead.append((page_label, h, a["text"], "null-ish")); continue
        slug = urlparse(h).path.lstrip("/").rstrip("/")
        if slug not in SLUGS:
            dead.append((page_label, h, a["text"], f"slug {slug!r} NOT registered"))
        where.setdefault(slug, 0)
        where[slug] += 1

def main():
    dead = []
    where = {}
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        pg = b.new_context(viewport={"width":1440,"height":2600}).new_page()
        for slug in PAGES:
            try:
                pg.goto(f"{BASE}/{slug}", wait_until="domcontentloaded", timeout=60000)
                settle(pg, 6)
                hrefs = harvest(pg)
            except Exception as e:
                print(f"  LOAD FAIL {slug}: {e}"); continue
            check_hrefs(hrefs, slug, dead, where)
            # drill into the FIRST entity-bearing detail link and re-scan
            drill = None
            for a in hrefs:
                if any(pm in a["href"] for pm in ENTITY_PARAMS):
                    drill = a["href"]; break
            if drill:
                dest = drill if drill.startswith("http") else f"{BASE}{drill if drill.startswith('/') else '/'+slug+drill}"
                try:
                    pg.goto(dest, wait_until="domcontentloaded", timeout=60000)
                    settle(pg, 6)
                    check_hrefs(harvest(pg), f"{slug}[detail]", dead, where)
                except Exception as e:
                    print(f"  DRILL FAIL {slug}: {e}")
        b.close()

    print("\n" + "="*78)
    print("DEAD / UNREGISTERED-SLUG LINKS")
    print("="*78)
    if not dead:
        print("  (none found)")
    else:
        seen = set()
        for pgl, href, text, why in dead:
            k = (href, why)
            if k in seen: continue
            seen.add(k)
            print(f"  [{pgl}]  {href!r}  ({why})  text={text!r}")
    print("\nInternal slug link tallies (sanity):")
    for s in sorted(where):
        mark = "" if s in SLUGS else "   <-- UNREGISTERED"
        print(f"  {s or '(root)':40} {where[s]}{mark}")
    print("\nJSON_DEAD:", json.dumps([{"page":p,"href":h,"why":w} for p,h,t,w in dead], ensure_ascii=False))

if __name__ == "__main__":
    main()
