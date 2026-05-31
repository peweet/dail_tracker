"""Second pass: drill into the gaps from the first 6-mission run."""
from __future__ import annotations
import json, re, sys, time
from pathlib import Path
from urllib.parse import quote
from playwright.sync_api import sync_playwright

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.stdout.reconfigure(encoding="utf-8")

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_journalist_6"
SETTLE = 6


def settle(page):
    time.sleep(SETTLE)
    try:
        btn = page.locator('[data-testid="stDialog"] button').first
        if btn.is_visible(timeout=300):
            btn.click(); time.sleep(0.3)
    except Exception:
        pass


def body(page):
    return page.locator('[data-testid="stApp"]').inner_text()


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 1200})
        page = ctx.new_page()
        page.set_default_timeout(20000)

        # ── 1. All-time payments ranking (click Rankings tab) ─────────────────
        print("\n--- all-time rankings tab ---")
        page.goto(f"{BASE}/rankings-payments", wait_until="domcontentloaded")
        settle(page)
        try:
            page.get_by_text("Rankings", exact=True).first.click()
            time.sleep(3)
        except Exception as e:
            print(f"  click Rankings tab failed: {e}")
        page.screenshot(path=str(OUT / "6c_alltime_rankings.png"), full_page=True)
        txt = body(page)
        # Top 5
        top5 = re.findall(r"#(\d+)\s+([A-Z][^\n]{1,60}?)\n([^\n]+?)\n.*?€([\d,]+)\s*total", txt[:6000], re.S)
        print(f"  parsed top entries: {len(top5)}")
        for hit in top5[:6]:
            print(f"  #{hit[0]:<3} {hit[1]:<30} {hit[2][:50]:<50} €{hit[3]}")

        # ── 2. Lobbying search box for 'greyhound' ────────────────────────────
        print("\n--- lobbying: type 'greyhound' into search ---")
        page.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded")
        settle(page)
        try:
            # First text input on the page is the typeahead
            search = page.locator('input[type="text"], input[role="combobox"]').first
            search.click()
            search.fill("greyhound")
            time.sleep(2)
            page.screenshot(path=str(OUT / "3d_lobbying_typed_greyhound.png"), full_page=True)
            # Look for any dropdown / autocomplete suggestions
            body_after = body(page)
            n_grey = body_after.lower().count("greyhound")
            print(f"  greyhound mentions after typing: {n_grey}")
            print(f"  visible suggestions snippet: {body_after[body_after.lower().find('greyhound'):][:400] if n_grey else '(none)'}")
        except Exception as e:
            print(f"  search interaction failed: {e}")

        # ── 3. McDonald immigration — click Questions tab + ministry filter ───
        print("\n--- McDonald deeper: open Questions section ---")
        code = "Mary-Lou-McDonald.D.2011-03-09"
        page.goto(f"{BASE}/member-overview?member={quote(code, safe='')}", wait_until="domcontentloaded")
        settle(page)
        # Try clicking "Open all sections" toggle, then look for Justice/Home Affairs ministry
        try:
            page.get_by_text(re.compile(r"Open all", re.I)).first.click()
            time.sleep(3)
        except Exception:
            pass
        page.screenshot(path=str(OUT / "4b_mcdonald_open_all.png"), full_page=True)
        full = body(page)
        # Count immigration-y question lines
        imm = [ln for ln in full.splitlines() if re.search(r"\b(migrat|asylum|refugee|naturalisation|protection bill|international protection)\b", ln, re.I)]
        print(f"  immigration-related lines on profile: {len(imm)}")
        for ln in imm[:8]:
            print(f"   - {ln[:160]}")

        # ── 4. Second-richest TD's property: visit Danny Healy-Rae profile ────
        # Per the 2025 snapshot #2 is Danny Healy-Rae. For the all-time #2 we'd
        # need to read the rankings tab properly — try both.
        print("\n--- Danny Healy-Rae interests (2025 #2 payments) ---")
        code = "Danny-Healy-Rae.D.2016-10-03"
        page.goto(f"{BASE}/member-overview?member={quote(code, safe='')}", wait_until="domcontentloaded")
        settle(page)
        try:
            page.get_by_text(re.compile(r"Open all", re.I)).first.click()
            time.sleep(3)
        except Exception:
            pass
        page.screenshot(path=str(OUT / "6d_danny_healyrae_interests.png"), full_page=True)
        d_full = body(page)
        # extract property block
        m = re.search(r"LAND & PROPERTY\s*[·\-]\s*(\d+)([\s\S]+?)(?:CONTRACTS|NOTHING DECLARED|EXPORT|LOBBYING)", d_full)
        if m:
            n_props = m.group(1)
            block = m.group(2).strip()[:1500]
            print(f"  Land & Property count declared: {n_props}")
            print(f"  ---\n{block}\n---")
        else:
            print("  no LAND & PROPERTY section found in body")
            # fall back: any "properties" / "landlord" markers
            for ln in d_full.splitlines():
                if "Landlord" in ln or "propert" in ln.lower():
                    print(f"  pertinent: {ln[:160]}")

        # ── 5. Votes page: just dump first 60 lines to understand format ──────
        print("\n--- votes page format inspection ---")
        page.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded")
        settle(page)
        v_full = body(page)
        page.screenshot(path=str(OUT / "2b_votes_landing.png"), full_page=True)
        print("\n".join(v_full.splitlines()[:60]))

        ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
