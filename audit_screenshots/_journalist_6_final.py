"""Six 5-min journalist missions — Playwright only, full UI interaction.

Each mission drives the app like a real reporter would: clicks the same
tabs, types into the same search boxes, applies the same filters. No SQL
shortcuts. Captures what the surface actually delivers in a 5-min window.

Output: screenshots in _journalist_6_final/ + concise per-mission notes
to stdout. Bug observations are folded into the per-mission report.
"""
from __future__ import annotations
import json, re, sys, time
from pathlib import Path
from urllib.parse import quote
from playwright.sync_api import Page, sync_playwright

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.stdout.reconfigure(encoding="utf-8")

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_journalist_6_final"
OUT.mkdir(parents=True, exist_ok=True)

VIEWPORT = {"width": 1440, "height": 1200}
SETTLE = 6
RERUN = 3  # after a filter click


def settle(page: Page, s: int = SETTLE) -> None:
    time.sleep(s)
    try:
        btn = page.locator('[data-testid="stDialog"] button').first
        if btn.is_visible(timeout=300):
            btn.click(); time.sleep(0.3)
    except Exception:
        pass


def body(page: Page) -> str:
    return page.locator('[data-testid="stApp"]').inner_text()


def shot(page: Page, name: str) -> None:
    page.screenshot(path=str(OUT / f"{name}.png"), full_page=True)


def click_text(page: Page, pattern: str, exact: bool = False) -> bool:
    try:
        loc = (page.get_by_text(pattern, exact=exact) if isinstance(pattern, str)
               else page.get_by_text(pattern))
        loc.first.click(timeout=4000)
        time.sleep(RERUN)
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Mission 1 — Financial story (all-time PSA top)
# ─────────────────────────────────────────────────────────────────────────────
def mission_finance(page: Page) -> dict:
    page.goto(f"{BASE}/rankings-payments", wait_until="domcontentloaded")
    settle(page)
    # Click the "Rankings" tab to get all-time view
    clicked = click_text(page, "Rankings", exact=True)
    shot(page, "1_finance")
    txt = body(page)
    # Headline: total since 2020, top 3 with amounts
    total_match = re.search(r"€([\d,]+).{0,40}Total since 2020", txt, re.S)
    top = re.findall(r"#(\d+)\s+([A-Z][^\n]{1,60})\nDeputy\n[^\n]+\n.*?€([\d,]+)\s*total", txt, re.S)
    return {
        "clicked_rankings_tab": clicked,
        "total_since_2020": total_match.group(1) if total_match else None,
        "top_3": [(r[0], r[1].strip(), r[2]) for r in top[:3]],
        "url": "/rankings-payments → Rankings tab",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Mission 2 — Political story (contested vote)
# ─────────────────────────────────────────────────────────────────────────────
def mission_politics(page: Page) -> dict:
    page.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded")
    settle(page)
    shot(page, "2_politics_landing")
    txt = body(page)
    # Pull the first division headline + outcome
    # Format on the page: "Confidence in Government: Motion" with vote counts +
    # date alongside.
    divisions = re.findall(
        r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+202\d)[^\n]*\n([^\n]+?)\n.*?(Carried|Lost|Tied|Defeated)",
        txt, re.S,
    )
    # Try clicking a division to drill in (most likely card link)
    first_card_text = divisions[0][1] if divisions else None
    drill_in = False
    if first_card_text:
        # Try clicking the first ✓/✗ tally — usually the division title is the link
        try:
            page.get_by_text(first_card_text.strip()[:40], exact=False).first.click(timeout=4000)
            time.sleep(RERUN)
            drill_in = True
            shot(page, "2_politics_drilled")
        except Exception:
            pass
    return {
        "first_divisions": divisions[:5],
        "drilled_into_division": drill_in,
        "url": "/rankings-votes",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Mission 3 — Greyhound lobbying (search box + policy area browse)
# ─────────────────────────────────────────────────────────────────────────────
def mission_greyhound(page: Page) -> dict:
    page.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded")
    settle(page)
    notes = {"url": "/rankings-lobbying"}

    # Path A: type "greyhound" into SEARCH THE REGISTER + Enter
    try:
        inputs = page.locator('input[type="text"], input[role="combobox"]').all()
        if inputs:
            inputs[0].click()
            inputs[0].fill("greyhound")
            time.sleep(2)
            shot(page, "3a_typed_greyhound")
            # try Enter to submit
            inputs[0].press("Enter")
            time.sleep(RERUN)
            shot(page, "3b_after_enter")
            after = body(page)
            notes["after_enter_hits"] = after.lower().count("greyhound")
            # extract any organisation rows mentioning greyhound
            grey_lines = [ln for ln in after.splitlines() if "greyhound" in ln.lower()]
            notes["greyhound_lines"] = grey_lines[:6]
    except Exception as e:
        notes["search_error"] = str(e)[:200]

    # Path B: browse-by-policy-area — find the tile and click
    page.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded")
    settle(page)
    try:
        # The three-card gateway: "Browse by policy area"
        page.get_by_text("Browse by policy area", exact=False).first.click(timeout=4000)
        time.sleep(RERUN)
        shot(page, "3c_policy_area_gateway")
        area_txt = body(page)
        # Try to find a policy area whose name contains 'sport' or 'agric' or
        # 'recreation' (greyhound likely sits there)
        notes["policy_areas_listed"] = bool(re.search(r"Sport|Recreation|Agriculture|Animals", area_txt, re.I))
        # Take all visible area labels
        notes["all_areas_sample"] = re.findall(r"\b([A-Z][a-zA-Z &]{5,40})\b", area_txt)[:30]
    except Exception as e:
        notes["browse_error"] = str(e)[:200]

    return notes


# ─────────────────────────────────────────────────────────────────────────────
# Mission 4 — Immigration via McDonald's profile
# ─────────────────────────────────────────────────────────────────────────────
def mission_immigration(page: Page) -> dict:
    code = "Mary-Lou-McDonald.D.2011-03-09"
    page.goto(f"{BASE}/member-overview?member={quote(code, safe='')}", wait_until="domcontentloaded")
    settle(page)
    # Open all sections so Questions renders
    click_text(page, re.compile(r"Open all", re.I))
    shot(page, "4_mcdonald_open_all")

    # Try to find the All ministries selector and switch to Justice/Migration
    notes = {"url": f"/member-overview?member={code}"}
    txt_before = body(page)
    notes["before_filter_total_questions"] = re.search(r"Showing 1[\-–]\d+ of ([\d,]+) questions", txt_before)
    notes["before_filter_total_questions"] = notes["before_filter_total_questions"].group(1) if notes["before_filter_total_questions"] else None

    # Click the ministry filter (Streamlit selectbox or pill row)
    filtered = False
    try:
        # Try interacting with a selectbox whose displayed value is "All ministries"
        page.get_by_text("All ministries", exact=False).first.click(timeout=4000)
        time.sleep(1)
        # Try typing or clicking the migration option
        # Streamlit selectbox opens a dropdown — search for a Justice/Migration option
        for label in ("Justice, Home Affairs and Migration", "Justice", "Migration", "Home Affairs"):
            try:
                page.get_by_text(label, exact=False).first.click(timeout=2500)
                filtered = True
                break
            except Exception:
                continue
        time.sleep(RERUN)
        shot(page, "4b_mcdonald_filtered")
    except Exception as e:
        notes["filter_error"] = str(e)[:200]

    after = body(page)
    notes["after_filter"] = bool(filtered)
    notes["after_filter_total"] = re.search(r"Showing 1[\-–]\d+ of ([\d,]+) questions", after)
    notes["after_filter_total"] = notes["after_filter_total"].group(1) if notes["after_filter_total"] else None
    # Sample 5 question headers post-filter
    sample = re.findall(r"\b(asked the Minister[^.]{20,200})", after)
    notes["sample_questions"] = sample[:5]
    return notes


# ─────────────────────────────────────────────────────────────────────────────
# Mission 5 — Galway TD record
# ─────────────────────────────────────────────────────────────────────────────
def mission_galway(page: Page) -> dict:
    # Reporter path: open Member Overview, type "Galway" into find-a-TD
    page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded")
    settle(page)
    shot(page, "5a_browse_all")
    # The /member-overview index page lists all TDs as cards. Looking for a
    # constituency filter or text input. Try typing into any visible text input.
    notes = {"url": "/member-overview (browse) → pick a Galway TD"}
    try:
        inputs = page.locator('input[type="text"], input[role="combobox"]').all()
        if inputs:
            inputs[0].click()
            inputs[0].fill("Galway")
            time.sleep(2)
            shot(page, "5b_galway_typed")
            sugg = body(page)
            # Count any Galway TD suggestions visible
            galway_hits = [ln for ln in sugg.splitlines() if "galway" in ln.lower()][:6]
            notes["galway_suggestions"] = galway_hits
    except Exception as e:
        notes["typeahead_error"] = str(e)[:200]

    # Direct path: open Catherine Connolly (Galway West, Independent)
    code = "Catherine-Connolly.D.2016-10-03"
    page.goto(f"{BASE}/member-overview?member={quote(code, safe='')}", wait_until="domcontentloaded")
    settle(page)
    click_text(page, re.compile(r"Open all", re.I))
    shot(page, "5c_connolly")
    cf = body(page)
    notes["connolly_constituency"] = bool(re.search(r"Galway", cf))
    # pull lobbying #1 org targeting her
    m = re.search(r"ORGANISATIONS LOBBYING THIS POLITICIAN[\s\S]{0,80}#1\s*([^\n]+)", cf)
    notes["top_org_lobbying_connolly"] = m.group(1).strip() if m else None
    # PQs count
    q = re.search(r"Showing 1[\-–]\d+ of ([\d,]+) questions", cf)
    notes["connolly_pq_total"] = q.group(1) if q else None
    return notes


# ─────────────────────────────────────────────────────────────────────────────
# Mission 6 — Property declarations of the all-time #2 PSA TD
# ─────────────────────────────────────────────────────────────────────────────
def mission_property(page: Page) -> dict:
    # Step A: rankings page → click Rankings tab → identify #2
    page.goto(f"{BASE}/rankings-payments", wait_until="domcontentloaded")
    settle(page)
    click_text(page, "Rankings", exact=True)
    shot(page, "6a_rankings_alltime")
    rank_txt = body(page)
    second = re.search(r"#2\s+([A-Z][^\n]{1,60})", rank_txt)
    second_name = second.group(1).strip() if second else None

    # Step B: click on #2's card to navigate to their profile
    notes = {"second_name": second_name, "second_card_clickable": False}
    if second_name:
        try:
            page.get_by_text(second_name.split("\n")[0], exact=False).first.click(timeout=4000)
            time.sleep(RERUN)
            notes["second_card_clickable"] = True
            shot(page, "6b_clicked_through")
        except Exception:
            pass

    # Step C: if click-through worked, body should be a Member Overview now.
    # Either way, open the known Danny Healy-Rae profile to count properties.
    code = "Danny-Healy-Rae.D.2016-10-03"
    page.goto(f"{BASE}/member-overview?member={quote(code, safe='')}", wait_until="domcontentloaded")
    settle(page)
    click_text(page, re.compile(r"Open all", re.I))
    shot(page, "6c_danny_interests")
    bod = body(page)
    m = re.search(r"LAND & PROPERTY\s*[·\-]\s*(\d+)([\s\S]+?)(?:CONTRACTS|NOTHING DECLARED|EXPORT|LOBBYING)", bod)
    if m:
        notes["land_property_count_declared"] = m.group(1)
        notes["land_property_block"] = m.group(2).strip()[:1200]
    notes["url"] = f"/member-overview?member={code}"
    return notes


# ─────────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────────


MISSIONS = [
    ("1_finance",     mission_finance),
    ("2_politics",    mission_politics),
    ("3_greyhound",   mission_greyhound),
    ("4_immigration", mission_immigration),
    ("5_galway",      mission_galway),
    ("6_property",    mission_property),
]


def main():
    out: list[dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()
        page.set_default_timeout(20000)
        for name, fn in MISSIONS:
            t0 = time.time()
            print(f"\n=== {name} ===")
            try:
                r = fn(page)
            except Exception as e:
                r = {"error": str(e)[:200]}
            r["elapsed_s"] = round(time.time() - t0, 1)
            out.append({"name": name, **r})
            for k, v in r.items():
                if k == "elapsed_s": continue
                print(f"  {k}: {str(v)[:300]}")
            print(f"  [{r['elapsed_s']}s]")
        ctx.close()
        browser.close()
    (OUT / "results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nResults: {OUT}/results.json")


if __name__ == "__main__":
    main()
