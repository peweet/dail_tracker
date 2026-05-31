"""Six 5-minute journalist missions. Drive the app, extract the story, log gaps.

Mission rules:
- The reporter does NOT touch the database. They only have the app.
- Five-minute budget per story (assessed by counting how many clicks /
  page-loads / filters the answer requires, not by real wall-clock).
- A story 'lands' if the reporter can quote a publishable headline fact +
  cite a primary source link (PDF, lobbying.ie return, etc).

Six missions:
  1) Financial: who's the highest-paid TD, by how much, doing what
  2) Political: what's the most recent contested division, who broke ranks
  3) Lobbying: greyhound racing - who's pushing it, who's being lobbied
  4) Immigration: a named TD's stance via PQs / vote record on immigration
  5) Galway: a current Galway TD's accountability picture
  6) Property: how many properties does the 2nd-highest-paid TD declare

Each mission:
- Goes to the most appropriate entry surface
- Pulls body text + screenshot
- Records the headline + the gap
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
OUT = Path(__file__).resolve().parent / "_journalist_6"
OUT.mkdir(parents=True, exist_ok=True)

VIEWPORT = {"width": 1440, "height": 1000}
SETTLE = 6  # seconds after networkidle


def settle(page: Page) -> None:
    time.sleep(SETTLE)
    try:
        btn = page.locator('[data-testid="stDialog"] button').first
        if btn.is_visible(timeout=300):
            btn.click(); time.sleep(0.3)
    except Exception:
        pass


def body(page: Page) -> str:
    try:
        return page.locator('[data-testid="stApp"]').inner_text()
    except Exception:
        return ""


def go(page: Page, path: str) -> None:
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=20000)
    settle(page)


def shot(page: Page, name: str) -> None:
    page.screenshot(path=str(OUT / f"{name}.png"), full_page=True)


# ── Mission helpers ───────────────────────────────────────────────────────────


def mission_1_finance(page: Page) -> dict:
    """Route: /rankings-payments → top of ranking → publishable headline."""
    go(page, "/rankings-payments")
    txt = body(page)
    shot(page, "1_finance")
    # Top of ranking; the page renders cards #1, #2, #3...
    top = re.search(r"#1\s+(.+?)\s+(?:Deputy|Senator|TD)?\s*Band\s*(\d+).*?€([\d,]+)\s*total", txt, re.S)
    return {
        "url": "/rankings-payments",
        "headline": top.group(0)[:200] if top else "(could not parse top card)",
        "body_chars": len(txt),
    }


def mission_2_politics(page: Page) -> dict:
    """Route: /rankings-votes → most recent contested division → who broke ranks."""
    go(page, "/rankings-votes")
    txt = body(page)
    shot(page, "2_politics")
    # Try to find a recent division row
    div = re.search(r"(\d+\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+202\d).{0,80}?(Carried|Lost|Tied|Defeated)", txt, re.S)
    return {
        "url": "/rankings-votes",
        "headline": (div.group(0)[:200] if div else "(no division row matched)"),
        "body_chars": len(txt),
    }


def mission_3_greyhound(page: Page) -> dict:
    """Route: /rankings-lobbying → look for greyhound. The lobbying page has
    a topic search; we'll try to land on a topic-results URL via query param."""
    # First try the topic-search entry
    go(page, "/rankings-lobbying?topic=greyhound")
    txt1 = body(page)
    shot(page, "3a_lobbying_topic_greyhound")
    # Also try a free-text policy area
    go(page, "/rankings-lobbying?area=greyhound")
    txt2 = body(page)
    shot(page, "3b_lobbying_area_greyhound")
    # And the bare lobbying page (to see if it surfaces a topic taxonomy at all)
    go(page, "/rankings-lobbying")
    txt3 = body(page)
    shot(page, "3c_lobbying_landing")
    found = {
        "topic_param_hits": txt1.lower().count("greyhound"),
        "area_param_hits": txt2.lower().count("greyhound"),
        "landing_hits": txt3.lower().count("greyhound"),
    }
    return {
        "url": "/rankings-lobbying?topic=greyhound",
        "headline": f"greyhound mentions: {found}",
        "body_chars": len(txt3),
    }


def mission_4_immigration(page: Page) -> dict:
    """Mary Lou McDonald's questions on immigration, via Member Overview."""
    # Open her profile and the questions tab
    code = "Mary-Lou-McDonald.D.2011-03-09"
    go(page, f"/member-overview?member={quote(code, safe='')}")
    # try to filter the questions table by ministry
    txt = body(page)
    shot(page, "4_mcdonald_profile")
    # count what's visible
    immigration_lines = [
        ln for ln in txt.splitlines()
        if re.search(r"\b(migrat|asylum|naturalisation|refugee|justice.*home.*affairs.*migration)\b", ln, re.I)
    ]
    return {
        "url": f"/member-overview?member={code}",
        "headline": f"Lines mentioning immigration on McDonald profile: {len(immigration_lines)}",
        "sample": immigration_lines[:5],
        "body_chars": len(txt),
    }


def mission_5_galway(page: Page) -> dict:
    """Find a Galway TD. Try the Member Overview browse-all surface first."""
    go(page, "/member-overview")
    txt = body(page)
    shot(page, "5a_browse_all")
    # Then go directly to a known Galway TD: Catherine Connolly (Independent, Galway West)
    code = "Catherine-Connolly.D.2016-10-03"
    go(page, f"/member-overview?member={quote(code, safe='')}")
    txt2 = body(page)
    shot(page, "5b_connolly_profile")
    galway_lines = [ln for ln in txt2.splitlines() if "galway" in ln.lower()][:5]
    return {
        "url": f"/member-overview?member={code}",
        "headline": f"Galway present on Connolly profile? -> {'yes' if 'Galway' in txt2 else 'no'}",
        "sample": galway_lines,
        "body_chars": len(txt2),
    }


def mission_6_property(page: Page) -> dict:
    """Step 1: identify #2-ranked TD by all-time payments. Step 2: open their
    interests profile and count property declarations."""
    go(page, "/rankings-payments")
    rank_txt = body(page)
    shot(page, "6a_rankings")
    # The list orders #1, #2, #3 ...
    second = re.search(r"#2\s+([A-Z][\w'\-\.\sŁŚŻáéíóúÁÉÍÓÚñÑÉÍóàü]+?)\s+(?:Deputy|Senator|TD|Independent)", rank_txt)
    second_name = second.group(1).strip() if second else None
    return_data = {"second_ranked_raw": second.group(0)[:120] if second else None, "name_extracted": second_name}

    # Without an automated way to map name->code, drive the Member Overview by
    # clicking the TD's card link. Easier path: visit /member-overview (browse)
    # and let the reporter pick their name. We capture what the rankings page
    # actually exposes as a link.
    return_data["link_to_profile_visible"] = "/member-overview" in rank_txt or "member-overview?member=" in rank_txt
    return return_data


# ── Driver ────────────────────────────────────────────────────────────────────


MISSIONS = [
    ("1_finance",     mission_1_finance),
    ("2_politics",    mission_2_politics),
    ("3_greyhound",   mission_3_greyhound),
    ("4_immigration", mission_4_immigration),
    ("5_galway",      mission_5_galway),
    ("6_property",    mission_6_property),
]


def main():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()
        page.set_default_timeout(20000)
        for name, fn in MISSIONS:
            t0 = time.time()
            print(f"\n--- MISSION {name} ---")
            try:
                r = fn(page)
                r["elapsed_s"] = round(time.time() - t0, 1)
                r["status"] = "ran"
            except Exception as e:
                r = {"status": "failed", "error": str(e)[:200], "elapsed_s": round(time.time() - t0, 1)}
            results.append({"name": name, **r})
            for k, v in r.items():
                if k in ("status", "elapsed_s"): continue
                preview = v if isinstance(v, (int, float)) else (str(v)[:200])
                print(f"  {k}: {preview}")
        ctx.close()
        browser.close()
    (OUT / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n" + "=" * 60)
    print("Wrote results.json + screenshots to", OUT)


if __name__ == "__main__":
    main()
