"""Journalist 5-minute test, v2: grab body innerText and grep for the
cross-source signals a reporter would look for.

Cohort (real unique_member_codes verified against flattened_members.parquet):
- Michael Healy-Rae (Independent, Kerry) — Radio Kerry's 2024 CRO-directorship
  story makes him the canonical "undeclared interest" case study.
- Simon Harris (Taoiseach, Fine Gael) — signs SIs daily; most likely
  lobbying-then-SI candidate.
- Mary-Lou McDonald (Sinn Fein leader) — opposition heavyweight with
  rich question / vote / lobbying surfaces.

The test mimics a journalist:
  T+0:00  open profile
  T+0:30  scan hero + stat strip — what jumps out?
  T+1:00  open Interests — landlord? director? shareholder?
  T+2:00  open Lobbying — who lobbied this TD, on what?
  T+3:00  open Legislation / SIs — what did they sign / sponsor?
  T+4:00  check provenance footer — can I cite the source PDF?
  T+5:00  stop.

Output: structured findings + screenshots in _journalist_test/.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_journalist_test"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 1000}
PAGE_LOAD_WAIT_MS = 9000
EXPANDER_WAIT_MS = 2500


def shot(page: Page, name: str) -> Path:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=True)
    return path


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def dismiss_modal(page: Page) -> None:
    try:
        btn = page.locator('[data-testid="stDialog"] button').first
        if btn.is_visible(timeout=500):
            btn.click()
            wait(300)
    except Exception:
        pass


def open_member(page: Page, code: str) -> None:
    url = f"{BASE}/member-overview?member={quote(code, safe='')}"
    page.goto(url, wait_until="networkidle")
    wait(PAGE_LOAD_WAIT_MS)
    dismiss_modal(page)


def open_all_sections(page: Page) -> bool:
    """Toggle the 'Open all' switch; also click each expander summary as backup."""
    clicked_any = False
    try:
        labels = page.get_by_text(re.compile(r"Open all", re.I)).all()
        for lab in labels[:2]:
            try:
                lab.click()
                clicked_any = True
                wait(500)
            except Exception:
                continue
    except Exception:
        pass
    # backup: click any closed <summary> tags inside the main area
    try:
        summaries = page.locator('details:not([open]) > summary').all()
        for s in summaries:
            try:
                s.click()
                clicked_any = True
                wait(300)
            except Exception:
                continue
    except Exception:
        pass
    if clicked_any:
        wait(EXPANDER_WAIT_MS)
    return clicked_any


SIGNAL_PATTERNS = {
    "landlord": re.compile(r"landlord|rental property|properties? declared|properties?\s*$", re.I | re.M),
    "shareholder": re.compile(r"shareholder", re.I),
    "director": re.compile(r"director(ship)?|directorship", re.I),
    "occupation": re.compile(r"occupation", re.I),
    "land": re.compile(r"\bland\b|hectare|acre", re.I),
    "cro_link": re.compile(r"\bCRO\b|companies registration office|company reg", re.I),
    "charity": re.compile(r"\bchariti(es|y)\b|trustee", re.I),
    "lobbying_count": re.compile(r"(\d+)\s+(lobbying\s+)?(contact|return|meeting)s?\b", re.I),
    "lobbied_by": re.compile(r"lobbied by|met with|on behalf of", re.I),
    "policy_area": re.compile(r"policy area|sector|theme", re.I),
    "si_signed": re.compile(r"statutory instrument|\bSI\b\s|signed", re.I),
    "bill_sponsor": re.compile(r"sponsored|sponsor of|sponsored bill", re.I),
    "minister_office": re.compile(r"minister", re.I),
    "pdf_link": re.compile(r"\.pdf", re.I),
    "source_caption": re.compile(r"source[:\s]|provenance", re.I),
    "oireachtas_link": re.compile(r"oireachtas\.ie|data\.oireachtas\.ie|lobbying\.ie|irisoifigiuil", re.I),
    "iris_link": re.compile(r"iris\s*oifig|irisoifigiuil\.ie", re.I),
    "wikipedia": re.compile(r"wikipedia", re.I),
    "x_twitter": re.compile(r"\bX\b\s|twitter|x\.com", re.I),
}


def hunt_signals(body: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for key, pat in SIGNAL_PATTERNS.items():
        hits = pat.findall(body)
        if not hits:
            continue
        # findall returns groups for grouped patterns — normalise to strings
        flat: list[str] = []
        for h in hits[:5]:
            if isinstance(h, tuple):
                h = " ".join(x for x in h if x)
            flat.append(str(h).strip())
        out[key] = flat
    return out


def collect_pdf_links(page: Page) -> list[str]:
    out: list[str] = []
    for a in page.locator('a[href*=".pdf"]').all():
        try:
            href = a.get_attribute("href") or ""
            label = (a.inner_text() or "").strip().replace("\n", " ")[:80]
            if href:
                out.append(f"{label} -> {href}")
        except Exception:
            continue
    return out[:10]


def run_test(name: str, code: str) -> dict:
    t0 = time.time()
    findings: dict = {"name": name, "code": code}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()

        open_member(page, code)
        shot(page, f"{name}_01_landed")
        findings["t_landed_s"] = round(time.time() - t0, 1)

        open_all_sections(page)
        wait(2000)
        shot(page, f"{name}_02_all_open")
        findings["t_expanded_s"] = round(time.time() - t0, 1)

        # Grab the full main-area text
        try:
            body = page.locator('main').inner_text() or ""
        except Exception:
            body = page.locator('body').inner_text() or ""

        findings["body_chars"] = len(body)
        findings["signals"] = hunt_signals(body)
        findings["pdf_links"] = collect_pdf_links(page)

        # Save full text dump for forensics
        (OUT / f"{name}_body.txt").write_text(body, encoding="utf-8")

        shot(page, f"{name}_03_full")
        browser.close()

    findings["t_total_s"] = round(time.time() - t0, 1)
    return findings


def report(r: dict) -> None:
    if "error" in r:
        print(f"[{r['name']}] ERROR: {r['error']}")
        return
    print(f"\n=== {r['name']} ({r['code']}) ===")
    print(f"  body: {r['body_chars']:,} chars")
    print(f"  timing: landed={r['t_landed_s']}s, expanded={r['t_expanded_s']}s, total={r['t_total_s']}s")
    print(f"  pdf links surfaced: {len(r['pdf_links'])}")
    for link in r["pdf_links"][:4]:
        print(f"    - {link}")
    print(f"  signals found ({len(r['signals'])}/{len(SIGNAL_PATTERNS)}):")
    for k in sorted(r["signals"]):
        sample = r["signals"][k][0][:60]
        print(f"    [{k:<16}] x{len(r['signals'][k])}  e.g. {sample!r}")
    print(f"  signals MISSING:")
    for k in sorted(set(SIGNAL_PATTERNS) - set(r["signals"])):
        print(f"    - {k}")


if __name__ == "__main__":
    cohort = [
        ("healyrae", "Michael-Healy-Rae.D.2011-03-09"),
        ("harris",   "Simon-Harris.D.2011-03-09"),
        ("mcdonald", "Mary-Lou-McDonald.D.2011-03-09"),
    ]
    results = []
    for name, code in cohort:
        print(f"\n>>> Running test on {name}")
        try:
            results.append(run_test(name, code))
        except Exception as e:
            results.append({"name": name, "code": code, "error": str(e)})

    print("\n\n" + "=" * 60)
    print("REPORT")
    print("=" * 60)
    for r in results:
        report(r)
