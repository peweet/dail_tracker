"""Crawl every current TD's Member Overview profile, looking for bugs.

For each unique_member_code in flattened_members.parquet:
  - Visit /member-overview?member=<code>
  - Wait for networkidle + 4s for sections to render
  - Extract body innerText
  - Pattern-match for bug markers (Page-not-found modal, render exceptions,
    "None"/"NaN"/"undefined" literals, missing-section banners on TDs that
    should have data)
  - Note bugs only; successful renders silent

Output: bug list to stdout + a JSONL log in _crawl_176/.
Screenshots ONLY for pages where a bug was detected, named by member code.
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_crawl_176"
OUT.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT / "bugs.jsonl"

VIEWPORT = {"width": 1440, "height": 1000}
HERO_TIMEOUT_MS = 20000  # wait for the hero to render before extracting body
EXTRA_SETTLE_S = 1.5  # tiny buffer for downstream sections after hero appears

# Bug patterns: each pattern triggers a finding with a label.
BUG_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    ("page_not_found", re.compile(r"We couldn'?t find this TD|Page not found", re.I), "Page-not-found modal/banner"),
    ("python_traceback", re.compile(r"Traceback \(most recent call last\)|RuntimeError|KeyError|TypeError|AttributeError", re.I), "Python exception leaked into page"),
    ("streamlit_error", re.compile(r"StreamlitAPIException|StreamlitDuplicateElementId", re.I), "Streamlit framework error"),
    ("literal_none", re.compile(r"(?<![\w/])None(?![\w/])", re.M), "Literal 'None' rendered as text"),
    ("literal_nan", re.compile(r"\bNaN\b", re.M), "Literal 'NaN' rendered as text"),
    ("literal_undefined", re.compile(r"\bundefined\b"), "Literal 'undefined' rendered as text"),
    ("sql_error", re.compile(r"Catalog Error|Binder Error|Parser Error", re.I), "Raw DuckDB error in DOM"),
    ("nat_date", re.compile(r"\bNaT\b"), "Pandas NaT date rendered as text"),
]


def detect_bugs(body: str) -> list[dict]:
    findings = []
    for key, pat, label in BUG_PATTERNS:
        m = pat.search(body)
        if not m:
            continue
        # capture a small context window
        i = m.start()
        snippet = body[max(0, i - 40):i + 80].replace("\n", " ⏎ ")
        findings.append({"bug": key, "label": label, "context": snippet})
    return findings


_PARTY_RE = re.compile(
    r"(Fianna\s*F[áa]il|Fine\s*Gael|Sinn\s*F[ée]in|Labour|Green\s*Party|Social\s*Democrats?|Independent(?:\s+Ireland)?|People\s*Before\s*Profit|Aont[úu]|Solidarity)",
    re.IGNORECASE,
)


def has_hero(body: str) -> bool:
    """Hero is present iff the body carries a party label.

    The party · constituency line uses a UTF-8 middle dot (U+00B7); we don't
    rely on the separator (it changes per breakpoint) — just on the party
    name appearing somewhere in the body, which only the rendered hero emits.
    """
    return bool(_PARTY_RE.search(body))


def dismiss_modal(page: Page) -> bool:
    try:
        btn = page.locator('[data-testid="stDialog"] button').first
        if btn.is_visible(timeout=300):
            btn.click()
            time.sleep(0.3)
            return True
    except Exception:
        pass
    return False


def main():
    members = pl.read_parquet("data/silver/parquet/flattened_members.parquet").select(
        ["unique_member_code", "full_name", "party"]
    ).unique(subset=["unique_member_code"]).sort("full_name")
    codes = members.to_dicts()
    print(f"crawling {len(codes)} member profiles…\n")

    bug_log: list[dict] = []
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()
        page.set_default_timeout(15000)

        for i, m in enumerate(codes, 1):
            code = m["unique_member_code"]
            name = m["full_name"]
            url = f"{BASE}/member-overview?member={quote(code, safe='')}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
            except Exception as e:
                bug_log.append({"i": i, "name": name, "code": code, "fatal": str(e)[:200]})
                print(f"  [{i:>3}/176] [LOAD-FAIL] {name}  ({e.__class__.__name__})")
                continue
            modal_present = dismiss_modal(page)
            # Wait for the hero anchor row OR a not-found banner to appear
            # before extracting body text — Streamlit's networkidle fires
            # before the page body finishes rendering.
            hero_ready = False
            try:
                page.wait_for_function(
                    f"""() => {{
                        const t = document.body.innerText || '';
                        if (/We couldn'?t find this TD|Page not found/i.test(t)) return true;
                        return /(Fianna|Fine Gael|Sinn|Labour|Green Party|Social Democrats|Independent|People Before Profit|Aont|Solidarity)/i.test(t);
                    }}""",
                    timeout=HERO_TIMEOUT_MS,
                )
                hero_ready = True
            except Exception:
                pass
            time.sleep(EXTRA_SETTLE_S)
            if not modal_present:
                modal_present = dismiss_modal(page)

            try:
                body = page.locator('[data-testid="stApp"]').inner_text()
            except Exception as e:
                bug_log.append({"i": i, "name": name, "code": code, "fatal": f"body-extract: {e}"})
                print(f"  [{i:>3}/176] [BODY-FAIL] {name}")
                continue

            findings = detect_bugs(body)
            if modal_present:
                findings.insert(0, {"bug": "spurious_modal", "label": "Page-not-found modal opened on valid /member-overview URL", "context": ""})
            if not has_hero(body):
                findings.append({"bug": "no_hero", "label": "Hero party·constituency line did not render", "context": body[:200].replace("\n", " ⏎ ")})

            if findings:
                bug_log.append({"i": i, "name": name, "code": code, "findings": findings, "body_chars": len(body)})
                with LOG_FILE.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"name": name, "code": code, "findings": findings}, ensure_ascii=False) + "\n")
                # screenshot only when buggy
                slug = code.replace(".", "_").replace("'", "")[:60]
                shot_path = OUT / f"{i:03d}_{slug}.png"
                try:
                    page.screenshot(path=str(shot_path), full_page=True)
                except Exception:
                    pass
                labels = ", ".join(sorted({f["label"] for f in findings}))
                print(f"  [{i:>3}/176] [BUG] {name}  -> {labels}")
            else:
                print(f"  [{i:>3}/176] ok  {name}")

        ctx.close()
        browser.close()

    # Summary
    print("\n" + "=" * 60)
    print(f"CRAWL COMPLETE: {len(codes)} members, {len(bug_log)} with at least one bug")
    print("=" * 60)
    # Aggregate by bug type
    by_bug: dict[str, int] = {}
    for entry in bug_log:
        for f in entry.get("findings", []):
            by_bug[f["bug"]] = by_bug.get(f["bug"], 0) + 1
        if entry.get("fatal"):
            by_bug["fatal_load"] = by_bug.get("fatal_load", 0) + 1
    for k, v in sorted(by_bug.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<25} {v}")
    print(f"\nFull log: {LOG_FILE}")


if __name__ == "__main__":
    main()
