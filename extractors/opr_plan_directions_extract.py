"""INGEST: OPR / Ministerial Directions — the record of ELECTED COUNCILLORS' development-plan
and zoning votes being overruled (or upheld) by the Planning Regulator and the Minister.

WHY THIS EXISTS. Making the development plan (and zoning land in it) is a RESERVED function —
one of the few real powers elected councillors hold. Since 2019 the Office of the Planning
Regulator evaluates each draft plan; where the members adopt a plan that is NOT consistent with
the OPR's recommendations, the OPR must notify the Minister (PDA 2000 s.31AM(8)), who may issue
a Direction that is "deemed to be incorporated into the plan" — the members' offending
provisions being "deemed not to be included" (s.31AN(11)). There is no appeal, only judicial
review. This fact makes that machinery countable per council for the first time: there is NO
official running total of s.31 Directions anywhere (the last authoritative count is a PQ of
Oct 2022). Research: doc/LOCAL_DEMOCRACY_OVERRIDE_RESEARCH.md.

⚠️ NEVER BLEND WITH THE APPEALS FACT. Three structurally different override relationships exist
and must never be merged into one "overruled" number:
  1. An Coimisiún Pleanála overturning the council's PLANNERS on appeal  → planning_appeal_outcomes
     (that is the CHIEF EXECUTIVE's executive decision — NOT a councillor decision)
  2. OPR/Minister overruling the elected MEMBERS' plan/zoning vote        → THIS FACT (reserved function)
  3. SHD/SID bypassing the council entirely at first instance
Same discipline as the three money grains.

⚠️ NOT AN "OVERRIDES" COUNTER. The outcome is NOT always an override — the register also records
the Minister DECLINING to follow the OPR (Sligo CDP 2024-2030, six parts, May 2026; Kilkenny
Variation 5, Apr 2026), plans still in progress, and s.63(6) suspension notices under the new
2024 Act. Counting only the overrides would misrepresent the process. `plan_outcome` carries the
full taxonomy, derived MECHANICALLY from the latest decisive document (no editorial judgement).

⚠️ RESTRICTIVE ONLY. The Minister/OPR can strike a zoning; they cannot create one. Say so in copy.

Source: the OPR's own register (opr.ie) — the de-facto national register, because s.31 Directions
are NOT centrally published on gov.ie. No WAF (unlike gov.ie); the page IS compressed, so the
fetch must decompress (curl --compressed) — a plain fetch returns binary.

Run:  ./.venv/Scripts/python.exe extractors/opr_plan_directions_extract.py
"""

from __future__ import annotations

import contextlib
import csv
import html as html_mod
import json
import re
import subprocess
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

OUT_CSV = ROOT / "data/_meta/opr_plan_directions.csv"
OUT_COV = ROOT / "data/_meta/opr_plan_directions_coverage.json"
REGISTER = "https://www.opr.ie/recommendations-made-by-the-opr-to-the-minister/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

CANON_31 = [
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal", "Dublin City",
    "Dun Laoghaire-Rathdown", "Fingal", "Galway City", "Galway County", "Kerry", "Kildare",
    "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo", "Meath",
    "Monaghan", "Offaly", "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
]
# longest first so "Cork County"/"Galway City" can never be shadowed by a bare "Cork"/"Galway"
_CANON_SORTED = sorted(CANON_31, key=len, reverse=True)

# Local Area Plans are named for a TOWN, not the council — the only cases the prefix match misses.
TOWN_TO_COUNCIL = {
    "athenry": "Galway County",
    "ballina": "Mayo",
    "castlebar": "Mayo",
    "kenmare": "Kerry",
    "letterkenny": "Donegal",
    "loughrea": "Galway County",
    "westport": "Mayo",
}

# The two DECISIVE stages — the latest of these determines the plan's outcome.
DECISIVE = {"minister_final_direction": "direction_issued", "minister_declined": "minister_declined"}


def fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


# The OPR site is a BROTLI trap with three layers — all three were hit building this:
#   1. a plain fetch returns binary (the body is compressed);
#   2. the repo's usual fix (ask for "gzip, deflate" only — see ministerial_diaries_extract) does
#      NOT work here: opr.ie IGNORES Accept-Encoding and serves `br` regardless, and `requests`
#      cannot decode brotli without the (uninstalled) brotli package;
#   3. Windows' bundled curl.exe has no brotli either — `--compressed` dies with
#      "curl: (61) Unrecognized content encoding type".
# Git's bundled curl DOES have brotli, so try that explicitly. Ordered fallback below; installing
# the `brotli` package would collapse this to a one-liner, but that's a dependency decision.
_CURLS = [r"C:\Program Files\Git\mingw64\bin\curl.exe", "curl"]


def _looks_html(s: str) -> bool:
    return "<a " in s.lower() or "<html" in s.lower()


def fetch(url: str) -> str:
    hdrs = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    with contextlib.suppress(Exception):  # works wherever the server honours Accept-Encoding
        r = requests.get(url, headers=hdrs, timeout=90)
        if r.ok and _looks_html(r.text):
            return r.text
    for exe in _CURLS:  # brotli-capable curl (Git's), then whatever is on PATH
        with contextlib.suppress(Exception):
            p = subprocess.run(
                [exe, "-sS", "-k", "-L", "--compressed", "--max-time", "90", "-A", UA, url],
                capture_output=True,
                timeout=120,
                check=False,
            )
            body = p.stdout.decode("utf-8", errors="replace")
            if _looks_html(body):
                return body
    return ""


def canon_council(plan_name: str) -> str | None:
    """Council from the plan title: prefix-match the canonical 31 (longest first), else the
    town→council map for Local Area Plans named after a town."""
    n = fold(plan_name).strip()
    low = n.lower().replace("-", " ")
    for canon in _CANON_SORTED:
        cl = fold(canon).lower().replace("-", " ")
        if low.startswith(cl):
            return canon
    first = low.split()[0] if low.split() else ""
    return TOWN_TO_COUNCIL.get(first)


def classify(text: str, url: str) -> str:
    """Stage of one published document.

    ⚠️ Read the link text + the FILENAME only — never the whole URL: every document lives on
    `www.opr.ie`, so a naive "does it mention OPR" test on the full URL matches the DOMAIN and
    classifies every Minister's Direction as an OPR document (this bug silently produced zero
    Directions on the first run).

    Order matters: "Minister's Decision not to issue draft Direction" also contains the words
    "draft Direction", so the decline test must run first; a "Statement on Draft Direction" is a
    statement, not a direction."""
    blob = f"{text} {url.rsplit('/', 1)[-1]}".lower()
    has_opr = "opr" in blob
    if any(k in blob for k in ("decision not to", "not to issue", "not to agree")):
        return "minister_declined"
    if "section 63" in blob or "section-63" in blob or "suspension" in blob:
        return "suspension_notice"  # the NEW 2024-Act machinery (pre-Direction suspension)
    if "statement of reasons" in blob or "statement on" in blob or "statement-of-reasons" in blob:
        return "statement_of_reasons"
    if "draft direction" in blob or "draft-direction" in blob:
        return "opr_proposed_draft" if has_opr else "minister_draft_direction"
    if "direction" in blob:
        return "opr_proposed_final" if has_opr else "minister_final_direction"
    return "other"


def plan_type(plan_name: str) -> str:
    low = plan_name.lower()
    if "variation" in low:
        return "variation"
    if "local area plan" in low or " lap" in low:
        return "local_area_plan"
    return "development_plan"


def doc_date(url: str) -> str:
    """Upload path carries the month: /wp-content/uploads/YYYY/MM/… → 'YYYY-MM'."""
    m = re.search(r"/uploads/(\d{4})/(\d{2})/", url)
    return f"{m.group(1)}-{m.group(2)}" if m else ""


_TITLE_RE = re.compile(r'<a[^>]*class="[^"]*elementor-accordion-title[^"]*"[^>]*>(.*?)</a>', re.I | re.S)
# hrefs are inconsistent on this page: mostly https://www.opr.ie/..., but at least one document
# (the Castlecomer s.63(6) notice) is plain http://, and site-furniture PDFs are relative. Accept
# all three shapes and normalise; the stage classifier then drops anything that isn't a
# Direction-chain document (the last accordion's block otherwise runs into the page footer).
_PDF_RE = re.compile(
    r'<a[^>]*href="((?:https?://www\.opr\.ie)?/wp-content/uploads/[^"]+?\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S,
)


def norm_url(href: str) -> str:
    if href.startswith("/"):
        return f"https://www.opr.ie{href}"
    return href.replace("http://", "https://", 1)


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", "", s))).strip()


def parse(page: str) -> tuple[list[dict], list[str]]:
    titles = list(_TITLE_RE.finditer(page))
    rows: list[dict] = []
    unmapped: list[str] = []
    for i, tm in enumerate(titles):
        plan = clean(tm.group(1))
        if not plan:
            continue
        end = titles[i + 1].start() if i + 1 < len(titles) else len(page)
        block = page[tm.end() : end]
        council = canon_council(plan)
        if not council:
            unmapped.append(plan)
            continue
        docs = []
        for pm in _PDF_RE.finditer(block):
            url, text = norm_url(pm.group(1)), clean(pm.group(2))
            stage = classify(text, url)
            if stage == "other":
                continue  # site furniture (the final block runs into the footer's own PDFs)
            docs.append(
                {
                    "local_authority": council,
                    "plan_name": plan,
                    "plan_type": plan_type(plan),
                    "stage": stage,
                    "doc_title": text or plan,
                    "doc_date": doc_date(url),
                    "doc_url": url,
                }
            )
        if not docs:
            continue
        # OUTCOME — mechanical, not editorial: the LATEST decisive document decides. A plan can
        # have a Direction on some parts and a decline on others (Sligo); latest-wins is the
        # honest read and keeps this reproducible.
        decisive = sorted(
            [d for d in docs if d["stage"] in DECISIVE], key=lambda d: (d["doc_date"], d["stage"])
        )
        if decisive:
            outcome = DECISIVE[decisive[-1]["stage"]]
        elif any(d["stage"] == "suspension_notice" for d in docs):
            outcome = "suspension_notice"
        elif any(d["stage"] in ("minister_draft_direction", "opr_proposed_final") for d in docs):
            outcome = "in_progress"
        elif any(d["stage"] == "opr_proposed_draft" for d in docs):
            outcome = "opr_recommended"
        else:
            outcome = "unknown"
        for d in docs:
            d["plan_outcome"] = outcome
        rows.extend(docs)
    return rows, unmapped


def main() -> None:
    page = fetch(REGISTER)
    if "elementor-accordion-title" not in page:
        print("register page did not parse (compression/layout change?) — refusing to write")
        return
    rows, unmapped = parse(page)
    if not rows:
        print("no rows parsed — refusing to overwrite the fact")
        return

    fields = ["local_authority", "plan_name", "plan_type", "stage", "plan_outcome", "doc_title", "doc_date", "doc_url"]
    rows.sort(key=lambda r: (r["local_authority"], r["plan_name"], r["doc_date"]))
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    plans = {(r["local_authority"], r["plan_name"]): r["plan_outcome"] for r in rows}
    by_outcome: dict[str, int] = {}
    for o in plans.values():
        by_outcome[o] = by_outcome.get(o, 0) + 1
    councils = sorted({c for c, _p in plans})
    print(f"  plans: {len(plans)} | councils: {len(councils)} | documents: {len(rows)}")
    print(f"  outcomes: {by_outcome}")
    if unmapped:
        print(f"  ⚠ unmapped plan titles (no council): {unmapped}")
    OUT_COV.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "source": REGISTER,
                "grain": "one row per (plan, published document); plan_outcome repeated per row",
                "plans": len(plans),
                "councils": councils,
                "documents": len(rows),
                "outcomes": by_outcome,
                "unmapped_plans": unmapped,
                "caveat": "Councillors' development-plan/zoning votes reviewed by the OPR. NOT an "
                "overrides counter — outcomes include the Minister DECLINING to follow the OPR, "
                "plans in progress, and s.63(6) suspensions. The Minister/OPR can strike a zoning, "
                "never create one. NEVER merge with planning_appeal_outcomes (which measures the "
                "board overturning the CHIEF EXECUTIVE's planners, not councillors).",
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"  wrote {OUT_CSV}\n        {OUT_COV}")


if __name__ == "__main__":
    main()
