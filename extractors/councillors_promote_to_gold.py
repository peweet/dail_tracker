"""Promote the vetted Your-Councillors sandbox data → git-tracked gold CSVs in data/_meta/.

The sandbox build lives in pipeline_sandbox/council_minutes/. This copies the FINAL, vetted
datasets into data/_meta/ (kept by the `!data/_meta/*.csv` .gitignore negation, exactly like
la_chief_executives.csv) where registered v_la_councillors* views read them. Nested lists
(agenda items, order-of-business) are flattened to a ` | ` delimiter the page re-splits.

CAVEATS are carried in the data so the UI can be honest:
  - roster ~96% (some councils undercounted on Wikipedia)
  - standing orders parsed for only ~8/31 councils (rest: source not located)
  - named votes only where the council records roll-calls (Carlow)
  - Louth minutes are book-format scans → no agendas
Run:  python extractors/councillors_promote_to_gold.py
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SBX = ROOT / "pipeline_sandbox" / "council_minutes"
META = ROOT / "data" / "_meta"
SEP = " | "


def _jsonl(p: Path) -> list[dict]:
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()] if p.exists() else []


_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _mdate(fn: str) -> str:
    """Best-effort meeting date from a minutes filename. Councils name files every way
    imaginable — ISO (2025_01_20), D-M-YYYY, compact DDMMYYYY (04122024), 'April 2026',
    'apr2026'. A filename that yields NO date returns '' (the page renders blank honestly)
    rather than a filename fragment, which is what put 'signed-minutes-council-m' into the
    gold CSV as a date."""
    from urllib.parse import unquote

    fn = unquote(str(fn)).rsplit("/", 1)[-1]
    m = re.search(r"(20\d{2})[.\-\s_](\d{1,2})[.\-\s_](\d{1,2})", fn)  # ISO-ish YYYY_MM_DD
    if m and 1 <= int(m.group(3)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(3)):02d}/{int(m.group(2)):02d}/{m.group(1)}"
    m = re.search(r"(\d{1,2})[.\-\s_](\d{1,2})[.\-\s_](20\d{2})", fn)  # D_M_YYYY
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"
    m = re.search(r"(?<!\d)(\d{2})(\d{2})(20\d{2})(?!\d)", fn)  # compact DDMMYYYY
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    m = re.search(r"(?<!\d)(\d{1,2})[.\-\s_](\d{1,2})[.\-\s_](2\d)(?!\d)", fn)  # DD_MM_YY (Cork City)
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/20{m.group(3)}"
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+(" + "|".join(_MONTHS) + r")\s+(20\d{2})",
        fn,
        re.I,
    )
    if m:
        return f"{m.group(1)} {m.group(2).title()} {m.group(3)}"
    m = re.search(r"(" + "|".join(mo[:3] for mo in _MONTHS) + r")[a-z]*[.\-\s_]?(20\d{2})", fn, re.I)  # April 2026 / apr2026
    if m:
        full = next(mo for mo in _MONTHS if mo.lower().startswith(m.group(1).lower()))
        return f"{full} {m.group(2)}"
    return ""


def _iso_to_display(iso: str) -> str:
    """'2026-02-09' → '09/02/2026'; anything non-ISO passes through _mdate or blanks."""
    m = re.fullmatch(r"(20\d{2})-(\d{2})-(\d{2})", str(iso or "").strip())
    return f"{m.group(3)}/{m.group(2)}/{m.group(1)}" if m else _mdate(iso)


def _agenda_date(raw: str, source_url: str) -> str:
    """A meeting_history date is kept only if it actually contains a date; otherwise fall
    back to parsing the source filename, else blank. Kills the 8 filename-fragment rows
    (Louth 'signed-minutes-council-m', Waterford '1_draft_plenary_minute')."""
    raw = str(raw or "").strip()
    if raw and re.search(r"20\d{2}", raw) and len(raw) <= 24:
        return raw
    return _mdate(raw) or _mdate(source_url)


# The local_authority value is the cross-source JOIN KEY and must match the CE roster / payments / AFS
# spelling EXACTLY (plain ASCII). The sandbox carries the Irish-accented DLR name; canonicalise it on the
# key column here so a re-promote never reintroduces the mismatch that orphans DLR from every other
# dataset. Display columns (source / agenda) keep their accents untouched.
_CANON_LA = {"Dún Laoghaire-Rathdown": "Dun Laoghaire-Rathdown"}


def _write(name: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(META / name, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            if "local_authority" in r:
                r = {**r, "local_authority": _CANON_LA.get(str(r["local_authority"]), r["local_authority"])}
            w.writerow(r)
    print(f"  data/_meta/{name}: {len(rows)} rows")


def main() -> int:
    META.mkdir(parents=True, exist_ok=True)
    print("Promoting Your-Councillors sandbox -> data/_meta gold CSVs")

    # 1. roster
    with open(SBX / "councillors_roster.csv", encoding="utf-8") as fh:
        roster = list(csv.DictReader(fh))
    _write(
        "la_councillors.csv",
        ["local_authority", "lea", "name", "party", "status", "source"],
        [{k: r.get(k, "") for k in ["local_authority", "lea", "name", "party", "status", "source"]} for r in roster],
    )

    # 2. coverage tiers
    with open(SBX / "council_coverage.csv", encoding="utf-8") as fh:
        cov = list(csv.DictReader(fh))
    _write(
        "la_council_meeting_coverage.csv",
        ["local_authority", "tier", "clean_minutes", "roster_councillors", "has_votes"],
        [
            {k: r.get(k, "") for k in ["local_authority", "tier", "clean_minutes", "roster_councillors", "has_votes"]}
            for r in cov
        ],
    )

    # 3. named votes (per councillor)
    votes = _jsonl(SBX / "member_votes.jsonl")
    _write(
        "la_councillor_votes.csv",
        ["local_authority", "member", "meeting_date", "motion", "vote"],
        [
            {
                "local_authority": v["local_authority"],
                "member": v["member"],
                # filename-derived date first; else the extractor's own meeting_date (ISO,
                # read from the minutes' first page — e.g. ModernGov MId-named files)
                "meeting_date": _mdate(v.get("meeting", "")) or _iso_to_display(v.get("meeting_date", "")),
                "motion": (v.get("motion") or "")[:300],
                "vote": v["vote"],
            }
            for v in votes
        ],
    )

    # 4. meeting agendas (flatten agenda_items)
    mh = _jsonl(SBX / "meeting_history.jsonl")
    _write(
        "la_meeting_agendas.csv",
        ["local_authority", "meeting_date", "agenda", "source_url"],
        [
            {
                "local_authority": r["council"],
                "meeting_date": _agenda_date(r.get("date", ""), r.get("source_url", "")),
                "agenda": SEP.join(r.get("agenda_items", [])),
                "source_url": r.get("source_url", ""),
            }
            for r in mh
            if r.get("agenda_items")
        ],
    )

    # 5. standing orders (flatten order_of_business)
    so = _jsonl(SBX / "standing_orders.jsonl")
    _write(
        "la_standing_orders.csv",
        [
            "local_authority",
            "order_of_business",
            "notice_of_motion",
            "voting",
            "quorum",
            "records_named_votes",
            "source_url",
        ],
        [
            {
                "local_authority": r["local_authority"],
                "order_of_business": SEP.join(r.get("order_of_business", [])),
                "notice_of_motion": r.get("notice_of_motion", ""),
                "voting": r.get("voting", ""),
                "quorum": r.get("quorum", ""),
                "records_named_votes": r.get("records_named_votes", False),
                "source_url": r.get("source_url", ""),
            }
            for r in so
            if r.get("status") == "ok"
        ],
    )

    print("Done. These 5 CSVs are the gold source for the v_la_councillors* views.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
