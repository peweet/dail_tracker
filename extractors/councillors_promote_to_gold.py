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


def _mdate(fn: str) -> str:
    from urllib.parse import unquote

    fn = unquote(str(fn)).rsplit("/", 1)[-1]
    m = re.search(r"(\d{1,2})[.\-\s](\d{1,2})[.\-\s](20\d{2})", fn)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(20\d{2})",
        fn,
        re.I,
    )
    return f"{m.group(1)} {m.group(2)} {m.group(3)}" if m else str(fn)[:24]


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
                "meeting_date": _mdate(v.get("meeting", "")),
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
                "meeting_date": r.get("date", ""),
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
