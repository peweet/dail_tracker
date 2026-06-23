"""Per-member councillor vote attribution from born-digital roll-call minutes.

Roll-call councils (e.g. Carlow) print a table:  Member | For | Against | Abstain | Absent
with a check mark (√ = U+221A, sometimes ✓/X) in the voted column. fitz `find_tables()` recovers
the cells incl. the mark, so we can attribute each NAMED councillor's vote — a true per-councillor
voting record, no OCR needed (these PDFs are born-digital).

Output: member_votes.jsonl (one row per councillor-per-vote) + member_votes.csv.
Each row: local_authority, meeting (file), motion (nearest preceding context), member, vote.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}

MARKS = {"√", "✓", "✔", "x", "X", "✗", "•", "Y", "y", "1"}
VOTE_COLS = ("for", "against", "abstain", "absent")


def is_mark(cell: str) -> bool:
    c = (cell or "").strip()
    return c in MARKS or (len(c) == 1 and not c.isalnum()) or "√" in c


def header_map(row: list[str]) -> dict[int, str] | None:
    """If this row is a vote-table header, return {col_index: 'for'|'against'|...}."""
    low = [(c or "").strip().lower() for c in row]
    m = {}
    for i, c in enumerate(low):
        for v in VOTE_COLS:
            if c == v or c.startswith(v):
                m[i] = v
    # need at least For + Against, and a name-ish first column
    if {"for", "against"} <= set(m.values()):
        return m
    return None


def parse_pdf(la: str, fname: str, pdf: bytes) -> list[dict]:
    import fitz  # noqa: PLC0415
    doc = fitz.open(stream=pdf, filetype="pdf")
    out = []
    last_motion = ""  # carry across page-breaks (motion often on the page before the table)
    for page in doc:
        try:
            tables = page.find_tables()
        except Exception:  # noqa: BLE001
            tables = None
        page_motion = _nearest_motion(page.get_text())
        if page_motion:
            last_motion = page_motion
        for tbl in (tables.tables if tables else []):
            rows = tbl.extract()
            if not rows:
                continue
            hmap = header_map(rows[0])
            if not hmap:
                continue
            y0 = tbl.bbox[1]
            above = "\n".join(b[4] for b in page.get_text("blocks") if b[1] < y0)
            motion = _nearest_motion(above) or last_motion
            name_col = min(set(range(len(rows[0]))) - set(hmap)) if set(hmap) else 0
            for r in rows[1:]:
                name = (r[name_col] or "").replace("\n", " ").strip()
                if not name or name.lower().startswith(("member", "total", "result")):
                    continue
                vote = next((hmap[i] for i in hmap if i < len(r) and is_mark(r[i])), None)
                if vote:
                    out.append({"local_authority": la, "meeting": fname,
                                "motion": motion[:240], "member": name, "vote": vote})
    return out


def normalise_members(rows: list[dict]) -> list[dict]:
    """Fold split/garbled names onto the council's real roster (high-frequency names)."""
    import difflib
    from collections import Counter
    by_la: dict[str, Counter] = {}
    for r in rows:
        by_la.setdefault(r["local_authority"], Counter())[r["member"]] += 1
    rosters = {la: [n for n, c in cnt.items() if c >= 3 and len(n) > 6 and " " in n]
               for la, cnt in by_la.items()}
    cleaned = []
    for r in rows:
        roster = rosters.get(r["local_authority"], [])
        if r["member"] in roster:
            cleaned.append(r)
            continue
        match = difflib.get_close_matches(r["member"], roster, n=1, cutoff=0.6)
        if match:
            r = {**r, "member": match[0]}
            cleaned.append(r)
        # else: drop unmatchable fragment
    return cleaned


_MOT = re.compile(r"(Resolution|Motion|Proposed by|That the|We the Members)[^\n]{0,220}", re.I)


def _nearest_motion(text_above: str) -> str:
    hits = _MOT.findall(text_above) if text_above else []
    # the closest preceding motion-ish line is the last match in reading order
    if hits:
        m = list(_MOT.finditer(text_above))[-1]
        return re.sub(r"\s+", " ", text_above[m.start():m.start() + 220]).strip()
    return ""


# Born-digital roll-call councils + a recent multi-meeting set to mine (extend as discovery grows).
SOURCES = {
    "Carlow": [
        "https://carlow.ie/sites/default/files/2026-05/Minutes%20Carlow%20County%20Council%20March%202026.pdf",
        "https://carlow.ie/sites/default/files/2026-05/Minutes%20Carlow%20County%20Council%20April%202026.pdf",
        "https://carlow.ie/sites/default/files/2026-06/Minutes%20Carlow%20County%20Council%20May%202026.pdf",
        "https://carlow.ie/sites/default/files/2026-05/Minutes%20Carlow%20County%20Council%20February%202026_0.pdf",
    ],
}


def main() -> int:
    extra = sys.argv[1:]  # optional extra PDF urls (la inferred as 'adhoc')
    rows = []
    work = list(SOURCES.items())
    if extra:
        work.append(("adhoc", extra))
    for la, urls in work:
        for u in urls:
            try:
                pdf = requests.get(u, headers=HDRS, timeout=70).content
                recs = parse_pdf(la, u.split("/")[-1], pdf)
                rows += recs
                print(f"{la:10} {u.split('/')[-1][:48]:48} -> {len(recs)} member-votes")
            except Exception as e:  # noqa: BLE001
                print(f"{la:10} ERR {type(e).__name__} {u.split('/')[-1][:48]}")
    rows = normalise_members(rows)
    (HERE / "member_votes.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    with open(HERE / "member_votes.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["local_authority", "meeting", "motion", "member", "vote"])
        w.writeheader()
        w.writerows(rows)
    print(f"\nTOTAL member-vote rows: {len(rows)}  -> member_votes.jsonl / .csv")
    # quick tally
    from collections import Counter
    print("by vote:", dict(Counter(r["vote"] for r in rows)))
    print("distinct members:", len({r["member"] for r in rows}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
