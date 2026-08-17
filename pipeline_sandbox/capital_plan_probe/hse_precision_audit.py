"""Independent precision audit of the 20 '-> Tender' transitions found by hse_multi_year.py.

Does NOT reuse the sequential block-parser's row logic. For each ref_no, finds every raw
line-index where that exact token appears in each year's PDF and dumps a wide raw-text
window (+-8 lines) around EACH occurrence. This catches the failure mode the parser can't
see itself: a ref_no appearing more than once on a page (e.g. as a real row AND inside a
cross-reference like "enabling work for Cap Ref #11643"), which would make the sequential
parser's row association wrong even though it "found" a status nearby.

Writes hse_precision_audit_raw.json for manual read-back; prints nothing bulky to stdout.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import fitz

HERE = Path(__file__).resolve().parent
REF_RE = re.compile(r"^\d{4,6}[A-Z]{0,2}$")


def year_lines(year: int) -> list[str]:
    doc = fitz.open(HERE / "corpus" / f"HSE_Capital_Plan_{year}.pdf")
    lines = []
    for p in doc:
        for ln in p.get_text().split("\n"):
            ln = ln.strip()
            if ln:
                lines.append(ln)
    doc.close()
    return lines


def occurrences(lines: list[str], ref_no: str, window: int = 8) -> list[dict]:
    hits = []
    for i, ln in enumerate(lines):
        if ln == ref_no:
            lo, hi = max(0, i - 1), min(len(lines), i + window)
            hits.append({"line_index": i, "context": lines[lo:hi]})
    return hits


def main() -> int:
    diff = json.loads((HERE / "hse_year_over_year_diff.json").read_text(encoding="utf-8"))
    # sample_transitions in the diff file was capped at 20 and already filtered to real
    # (non-None-involved) transitions in the earlier session -- but recompute the ->Tender
    # subset directly from the per-year jsonl to be sure we audit the right set.
    rows25 = {json.loads(l)["ref_no"]: json.loads(l)
              for l in (HERE / "hse_projects_2025.jsonl").read_text(encoding="utf-8").splitlines()}
    rows26 = {json.loads(l)["ref_no"]: json.loads(l)
              for l in (HERE / "hse_projects_2026.jsonl").read_text(encoding="utf-8").splitlines()}
    to_tender = [ref for ref in (set(rows25) & set(rows26))
                 if rows26[ref]["status"] == "Tender" and rows25[ref]["status"]
                 and rows25[ref]["status"] != "Tender"]

    lines25 = year_lines(2025)
    lines26 = year_lines(2026)

    audit = []
    for ref in sorted(to_tender):
        audit.append({
            "ref_no": ref,
            "parsed_2025_status": rows25[ref]["status"],
            "parsed_2026_status": rows26[ref]["status"],
            "parsed_2026_text": rows26[ref]["text_block"],
            "raw_2025_occurrences": occurrences(lines25, ref),
            "raw_2026_occurrences": occurrences(lines26, ref),
        })

    (HERE / "hse_precision_audit_raw.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"to_tender count={len(to_tender)}")
    for a in audit:
        print(f"{a['ref_no']}: 2025 occurrences={len(a['raw_2025_occurrences'])} "
              f"2026 occurrences={len(a['raw_2026_occurrences'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
