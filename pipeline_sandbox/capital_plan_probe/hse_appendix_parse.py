"""Parse HSE Capital Plan 2026 Appendices 1/2/2A/3/4 into (ref_no, facility, description, status) rows.

EXPERIMENTAL sandbox probe — second pass after hse_capital_plan_probe.py showed the narrative
bullets are sparse and not project-scoped. Pages 54-79 hold a real per-project register instead:
a repeating 4-field block (Capital Reference No. / Facility / Brief Project Description / Status)
with a small closed status vocabulary. This measures how cleanly that block-structure parses
before anything is proposed for PublicSignal. Writes nothing to data/, no gold, no promotion.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import fitz

HERE = Path(__file__).resolve().parent
PDF_PATH = HERE / "corpus" / "HSE_Capital_Plan_2026.pdf"

APPENDICES = {
    "Appendix 1 - Construction Phase": (53, 62),   # 0-indexed page range, end exclusive
    "Appendix 2 - Design Phase": (62, 71),
    "Appendix 2A - Appraisal": (70, 76),
    "Appendix 3 - Disability Design/Construction": (75, 79),
    "Appendix 4 - Climate Action": (78, 80),
}

REF_RE = re.compile(r"^\d{4,6}[A-Z]{0,2}$")
STATUS_VOCAB = {"Construction", "Complete", "Tender", "Detailed Design",
                "Design Feasibility", "Appraisal", "Appraisal - AG 0"}
NOISE_RE = re.compile(
    r"^(Capital\s*$|Reference\s*$|No\.\s*$|Facility\s*$|Brief Project Description\s*$|"
    r"Status\s*$|Appendix \d|Projects Post Approval|Funding source|HSE |Acute Projects|"
    r"Community |Primary Care|Mental Health|Radiation Oncology|Enhanced Community Care|"
    r"\*All projects|Capital Plan 2026\s*$|\d+ \| P a g e|Disability Services|"
    r"Climate Action|Older Persons|Regional Hospital Group)", re.I)


def rows_for_range(doc, lo: int, hi: int) -> list[dict]:
    lines: list[str] = []
    for pno in range(lo, hi):
        for ln in doc[pno].get_text().split("\n"):
            ln = ln.strip()
            if ln:
                lines.append(ln)

    rows = []
    i = 0
    n = len(lines)
    while i < n:
        if REF_RE.match(lines[i]):
            ref = lines[i]
            j = i + 1
            desc_parts = []
            status = None
            # consume until we hit a known status token or the next ref number
            while j < n and not REF_RE.match(lines[j]):
                if lines[j] in STATUS_VOCAB:
                    status = lines[j]
                    j += 1
                    break
                desc_parts.append(lines[j])
                j += 1
            rows.append({
                "ref_no": ref,
                "text_block": " | ".join(desc_parts),
                "status": status,
            })
            i = j
        else:
            i += 1
    return rows


def main() -> int:
    doc = fitz.open(PDF_PATH)
    all_rows = {}
    for name, (lo, hi) in APPENDICES.items():
        rows = rows_for_range(doc, lo, hi)
        all_rows[name] = rows
    doc.close()

    summary = {}
    for name, rows in all_rows.items():
        with_status = [r for r in rows if r["status"]]
        status_counts = {}
        for r in with_status:
            status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
        summary[name] = {
            "n_rows": len(rows),
            "n_with_status": len(with_status),
            "status_counts": status_counts,
        }

    (HERE / "hse_appendix_rows.json").write_text(
        json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    (HERE / "hse_appendix_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    total = sum(s["n_rows"] for s in summary.values())
    total_status = sum(s["n_with_status"] for s in summary.values())
    print(f"TOTAL rows={total} with_status={total_status} "
          f"({total_status/max(1,total):.1%})")
    for name, s in summary.items():
        print(f"{name}: rows={s['n_rows']} with_status={s['n_with_status']} "
              f"{s['status_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
