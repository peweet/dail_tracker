"""Extract HSE Capital Plan project appendices for multiple years and diff on Capital Reference No.

EXPERIMENTAL sandbox — answers the "highest-value open question" from QUALITY.md: is the
Capital Reference No. stable across plan years, so a status change (e.g. Design Feasibility ->
Tender) becomes a leading indicator? Reuses the appendix-table parse validated against the 2026
plan in hse_appendix_parse.py, generalised to auto-locate appendix page ranges per document
(page numbers differ year to year) instead of hardcoding them.

Writes: hse_projects_<year>.jsonl (clean rows) + hse_year_over_year_diff.json. No data/, no gold.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import fitz
import requests

HERE = Path(__file__).resolve().parent
CORPUS = HERE / "corpus"
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}

PLANS = {
    2026: "https://www.drugsandalcohol.ie/45231/1/HSE_Capital_Plan_2026.pdf",
    2025: "https://www.drugsandalcohol.ie/43044/1/HSE_Capital_plan_2025.pdf",
}

REF_RE = re.compile(r"^\d{4,6}[A-Z]{0,2}$")
STATUS_VOCAB = {"Construction", "Complete", "Tender", "Detailed Design",
                "Design Feasibility", "Appraisal", "Appraisal - AG 0",
                "Appraisal - AG0", "Preliminary Business Case", "Final Business Case"}
APPENDIX_HEAD_RE = re.compile(r"^Appendix\s+(\d[A-Z]?)\s*(?:[-–]\s*(.+))?$", re.I)


def fetch_pdf(year: int, url: str) -> Path:
    path = CORPUS / f"HSE_Capital_Plan_{year}.pdf"
    if not path.exists():
        CORPUS.mkdir(parents=True, exist_ok=True)
        r = requests.get(url, headers=HDRS, timeout=60)
        r.raise_for_status()
        path.write_bytes(r.content)
    return path


def find_appendix_ranges(doc) -> dict[str, tuple[int, int]]:
    """Locate contiguous page ranges for each 'Appendix N - Title' heading."""
    page_headings: dict[int, str] = {}
    for i, page in enumerate(doc):
        for ln in page.get_text().split("\n"):
            m = APPENDIX_HEAD_RE.match(ln.strip())
            if m:
                title = (m.group(2) or "").strip()
                page_headings[i] = f"Appendix {m.group(1)}" + (f" - {title}" if title else "")
                break
    ranges: dict[str, tuple[int, int]] = {}
    if not page_headings:
        return ranges
    pages_sorted = sorted(page_headings)
    for idx, pno in enumerate(pages_sorted):
        name = page_headings[pno]
        # stop at the next DIFFERENT heading, or +1 page if it's the last
        end = pages_sorted[idx + 1] if idx + 1 < len(pages_sorted) else pno + 1
        lo, hi = ranges.get(name, (pno, pno + 1))
        ranges[name] = (min(lo, pno), max(hi, end))
    return ranges


def rows_for_range(doc, lo: int, hi: int) -> list[dict]:
    lines: list[str] = []
    for pno in range(lo, hi):
        for ln in doc[pno].get_text().split("\n"):
            ln = ln.strip()
            if ln:
                lines.append(ln)
    rows, i, n = [], 0, len(lines)
    while i < n:
        if REF_RE.match(lines[i]):
            ref = lines[i]
            j = i + 1
            desc_parts, status = [], None
            while j < n and not REF_RE.match(lines[j]):
                if lines[j] in STATUS_VOCAB:
                    status = lines[j]
                    j += 1
                    break
                desc_parts.append(lines[j])
                j += 1
            rows.append({"ref_no": ref, "text_block": " | ".join(desc_parts), "status": status})
            i = j
        else:
            i += 1
    return rows


def extract_year(year: int, url: str) -> list[dict]:
    path = fetch_pdf(year, url)
    doc = fitz.open(path)
    ranges = find_appendix_ranges(doc)
    all_rows = []
    for name, (lo, hi) in ranges.items():
        for r in rows_for_range(doc, lo, hi):
            r["appendix"] = name
            r["plan_year"] = year
            all_rows.append(r)
    doc.close()
    (HERE / f"hse_projects_{year}.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in all_rows), encoding="utf-8")
    print(f"{year}: appendix ranges={ {k: v for k, v in ranges.items()} }")
    print(f"{year}: rows={len(all_rows)} with_status={sum(1 for r in all_rows if r['status'])}")
    return all_rows


def main() -> int:
    by_year = {y: extract_year(y, u) for y, u in sorted(PLANS.items())}

    y2025 = {r["ref_no"]: r for r in by_year[2025]}
    y2026 = {r["ref_no"]: r for r in by_year[2026]}
    shared = set(y2025) & set(y2026)
    only_2025 = set(y2025) - set(y2026)
    only_2026 = set(y2026) - set(y2025)

    transitions = []
    same_status = 0
    for ref in shared:
        s25, s26 = y2025[ref]["status"], y2026[ref]["status"]
        if s25 != s26:
            transitions.append({"ref_no": ref, "2025_status": s25, "2026_status": s26,
                                 "facility_desc": y2026[ref]["text_block"][:120]})
        else:
            same_status += 1

    diff = {
        "n_2025_rows": len(y2025), "n_2026_rows": len(y2026),
        "n_shared_ref_no": len(shared),
        "shared_rate_of_2026": round(len(shared) / max(1, len(y2026)), 3),
        "n_only_2025": len(only_2025), "n_only_2026": len(only_2026),
        "n_same_status": same_status, "n_status_transitions": len(transitions),
        "sample_transitions": transitions[:20],
    }
    (HERE / "hse_year_over_year_diff.json").write_text(
        json.dumps(diff, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSHARED ref_no: {len(shared)}/{len(y2026)} ({diff['shared_rate_of_2026']:.1%} of 2026)")
    print(f"only-2025: {len(only_2025)}  only-2026: {len(only_2026)}")
    print(f"same status: {same_status}  status TRANSITIONS: {len(transitions)}")
    for t in transitions[:10]:
        print(" ", t["ref_no"], t["2025_status"], "->", t["2026_status"], "|", t["facility_desc"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
