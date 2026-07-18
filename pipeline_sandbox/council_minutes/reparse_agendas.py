"""Re-parse born-digital agenda/minutes docs behind meeting_history.jsonl with the fixed
line-joining ``agenda_items()`` (comprehensive_ocr.py) — the old single-line capture
truncated most items mid-sentence at the first line wrap, and its bare ``\\d.`` start
matched numbered paragraphs deep inside minutes prose (the Louth fragment garbage).

Scope & honesty:
  * Only rows whose source_url serves a PDF **with a native text layer** are re-parsed —
    the CPU-only fix. Scanned docs (Wicklow minutes, Louth signed books, old Galway City)
    keep their existing items untouched; they need a GPU re-OCR to improve.
  * Provenance is preserved: each row keeps its source_url / date / file; only
    agenda_items is replaced, and only when the new parse yields >= 1 item.
  * A timestamped .bak of meeting_history.jsonl is written first (archive, don't delete).
  * Regenerates ONLY data/_meta/la_meeting_agendas.csv via the promote script's own
    helpers (the documented surgical flow — never re-run the whole promote, which
    rewrites all 5 gold CSVs from sandbox state).

Run:  .venv/Scripts/python pipeline_sandbox/council_minutes/reparse_agendas.py [--dry-run]
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import fitz  # noqa: E402
from comprehensive_ocr import agenda_items  # noqa: E402  (the fixed parser)

from extractors.councillors_promote_to_gold import SEP, _agenda_date, _write  # noqa: E402
from services.http_engine import fetch_bytes, polite_headers  # noqa: E402

SBX_DIR = ROOT / "pipeline_sandbox" / "council_minutes"
MH = SBX_DIR / "meeting_history.jsonl"
# Same born-digital threshold the OCR harvester used: below this chars/page the doc is a scan.
NATIVE_MIN_PER_PAGE = 80

# Louth's harvested rows are signed-minutes BOOK SCANS whose old parse produced motion-body
# fragments (junk). The real agenda-only PDFs are born-digital and retained locally
# (louth_parse.py DOCS map, dates from the page header / filename) — but they were fetched
# manually, so no source URL was recorded. We replace the junk rows with clean extractions
# carrying an honest BLANK source_url (never an invented link); the scans stay in the .bak.
LOUTH_DOCS = [
    ("2026-04-20", "county-council-monthly-meeting-20-04-2026-agenda-only.pdf"),
    ("2026-03-23", "county-council-monthly-meeting-23-03-2026-agenda-only.pdf"),
    ("2026-02-16", "county-council-monthly-meeting-16-02-2026-agenda-only.pdf"),
    ("2026-01-19", "county-council-monthly-meeting-19-01-2026-agenda-only.pdf"),
    ("2025-12-15", "county-council-monthly-meeting-15-12-2025-agenda-only.pdf"),
    ("2025-11-17", "county-council-monthly-meeting-17-11-2025-agenda-only.pdf"),
]

# Signal that the OLD parse truncated items mid-sentence (the [^\n]{5,95} single-line bug):
# an item ending on a dangling function word. Rows without this signal keep their items —
# several councils were parsed by bespoke scripts whose output is already clean.
_DANGLING = re.compile(
    r"\b(of|the|for|and|in|at|on|to|being|a|an|from|with|which|will|by|its|their|or)\s*$", re.I
)


def looks_truncated(items: list[str]) -> bool:
    return any(_DANGLING.search(str(it)) for it in items)


def native_text(pdf_bytes: bytes) -> str | None:
    """Full text if the PDF has a real text layer, else None (scan — needs OCR, skip)."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:  # noqa: BLE001 — a garbled download is a skip, not an abort
        return None
    text = "\n".join(p.get_text() for p in doc)
    return text if len(text.strip()) >= NATIVE_MIN_PER_PAGE * max(1, len(doc)) else None


def main() -> int:
    dry = "--dry-run" in sys.argv
    rows = [json.loads(line) for line in MH.read_text(encoding="utf-8").splitlines() if line.strip()]
    stats: Counter[str] = Counter()
    changed_examples: list[tuple[str, str, str]] = []

    for r in rows:
        if r.get("council") == "Louth":
            continue  # handled from the local agenda-only PDFs below
        url = str(r.get("source_url") or "")
        old = r.get("agenda_items") or []
        if ".pdf" not in url.lower():
            stats["skip_not_pdf"] += 1  # ModernGov HTML rows etc. — already-clean harvesters
            continue
        if old and not looks_truncated(old):
            stats["skip_looks_clean"] += 1  # bespoke-parser output — leave untouched
            continue
        pdf = fetch_bytes(url, headers=polite_headers(), timeout=60)
        if not pdf:
            stats["skip_fetch_failed"] += 1
            continue
        text = native_text(pdf)
        if text is None:
            stats["skip_scanned"] += 1  # needs GPU re-OCR; keep existing items
            continue
        new_items = agenda_items(text)
        if not new_items:
            stats["skip_no_items"] += 1
            continue
        if new_items != old:
            if old and len(changed_examples) < 8:
                changed_examples.append((str(r.get("council")), str(old[0])[:80], str(new_items[0])[:160]))
            r["agenda_items"] = new_items
            stats[f"updated:{r.get('council')}"] += 1
            stats["updated_total"] += 1
        else:
            stats["unchanged"] += 1
        time.sleep(0.4)  # polite pacing across council sites

    # Louth: junk scan-parses out, clean local agenda-only extractions in.
    n_louth_old = sum(1 for r in rows if r.get("council") == "Louth")
    louth_new = []
    for iso, fn in LOUTH_DOCS:
        path = SBX_DIR / "louth_pdfs" / fn
        if not path.exists():
            continue
        doc = fitz.open(path)
        items = agenda_items("\n".join(p.get_text() for p in doc))
        if items:
            louth_new.append(
                {"council": "Louth", "file": fn, "date": iso, "agenda_items": items, "source_url": ""}
            )
    if louth_new:
        rows = [r for r in rows if r.get("council") != "Louth"] + louth_new
        stats["louth_replaced"] = n_louth_old
        stats["louth_new"] = len(louth_new)
        stats["updated_total"] += len(louth_new)

    print("\n".join(f"  {k}: {v}" for k, v in sorted(stats.items())))
    for la, old, new in changed_examples:
        print(f"\n[{la}]\n  OLD: {old}\n  NEW: {new}")
    if dry or not stats["updated_total"]:
        print("\ndry-run / nothing to write" if dry else "\nno rows changed")
        return 0

    bak = MH.with_suffix(f".jsonl.bak-{time.strftime('%Y%m%d')}")
    shutil.copy2(MH, bak)
    MH.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")
    print(f"\nwrote {MH.name} ({len(rows)} rows); backup at {bak.name}")

    # Surgical gold refresh — ONLY the agendas CSV, using the promote script's own writer
    # (same shape, same _CANON_LA key canonicalisation, same date fallback).
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
            for r in rows
            if r.get("agenda_items")
        ],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
