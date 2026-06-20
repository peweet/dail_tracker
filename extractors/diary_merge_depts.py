"""Targeted merge of re-parsed departments into the canonical diary sandbox.

Lets us land a FEW departments' freshly-parsed entries WITHOUT a full corpus
re-extract (which would re-hammer the gov.ie WAF). Used 2026-06-20 to land Finance
(the Chambers/DFIN calendar-export layout the parser now handles) and DECC (the
Climate/Energy/Environment diary, retry-downloaded) after the parser fix.

Network: only the per-dept LISTING crawl (a few GETs); the PDFs themselves come
from the on-disk cache (download() is a cache hit), so no bulk re-download.

DEDUP (load-bearing): Eamon Ryan held Transport AND Climate/Environment at once and
his diary is published in BOTH collections — ~52% of DECC entries are byte-identical
to existing TRANSPORT rows (measured). We anti-join the incoming rows against the
rows we are KEEPING on (minister, entry_date, time_slot, subject) so the cross-
published entries are not double-counted, while DECC-unique meetings survive.

After this, run the rest of the chain in order (it re-stamps entry_class/entry_id
and rebuilds mentions/overlap/gold):
  diary_entry_classify -> diary_org_match -> diary_lobbying_overlap -> diary_promote_gold

Run: .venv/Scripts/python.exe extractors/diary_merge_depts.py --depts FINANCE,DECC
"""

from __future__ import annotations

import argparse
import json
import logging
import unicodedata
from datetime import date
from pathlib import Path

import fitz
import polars as pl

from extractors._diary_minister import minister_from_filename
from extractors.ministerial_diaries_extract import (
    OUT_DIR,
    _infer_default_year,
    discover_files,
    download,
    parse_entries,
)
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ENTRIES = OUT_DIR / "ministerial_diary_entries.parquet"
_RAW_COLS = ["entry_date", "time_slot", "subject", "department", "minister", "source_pdf_url", "ingested_date"]
# Dedup on a NORMALISED surname (not the raw filename guess) + date + time + subject: the canonical
# Transport rows carry "Ryans"/"Ryan" inconsistently while incoming DECC is "Ryan", so an exact-string
# key misses ~half the cross-published Eamon Ryan entries. Surname-folding catches the same person
# without collapsing genuinely different ministers who share a date/time/subject.
_DEDUP_KEY = ["_mk", "entry_date", "time_slot", "subject"]


def _surname_key(name: str | None) -> str:
    """Folded last-name token, trailing possessive 's' stripped — 'Ryans'/'Ryan' → 'ryan'."""
    if not name:
        return ""
    toks = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower().split()
    if not toks:
        return ""
    last = toks[-1]
    return last[:-1] if last.endswith("s") and len(last) > 4 else last


def parse_dept_entries(depts: set[str]) -> pl.DataFrame:
    """Re-parse the cached PDFs for the given departments into raw entry rows."""
    files = discover_files(only_depts=depts)
    rows: list[dict] = []
    skipped_scan = 0
    for f in files:
        pdf = download(f["file_url"], f["file_name"])  # cache hit — no bulk re-download
        if pdf is None:
            continue
        try:
            text = "".join(p.get_text() for p in fitz.open(pdf))
        except Exception as e:  # noqa: BLE001
            log.warning("unreadable %s: %s", f["file_name"], e)
            continue
        if len(text.strip()) <= 100:  # image-only scan → OCR queue, not here
            skipped_scan += 1
            continue
        year = f["period_year_guess"] or _infer_default_year(text)
        for e in parse_entries(text, year, f["period_month_guess"]):
            e.update(
                department=f["department"],
                minister=minister_from_filename(f["file_name"]),
                source_pdf_url=f["file_url"],
                ingested_date=date.today(),
            )
            rows.append(e)
    log.info("re-parsed %d entries from %s (%d scans skipped for OCR)", len(rows), sorted(depts), skipped_scan)
    # infer_schema_length=None: scan ALL rows so a leading run of minister=None (generic files whose
    # surname is denylisted, e.g. "Minister_DFHERIS_Calendar") doesn't infer the column as Null and
    # then fail to append a later real name ("O'Donovan").
    return (
        pl.DataFrame(rows, infer_schema_length=None).select(_RAW_COLS)
        if rows
        else pl.DataFrame(schema={c: pl.Object for c in _RAW_COLS})
    )


def _ocr_entries(json_path: str) -> pl.DataFrame:
    """Load entries recovered by extractors/diary_ocr.py (the GPU OCR runner) into raw rows.
    Minister is already resolved in the OCR step; just normalise columns + stamp ingest date."""
    recs = json.loads(Path(json_path).read_text(encoding="utf-8"))
    for r in recs:
        r.setdefault("time_slot", "")
        r["ingested_date"] = date.today()
    return pl.DataFrame(recs, infer_schema_length=None).select(_RAW_COLS)


def main(depts: set[str], ocr_json: str | None = None) -> int:
    setup_standalone_logging("diary_merge_depts")
    if not ENTRIES.exists():
        log.error("canonical entries missing: %s", ENTRIES)
        return 1

    canon = pl.read_parquet(ENTRIES).select(_RAW_COLS)
    keep = canon.filter(~pl.col("department").is_in(list(depts)))  # everything except the depts we re-parse
    src = _ocr_entries(ocr_json) if ocr_json else parse_dept_entries(depts)
    incoming = src.with_columns(pl.col("entry_date").cast(keep["entry_date"].dtype))

    # normalised surname key on both sides for the dedup join
    mk = pl.col("minister").map_elements(_surname_key, return_dtype=pl.Utf8).alias("_mk")
    keep_keys = keep.with_columns(mk).select(_DEDUP_KEY).unique()
    incoming = incoming.with_columns(mk)

    # anti-join incoming against the rows we keep → drop cross-published dupes (DECC<->TRANSPORT
    # Ryan overlap), then drop any internal dupes within incoming.
    before = len(incoming)
    incoming = incoming.join(keep_keys, on=_DEDUP_KEY, how="anti").unique(subset=_DEDUP_KEY).drop("_mk")
    log.info("incoming after cross-dept + internal dedup: %d (dropped %d dupes)", len(incoming), before - len(incoming))

    merged = pl.concat([keep, incoming], how="vertical_relaxed")
    log.info(
        "merged canonical entries: %d (was %d; +%d net for %s)",
        len(merged),
        len(canon),
        len(merged) - len(canon),
        sorted(depts),
    )
    # floor: never shrink the corpus (a merge only adds) — guard a botched re-parse
    save_parquet(merged, ENTRIES, min_rows=len(canon))
    merged.write_csv(ENTRIES.with_suffix(".csv"))
    for dept in sorted(depts):
        n = merged.filter(pl.col("department") == dept).height
        log.info("  %s now %d raw entries", dept, n)
    log.info("NEXT: classify -> match -> overlap -> promote (chain re-stamps entry_class/entry_id)")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Merge re-parsed departments into the canonical diary sandbox.")
    ap.add_argument("--depts", required=True, help="comma-separated dept labels, e.g. FINANCE,DECC")
    ap.add_argument(
        "--ocr-json", help="merge entries from extractors/diary_ocr.py output JSON instead of re-parsing PDFs"
    )
    a = ap.parse_args()
    raise SystemExit(main({d.strip().upper() for d in a.depts.split(",")}, a.ocr_json))
