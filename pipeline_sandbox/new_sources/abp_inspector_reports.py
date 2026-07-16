"""An Coimisiún Pleanála (ABP) INSPECTOR-REPORT text layer — sandbox ingest.

WHY: the case-level appeal OUTCOMES are already ingested and promoted
(`extractors/planning_appeal_outcomes.py`, from the ACP Cases_2016_Onwards ArcGIS
FeatureServer, registry PC02, CC-BY). That gives decision / authority / category /
location / dates — the WHAT. It does NOT give the WHY: the inspector's reasoning,
the council's reasons for refusal, how each constraint (SAC/AA, flood, traffic/TII,
land-use/zoning) was treated, the recommendation, and the conditions. That reasoning
corpus is the single highest-value planning add (see memory
project_planning_feasibility_product_learnings_2026_07_14) and is what turns a
constraint-triage tool into a precedent engine.

WHAT THIS DOES: drives off the SAME ACP case list, and for each case fetches the
inspector's report PDF from the deterministic pleanala.ie path
    https://www.pleanala.ie/anbordpleanala/media/abp/cases/reports/<grp>/r<caseid>.pdf
(<grp> = first 3 digits of the 6-digit case core; verified working for r315397).
Born-digital PDFs → fitz text (NO OCR). First-pass structure: section presence
flags, inspector name/date, recommendation verdict, and short reasons/conditions
snippets. Raw PDF + extracted text cached to bronze with a SHA-256.

ISOLATION: sandbox only. Reads nothing in the repo data tree, writes nothing there,
does not touch pipeline.py. Output under c:/tmp/dail_new_sources/ via _common. Bounded
by --authority / --limit; default is a small Galway County proof batch. Nothing promoted.

Licence: ACP case register is CC-BY (as used by the promoted outcomes chain); the
reports are official public documents. Confirm ACP re-use terms for the report PDFs
in doc/source_licensing.md before any promotion.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import re

import fitz  # PyMuPDF — born-digital text, no OCR
import polars as pl

from pipeline_sandbox.new_sources._common import SILVER as SILVER_DIR
from pipeline_sandbox.new_sources._common import cache_raw, fetch, write_silver

ACP = (
    "https://services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/"
    "Cases_2016_Onwards/FeatureServer/3/query"
)
REPORT_URL = "https://www.pleanala.ie/anbordpleanala/media/abp/cases/reports/{grp}/r{cid}.pdf"
SOURCE = "abp_inspector_reports"
MISSES = SILVER_DIR / "abp_report_misses.txt"
_SIX = re.compile(r"\d{6}")

# Section headings that recur in ACP inspector reports (matched case-insensitively).
_SECTIONS = {
    "reasons_for_refusal": r"reasons?\s+for\s+refusal",
    "grounds_of_appeal": r"grounds?\s+of\s+appeal",
    "planning_history": r"planning\s+history",
    "assessment": r"\bassessment\b",
    "appropriate_assessment": r"appropriate\s+assessment",
    "eia": r"\bEIA\b|environmental\s+impact",
    "flood": r"flood",
    "traffic": r"traffic|road\s+safety|\bTII\b|access",
    "recommendation": r"\brecommendation\b",
    "reasons_and_considerations": r"reasons?\s+and\s+considerations",
    "conditions": r"\bconditions?\b",
}
# Inspector signature block: a name on its own line (optionally under a "____" signature rule)
# immediately above "(Senior) Planning Inspector". fitz keeps the line breaks, so require the newline
# (stops "Senior"/"Planning" being swallowed into the name). Take the LAST match = the end signature.
_INSPECTOR = re.compile(
    r"(?:^|\n)\s*_*\s*([A-Z][A-Za-z'’]+(?:[ ][A-Z][A-Za-z'’.]+){1,2})\s*\n+\s*(?:Senior\s+)?Planning\s+Inspector\b"
)
# Verdict: find the RECOMMENDATION WINDOW, then the operative decision verb inside it.
# Wording varies a lot ("should be granted" / "be granted for the following reasons" / "the Board
# grant planning permission" / bare "Grant permission subject to conditions" / "should be refused"
# with no "recommend" at all), so match the verb, not a fixed sentence shape.
# Windows are evaluated LAST-FIRST (the operative recommendation sits near the end; a table-of-
# contents entry carries no decision verb, so it falls through harmlessly).
_REC_HEADING = re.compile(r"(?:^|\n)\s*[\d.]*\s*Recommendations?\s*\n", re.I)  # [\d.]* handles "9.0", "10.0"
_REC_SENTENCE = re.compile(r"(?:I\s+recommend|It\s+is\s+recommended)\b[^.]{0,220}\.", re.I)
# Ordered by position-of-match, NOT list order: the EARLIEST verb in the window is the operative one
# (so "granted notwithstanding the planning authority's reasons for refusal" reads as GRANT).
_DECISION_PATS = [
    (re.compile(r"(?:be|is|should\s+be)\s+refused", re.I), "REFUSE"),
    (re.compile(r"(?:be|is|should\s+be)\s+granted", re.I), "GRANT"),
    (re.compile(r"\b(?:grant|granting)\s+(?:planning\s+|retention\s+)?permission", re.I), "GRANT"),
    (re.compile(r"\brefuse\s+(?:planning\s+|retention\s+)?permission", re.I), "REFUSE"),
]
# NOTE: a null verdict is often CORRECT — condition-only appeals ("I recommend that Condition No. 2
# be revised"), s.9(5) vacant-site confirmations and s.5 referrals carry no grant/refuse at all.


def _verdict(text: str) -> tuple[str | None, str | None]:
    windows: list[str] = []
    for m in _REC_HEADING.finditer(text):
        windows.append(text[m.end(): m.end() + 500])
    windows.extend(m.group(0) for m in _REC_SENTENCE.finditer(text))
    for w in reversed(windows):
        best: tuple[int, str] | None = None
        for pat, v in _DECISION_PATS:
            mm = pat.search(w)
            if mm and (best is None or mm.start() < best[0]):
                best = (mm.start(), v)
        if best:
            return best[1], re.sub(r"\s+", " ", w[:220]).strip()
    return None, None


def _year(ms) -> int | None:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).year if ms else None


def fetch_all_cases(authority: str, since_year: int) -> pl.DataFrame:
    """Full (paginated) ACP case list, filtered to `since_year`+ and PRIORITISED.

    Year cutoff is on relevance as much as cost: the NPF landed 2018, the OPR in 2019, and most
    councils' current development plans start ~2022 — an appeal decided pre-2020 was judged against
    a superseded plan and a pre-OPR framework. (The source itself floors at 2016.)

    Queue order = Appeals first, then most-recent-first, so the precedent-bearing cases land first
    and value accrues even if a run is stopped early. Metadata only — the PDF is the expensive part.
    """
    where = f"PLANINGATY LIKE '%{authority}%'" if authority else "1=1"
    rows, off = [], 0
    while True:
        payload, _ = fetch(
            ACP,
            params={
                "where": where,
                "outFields": "ABPCASEID,DECISION,PLANINGATY,CATEGORY,DECIDED_ON,LODGEDON",
                "returnGeometry": "false",
                "orderByFields": "DECIDED_ON DESC",
                "resultOffset": off,
                "resultRecordCount": 2000,
                "f": "json",
            },
        )
        feats = json.loads(payload).get("features", [])
        if not feats:
            break
        rows.extend(f["attributes"] for f in feats)
        off += len(feats)
        if len(feats) < 2000:
            break
    df = pl.DataFrame(rows)
    if df.is_empty():
        return df
    df = df.with_columns(
        pl.col("ABPCASEID").cast(pl.Utf8).map_elements(
            lambda s: (_SIX.search(s or "").group(0) if _SIX.search(s or "") else None), return_dtype=pl.Utf8
        ).alias("case_core"),
        pl.coalesce(
            pl.col("DECIDED_ON").map_elements(_year, return_dtype=pl.Int32),
            pl.col("LODGEDON").map_elements(_year, return_dtype=pl.Int32),
        ).alias("case_year"),
    )
    # ~2% of cases carry no 6-digit core → unreachable by the report URL pattern. Known coverage hole.
    unreachable = df.filter(pl.col("case_core").is_null()).height
    df = df.filter(pl.col("case_core").is_not_null()).unique(subset=["case_core"], keep="first")
    print(f"[cases] {df.height} reachable (6-digit core); {unreachable} with NO core = permanent coverage hole")
    df = df.filter(pl.col("case_year") >= since_year)
    # An UNDECIDED case has no inspector report yet — fetching it is a guaranteed 404. Recent-first
    # ordering runs straight into the in-progress 2026 cases, so drop them: measured 404 rate fell
    # from ~40% to the true "decided but no report" floor (direct Board orders).
    pre = df.height
    # TRAP: `DECISION` is NOT null for a live case — it holds a STATUS STRING ("Case due to be decided
    # by 21-07-2026"). The only reliable "is it decided" signal is a non-null DECIDED_ON.
    _UNDECIDED.update(df.filter(pl.col("DECIDED_ON").is_null())["case_core"].to_list())
    df = df.filter(pl.col("DECIDED_ON").is_not_null())
    print(f"[scope] dropped {pre - df.height} UNDECIDED cases (DECIDED_ON null → no report exists)")
    # Order: Appeals first (the precedent-bearing category), then LIKELY-PUBLISHED first, then most
    # recent. Without the publish guard the queue front-loads the newest 501xxx cases, whose reports
    # aren't uploaded yet — they 404 at ~98% and would burn the batch. They sort to the tail and are
    # retried on later runs instead.
    # TRAP: map_elements returns NULL for a null input, and polars sorts NULLS FIRST on a descending
    # sort — an unfilled _publishable floats every unpublished case to the TOP of the queue, the exact
    # inverse of the intent (this is what produced a 93% 404 batch). fill_null(False) + nulls_last.
    df = df.with_columns(
        (pl.col("CATEGORY") == "Appeals").alias("_appeal"),
        pl.col("DECIDED_ON")
        .map_elements(lambda ms: not _recently_decided(ms, PUBLISH_LAG_DAYS), return_dtype=pl.Boolean)
        .fill_null(False)
        .alias("_publishable"),
    )
    lag = df.filter(~pl.col("_publishable")).height
    print(f"[scope] {lag} cases decided <{PUBLISH_LAG_DAYS}d ago → sorted to TAIL (report likely not up yet)")
    return df.sort(["_appeal", "_publishable", "case_year"], descending=[True, True, True], nulls_last=True)


MISS_RETRY_DAYS = 180  # a 404 on a RECENTLY-decided case is publication lag, not "no report"
PUBLISH_LAG_DAYS = 90  # cases decided more recently than this are unlikely to have a report up yet
_UNDECIDED: set[str] = set()  # live cases seen this run — never legitimate "no report" misses
CHECKPOINT_EVERY = 100  # persist silver+misses every N new reports, so a long run is interruptible


def _recently_decided(ms, days: int = MISS_RETRY_DAYS) -> bool:
    """True if the case was decided within `days`. ACP publishes the inspector's report AFTER the
    decision, with a lag — the newest cases (501xxx) 404 at ~98% purely because the PDF isn't up
    yet. Treating those 404s as permanent would blacklist the most valuable recent precedents
    forever, so they stay retryable."""
    if not ms:
        return True  # no decision date → don't blacklist it
    decided = dt.datetime.fromtimestamp(ms / 1000, dt.UTC)
    return (dt.datetime.now(dt.UTC) - decided).days < days


def _load_misses() -> set[str]:
    """Cases that 404'd. NOTE: only a PERMANENT miss (decided long ago → genuinely no report, e.g.
    a direct Board order) is skipped on re-runs; recently-decided misses are retried — see the
    _recently_decided() guard where the queue is built."""
    return set(MISSES.read_text().split()) if MISSES.exists() else set()


def _save_misses(misses: set[str]) -> None:
    MISSES.write_text("\n".join(sorted(misses)))


def _load_existing() -> pl.DataFrame | None:
    p = SILVER_DIR / f"{SOURCE}.parquet"
    return pl.read_parquet(p) if p.exists() else None


def parse_report(text: str) -> dict:
    low = text.lower()
    out = {f"has_{k}": bool(re.search(pat, low)) for k, pat in _SECTIONS.items()}
    insp = list(_INSPECTOR.finditer(text))
    out["inspector"] = insp[-1].group(1).strip() if insp else None
    out["recommendation_verdict"], out["recommendation_snippet"] = _verdict(text)
    return out


def extract_pdf(content: bytes) -> tuple[str, int, int]:
    """→ (text, page_count, image_only_pages).

    ~1.5% of reports are SCANNED (100% image pages, ZERO extractable text). Without this count
    they land silently as an empty-text row and quietly degrade the corpus — so count the pages
    that carry an image but no text, and let the caller flag them for a bounded OCR pass.
    Born-digital pages that merely carry a figure/logo are NOT image_only: their text extracts
    fine and needs no OCR (verified: the biggest raster on a text page is the ABP letterhead)."""
    with fitz.open(stream=io.BytesIO(content), filetype="pdf") as doc:
        texts, img_only = [], 0
        for p in doc:
            t = p.get_text()
            if len(t.strip()) < 50 and p.get_images():
                img_only += 1
            texts.append(t)
        return "\f".join(texts), doc.page_count, img_only


def scan_flags(text: str, pages: int, img_only: int) -> dict:
    scanned = pages > 0 and (img_only / pages) > 0.5
    return {
        "image_only_pages": img_only,
        "is_scanned": scanned,
        # OCR is worth it ONLY where text is actually missing — never for illustrative figures.
        "needs_ocr": scanned or len(text.strip()) < 200,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--authority", default="", help="PLANINGATY substring; '' (default) = nationwide")
    ap.add_argument("--since-year", type=int, default=2020, help="only cases decided/lodged this year or later")
    ap.add_argument("--max-fetch", type=int, default=250, help="cap on NEW report PDFs fetched this run")
    ap.add_argument("--keep-pdf", action="store_true", help="cache every PDF (default: text-only + PDFs for needs_ocr)")
    ap.add_argument("--reparse", action="store_true", help="re-parse cached bronze .txt only (no network)")
    args = ap.parse_args()

    if args.reparse:
        # Re-derive the parsed fields from the CACHED bronze .txt — no network. Lets the verdict/
        # inspector regexes be improved and re-applied without re-fetching a single PDF.
        ex = _load_existing()
        if ex is None:
            print("[reparse] no existing silver — nothing to re-parse")
            return
        from pipeline_sandbox.new_sources._common import BRONZE

        out = []
        for r in ex.iter_rows(named=True):
            t = BRONZE / SOURCE / f"r{r['abp_case']}.txt"
            pdf = BRONZE / SOURCE / f"r{r['abp_case']}.pdf"
            if not t.exists():
                out.append(r)
                continue
            text = t.read_text(encoding="utf-8", errors="ignore")
            row = {**r, **parse_report(text)}
            if pdf.exists():  # recompute the scan flags from the cached PDF (no network)
                _, pages, img_only = extract_pdf(pdf.read_bytes())
                flags = scan_flags(text, pages, img_only)
                row = {
                    **row,
                    **flags,
                    "extraction_method": "ocr_required" if flags["needs_ocr"] else "fitz_text",
                    "confidence": "none" if flags["needs_ocr"] else ("high" if len(text) > 2000 else "low"),
                }
            out.append(row)
        df = pl.DataFrame(out)
        write_silver(SOURCE, df)
        appeals = df.filter(pl.col("category") == "Appeals")
        print(f"[reparse] {df.height} rows re-parsed from bronze")
        print(f"[evidence] Appeals={appeals.height}  verdicts: {dict(appeals.group_by('recommendation_verdict').len().iter_rows())}")
        print(f"[evidence] inspector named: {df['inspector'].is_not_null().sum()}/{df.height}")
        if "needs_ocr" in df.columns:
            n = df.filter(pl.col("needs_ocr")).height
            print(f"[evidence] SCANNED / needs_ocr: {n}/{df.height} ({100*n/df.height:.1f}%) — bounded OCR queue")
        return

    # self-test the URL pattern on a known case (315397 = the CSSI case) before any batch
    try:
        probe, _ = fetch(REPORT_URL.format(grp="315", cid="315397"), binary=True)
        print(f"[self-test] r315397.pdf → {len(probe):,} bytes ({'PDF OK' if probe[:4] == b'%PDF' else 'NOT PDF'})")
    except Exception as e:  # noqa: BLE001
        print(f"[self-test] FAILED: {e}")

    existing = _load_existing()
    done: set[str] = set(existing["abp_case"].to_list()) if existing is not None else set()
    misses = _load_misses()
    print(f"[resume] already parsed={len(done)}  known-404={len(misses)}")

    cases = fetch_all_cases(args.authority, args.since_year)
    print(f"[scope] {cases.height} cases ({args.authority or 'nationwide'}) from {args.since_year}+, Appeals-first")
    if cases.is_empty():
        return

    # Purge misses recorded against cases that are simply NOT DECIDED YET — they were never
    # legitimate "no report exists" records, and leaving them would misreport coverage.
    bogus = misses & _UNDECIDED
    if bogus:
        misses -= bogus
        _save_misses(misses)
        print(f"[misses] purged {len(bogus)} bogus entries (undecided cases, not 'no report')")

    # Skip a cached miss ONLY if it is permanent (decided long enough ago that a 404 means the report
    # genuinely does not exist). Recently-decided misses are retried — their PDF may have landed since.
    todo, lagging = [], 0
    for r in cases.iter_rows(named=True):
        c = r["case_core"]
        if c in done:
            continue
        if c in misses:
            if not _recently_decided(r.get("DECIDED_ON")):
                continue  # permanent no-report
            lagging += 1  # retry: publication lag
        todo.append(r)
    print(
        f"[queue] {len(todo)} cases to try ({lagging} are retries of recent 404s = publication lag); "
        f"fetching up to {args.max_fetch} this run"
    )

    def _flush(new_rows: list[dict]) -> None:
        """Checkpoint: merge new rows into the silver and persist misses. Called periodically so a
        long backfill is safe to interrupt — without this, killing a 5-hour run loses every row."""
        if not new_rows:
            return
        base = _load_existing()
        out = pl.DataFrame(new_rows)
        if base is not None:
            out = pl.concat([base, out], how="diagonal_relaxed").unique(subset=["abp_case"], keep="last")
        write_silver(SOURCE, out)
        _save_misses(misses)

    rows, found, new_miss, kept_pdf = [], 0, 0, 0
    for r in todo[: args.max_fetch]:
        cid = r["case_core"]
        url = REPORT_URL.format(grp=cid[:3], cid=cid)
        try:
            content, meta = fetch(url, binary=True)
        except Exception:  # noqa: BLE001 — 404 = no inspector report (direct order); expected, cached as a miss
            misses.add(cid)
            new_miss += 1
            continue
        if content[:4] != b"%PDF":
            misses.add(cid)
            new_miss += 1
            continue
        found += 1
        try:
            text, pages, img_only = extract_pdf(content)
        except Exception as e:  # noqa: BLE001
            print(f"  ! parse fail {cid}: {e}")
            continue
        cache_raw(SOURCE, f"r{cid}.txt", text.encode("utf-8"))
        flags = scan_flags(text, pages, img_only)
        # TEXT-ONLY by default: measured mean PDF 0.51 MB vs 55 KB of text (~9x). The SHA-256 + the
        # deterministic URL preserve provenance, so a PDF can always be re-fetched. Keep the binary
        # ONLY where it's actually needed again — the ~1.7% flagged needs_ocr.
        if flags["needs_ocr"] or args.keep_pdf:
            cache_raw(SOURCE, f"r{cid}.pdf", content)
            kept_pdf += 1
        rows.append(
            {
                "abp_case": cid,
                "planning_authority": r.get("PLANINGATY"),
                "category": r.get("CATEGORY"),
                "case_year": r.get("case_year"),
                "abp_decision_raw": r.get("DECISION"),
                "report_url": url,
                "source_document_hash": meta["source_document_hash"],
                "fetched_at": meta["fetched_at"],
                "bytes": meta["bytes"],
                "page_count": pages,
                "text_chars": len(text),
                **flags,
                **parse_report(text),
                "extraction_method": "ocr_required" if flags["needs_ocr"] else "fitz_text",
                "confidence": "none" if flags["needs_ocr"] else ("high" if len(text) > 2000 else "low"),
                "privacy_tier": "public",
                "source_published_date": None,
                "source_last_modified": meta.get("source_last_modified"),
            }
        )
        if len(rows) and len(rows) % CHECKPOINT_EVERY == 0:
            _flush(rows)
            print(f"  [checkpoint] {len(rows)} new rows persisted ({found} found / {new_miss} 404s so far)")
    _save_misses(misses)
    print(f"[fetch] new reports={found}  new 404s={new_miss}  PDFs kept={kept_pdf} (text-only otherwise)")

    df = pl.DataFrame(rows) if rows else None
    if existing is not None and df is not None:
        df = pl.concat([existing, df], how="diagonal_relaxed").unique(subset=["abp_case"], keep="last")
    elif df is None:
        df = existing
    if df is None or df.is_empty():
        print("[done] nothing to write")
        return

    p = write_silver(SOURCE, df)
    remaining = len(todo) - min(len(todo), args.max_fetch)
    print(f"[silver] {df.height} cumulative rows → {p}   ({remaining} cases still queued — re-run to continue)")

    appeals = df.filter(pl.col("category") == "Appeals")
    verd = dict(appeals.group_by("recommendation_verdict").len().iter_rows())
    print(f"[evidence] Appeals={appeals.height}  verdicts: {verd}")
    print(f"[evidence] inspector named: {df['inspector'].is_not_null().sum()}/{df.height}")
    print(f"[evidence] mean pages={df['page_count'].mean():.1f}  mean chars={df['text_chars'].mean():.0f}")
    print(f"[evidence] authorities covered: {df['planning_authority'].n_unique()}")


if __name__ == "__main__":
    main()
