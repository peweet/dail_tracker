"""main() + merge/idempotency logic for the public-body payments extractor
package (split out of extractors/procurement_public_body_extract.py,
doc/REFACTORING_CANDIDATES.md C7, pure move-function -- no logic changes).

The publisher_id-dedup merge block, the save_parquet call and its min_rows
row-floor guard, and the privacy runtime invariant are all untouched.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import time
from datetime import UTC, datetime

import polars as pl

from extractors._paid_flag_clean import clean_paid_flag
from services.fetch_report import Breaker, classify_body, write_sentinel
from services.parquet_io import save_parquet

from .config import DATA_EXT, MIN_FACT_ROWS, OUT_COV, OUT_FACT, PARSER_VERSION, PAYMENTS_FACT_SCHEMA_COLS, PUBLISHERS
from .emit import canonicalise_supplier_raw, classify_and_flag, dedup_source_repeats, emit_rows, flag_unidentifiable_suppliers
from .harvest import LAST_ERR, REPORT, fetch_to_bronze, harvest_files

# ============================================================================ main
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="harvest-only: print candidate files, no parse")
    ap.add_argument("--only", default="", help="comma-separated publisher ids")
    ap.add_argument(
        "--max-files", type=int, default=None, help="cap files parsed per publisher (default: all — full history)"
    )
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages per PDF")
    ap.add_argument(
        "--merge",
        action="store_true",
        help="with --only: update JUST those publishers inside the existing fact (all other "
        "publishers kept) instead of overwriting the whole parquet — avoids re-downloading the "
        "full ~600-file history and prevents the clobber-to-N-publishers footgun",
    )
    ap.add_argument("--refetch", action="store_true", help="bypass the bronze cache and re-download every file")
    args = ap.parse_args()
    only = {x.strip() for x in args.only.split(",") if x.strip()} or None
    if args.merge and not only:
        raise SystemExit("--merge requires --only (it updates the named publishers in place)")

    pubs = [p for p in PUBLISHERS if not only or p["id"] in only]
    print(f"{'=' * 80}\nPUBLIC-BODY EXTRACT{' (LIST MODE)' if args.list else ''} — {len(pubs)} publishers\n{'=' * 80}")

    all_rows: list[dict] = []
    per_pub: list[dict] = []
    for cf in pubs:
        files = harvest_files(cf)
        print(f"\n[{cf['id']:<22}] {cf['name']}  (tier {cf['tier']}, {cf['amount_semantics']})")
        print(f"   listing: {cf['listing_url']}")
        print(f"   files harvested: {len(files)}")
        if not files:
            REPORT.record_zero_harvest(publisher_id=cf["id"], publisher_name=cf["name"], listing_url=cf["listing_url"])
        for u in files[:8]:
            print(f"     - {u.rsplit('/', 1)[-1][:70]}")
        if len(files) > 8:
            print(f"     … +{len(files) - 8} more")
        if args.list:
            per_pub.append({"id": cf["id"], "files": len(files)})
            continue

        pub_rows, parsed, ok_files, skipped = [], 0, 0, 0
        breaker = Breaker()
        file_list = files[: args.max_files]
        for i, u in enumerate(file_list):
            ext = next((e for e in DATA_EXT if u.lower().split("?")[0].endswith(e)), "")
            fmt = {".pdf": "pdf", ".xlsx": "xlsx", ".xls": "xls", ".csv": "csv"}.get(ext)
            if not fmt:
                continue
            b, fresh = fetch_to_bronze(cf["id"], u, ext, refetch=args.refetch)
            if not b and fresh:
                time.sleep(30.0)  # cool off once before the single retry
                b, fresh = fetch_to_bronze(cf["id"], u, ext, refetch=True)
            bad = classify_body(b, b"%PDF" if fmt == "pdf" else None) if b else None
            if not b or bad:
                breaker.record(False)
                err = bad or LAST_ERR.get("error_class", "unknown")
                REPORT.record_failure(
                    publisher_id=cf["id"],
                    publisher_name=cf["name"],
                    url=u,
                    listing_url=cf["listing_url"],
                    error_class=err,
                    http_status=LAST_ERR.get("http_status"),
                    attempts=4 if not b else 1,
                )
                print(f"     ! download failed ({err}): {u.rsplit('/', 1)[-1][:50]}")
                if breaker.tripped:
                    rest = len(file_list) - i - 1
                    REPORT.record_breaker_trip(publisher_id=cf["id"], publisher_name=cf["name"], files_skipped=rest)
                    print(
                        f"     !! breaker tripped: {breaker.consecutive} consecutive failures — skipping {rest} remaining files"
                    )
                    break
                continue
            breaker.record(True)
            write_sentinel("public_body", cf["id"], u)
            try:
                rows, stat = emit_rows(cf, u, b, fmt, args.max_pages)
            except Exception as e:  # one malformed file must not abort the run
                skipped += 1
                print(f"     ! parse error ({type(e).__name__}): {u.rsplit('/', 1)[-1][:46]}")
                continue
            parsed += 1
            if rows:
                ok_files += 1
            pub_rows.extend(rows)
            print(
                f"     -> {u.rsplit('/', 1)[-1][:48]:<48} {stat['status']:<8} "
                f"rows={stat['rows']} conf={stat['confidence']}"
            )
        all_rows.extend(pub_rows)
        per_pub.append(
            {
                "id": cf["id"],
                "name": cf["name"],
                "tier": cf["tier"],
                "files_seen": len(files),
                "files_parsed": parsed,
                "files_skipped": skipped,
                "files_with_rows": ok_files,
                "rows": len(pub_rows),
                "amount_semantics": cf["amount_semantics"],
                "privacy_risk": cf["privacy_risk"],
            }
        )

    if args.list:
        print(f"\n{'=' * 80}\nLIST DONE. Lock URLs into config, then run without --list.")
        return

    if not all_rows:
        print("\nno rows extracted")
        REPORT.write()
        for line in REPORT.summary_lines():
            print(line)
        return

    SCHEMA_COLS = PAYMENTS_FACT_SCHEMA_COLS
    df = pl.DataFrame(all_rows, infer_schema_length=None)
    df, rows_deduped = dedup_source_repeats(df)
    if rows_deduped:
        print(f"\ndeduped {rows_deduped:,} within-file parser repeats (indistinguishable rows)")
    df = canonicalise_supplier_raw(df)
    df = classify_and_flag(df)
    df = flag_unidentifiable_suppliers(df)
    df = df.select([c for c in SCHEMA_COLS if c in df.columns])

    # MERGE MODE: fold the freshly-parsed publishers into the existing fact instead of replacing
    # it. Drop any prior rows for the publishers we just (re)parsed — idempotent — then concat the
    # untouched publishers back on. The kept rows are already fully classified (same SCHEMA_COLS),
    # so no re-classification is needed; we align dtypes defensively before concat.
    merged_kept_pubs: list[dict] = []
    if args.merge and OUT_FACT.exists():
        existing = pl.read_parquet(OUT_FACT)
        sel_ids = [p["id"] for p in pubs]
        kept = existing.filter(~pl.col("publisher_id").is_in(sel_ids))
        print(
            f"\nMERGE: existing fact {existing.height:,} rows / {existing['publisher_id'].n_unique()} "
            f"publishers; replacing {existing.height - kept.height:,} rows for {sel_ids}, "
            f"keeping {kept.height:,} rows for {kept['publisher_id'].n_unique()} other publishers"
        )
        df = df.select(existing.columns).cast(dict(existing.schema))
        df = pl.concat([kept, df], how="vertical")
        # Preserve the by_publisher coverage entries for the publishers we did NOT reparse.
        if OUT_COV.exists():
            with contextlib.suppress(Exception):
                old_cov = json.loads(OUT_COV.read_text(encoding="utf-8"))
                merged_kept_pubs = [e for e in old_cov.get("by_publisher", []) if e.get("id") not in sel_ids]

    # paid_flag column-misalignment repair (see extractors/_paid_flag_clean.py): a publisher
    # whose layout put a category/date/amount column where the paid flag usually sits would
    # otherwise leak that content into paid_flag (~16% of rows pre-fix). Clean the final frame —
    # in merge mode this also re-cleans carried-forward publishers (idempotent), so the whole
    # fact converges to a flag-or-null paid_flag with recoverable category text moved into
    # description. Applied here (after merge, before the invariants/write) so silver is clean at
    # source and gold inherits it through the silver→gold fold.
    df, _pf_stats = clean_paid_flag(df)
    if _pf_stats.get("n_leak"):
        print(
            f"\npaid_flag repair: {_pf_stats['n_leak']:,} leaked values cleared "
            f"({_pf_stats.get('n_recovered', 0):,} category texts recovered into description, "
            f"{_pf_stats.get('n_month', 0):,} dates / {_pf_stats.get('n_amount', 0):,} amounts nulled); "
            f"{_pf_stats.get('n_genuine', 0):,} genuine flags kept"
        )

    # PRIVACY INVARIANT (runtime, -O-proof): no displayable row may be a likely person.
    leaked = df.filter(pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual"))
    if leaked.height:
        raise RuntimeError(
            f"privacy quarantine breached: {leaked.height} sole_trader_or_individual rows "
            "left public_display=True; refusing to write public_payments_fact"
        )

    OUT_FACT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_FACT, min_rows=MIN_FACT_ROWS)

    print(f"\n{'=' * 80}\nGOLD-CANDIDATE WRITTEN\n{'=' * 80}")
    print(f"rows: {df.height:,}  ->  {OUT_FACT}")
    print(df.group_by("supplier_class").len().sort("len", descending=True))
    safe = df.filter(pl.col("value_safe_to_sum"))
    print(
        f"\nvalue_safe_to_sum rows: {safe.height:,}  "
        f"sum=€{(safe['amount_eur'].sum() or 0):,.0f} (po_committed+payment_actual only)"
    )

    by_publisher = merged_kept_pubs + per_pub  # kept (merge mode) + freshly parsed this run
    # HONESTY RECONCILIATION: a carried-forward coverage entry (merge mode) keeps the PRIOR run's
    # row count, which can drift from the written fact — e.g. dept_children (DCEDIY asylum/IP +
    # Ukraine accommodation POs) is a reading-order PDF layout the generic parser yields 0 rows for,
    # yet its stale entry kept reporting 30,526 rows, masking the loss. Recompute every entry's row
    # count from the actual fact so coverage can NEVER claim rows the fact does not contain.
    _actual_rows = dict(df.group_by("publisher_id").len().iter_rows())
    for _e in by_publisher:
        _n = _actual_rows.get(_e.get("id"), 0)
        if _n != _e.get("rows"):
            _e["rows"] = _n
            if _n == 0:
                _e["files_with_rows"] = 0  # 0 rows in the fact ⇒ no file contributed rows
    cov = {
        "publishers_attempted": len(by_publisher),
        "publishers_with_rows": sum(p.get("rows", 0) > 0 for p in by_publisher),
        "rows_extracted": df.height,
        "rows_deduped_within_file": rows_deduped,
        "rows_public_display": int(df["public_display"].sum()),
        "rows_review_personal_data": int((df["privacy_status"] == "review_personal_data").sum()),
        "rows_quarantined": int((~df["public_display"]).sum()),
        "supplier_class_counts": {
            r["supplier_class"]: r["len"] for r in df.group_by("supplier_class").len().iter_rows(named=True)
        },
        "amount_semantics_counts": {
            r["amount_semantics"]: r["len"] for r in df.group_by("amount_semantics").len().iter_rows(named=True)
        },
        "value_safe_to_sum_rows": safe.height,
        "value_safe_to_sum_total_eur": float(safe["amount_eur"].sum() or 0),
        "by_publisher": by_publisher,
        "privacy_quarantine_applied": True,
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "GOLD-CANDIDATE (sandbox, pre-promotion). One row per source line. "
        "amount_semantics distinguishes po_committed/payment_actual/contract_award_value; "
        "only value_safe_to_sum (po_committed+payment_actual, EXCLUDING public_body "
        "recipients which are intergovernmental transfers/grants e.g. TII road grants) "
        "may be totalled, labelled 'ordered/paid', never mixed with award ceilings. "
        "PRIVACY QUARANTINE APPLIED: "
        "rows flagged privacy_status=review_personal_data (likely sole traders / "
        "individuals) are marked public_display=False and must be filtered out before any "
        "UI use; they are retained here for analysis only. "
        "A line is a purchase order or payment record, not evidence of influence.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")

    report_path = REPORT.write()
    lines = REPORT.summary_lines()
    if lines:
        print(f"\nfetch-failure report -> {report_path}")
        for line in lines:
            print(line)
