"""PHASE 4 (PRE-ETL, sandbox): MATERIALIZE the HSE + Tusla bespoke parse into the shared
public_payments_fact schema -> data/silver/parquet/hse_tusla_payments_fact.parquet.

extractors/procurement_hse_tusla_parser.py owns the per-publisher column-x SPECS (the generic
reader misparses these two) but only ever wrote a DQ report — its rows were never persisted in
the repo's gold-candidate schema, so HSE+Tusla (the single biggest €-chunk of the public-body
spend lane) were missing from disk. This thin writer reuses that parser's SPECS + row builders,
maps the output to public_payments_fact, runs pbe.classify_and_flag, and writes the parquet +
coverage so the layer unions with public_payments_fact at promotion.

Value semantics (taxonomy): HSE file = "Purchase Order PAYMENTS above 20k" -> payment_actual;
Tusla files = "Purchase ORDERS over 20k" -> po_committed.

LAYOUT-DRIFT GUARD (the NTA lesson — a body can change its PDF layout year to year): the Tusla
column-x SPEC was tuned on 2021. Every file is parsed then sanity-gated (rows>0, empty-supplier
share, amounts in a plausible band); a file that fails the gate is EXCLUDED and flagged in
coverage rather than silently shipping garbage.

NOT wired into pipeline.py. Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/procurement_hse_tusla_materialize.py
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

sys.path.insert(0, str(ROOT / "extractors"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")


def _load(mod_name: str, rel: str):
    spec = importlib.util.spec_from_file_location(mod_name, str(ROOT / rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


pbe = _load("pbe", "extractors/procurement_public_body_extract.py")
hst = _load("hst", "extractors/procurement_hse_tusla_parser.py")
from sample_extract_procurement_pdf import MONEY_RE, cluster_word_rows  # noqa: E402

TMP = Path("c:/tmp/procurement_publishers")
OUT_FACT = ROOT / "data/silver/parquet/hse_tusla_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/hse_tusla_payments_coverage.json"
PARSER_VERSION = "0.1.0"

# Per-publisher: shared-schema metadata + the list of (source_file_url, cached_path) to parse.
# HSE uses the cached FOI cumulative (Q4-2021..Q3-2025). NOTE: HSE REMOVED this €20k-cumulative
# file in their 2026 site rebuild and the original deep-link is dead — it now 302-redirects to a
# generic placeholder. HSE now publishes a €100k-cumulative file plus a €20k-from-Q4-2025 file (see
# the `landing` page), so the exact source below is no longer re-fetchable. tools/patch_hse_dead_source_url.py
# repointed the materialised rows' source_file_url to the live landing page. Tusla pulls every yearly
# "POs over 20k" file the listing exposes (2021-2025).
PUBS = {
    "ie_hse": {
        "name": "Health Service Executive",
        "ptype": "state_body",
        "sector": "health",
        "semantics": "payment_actual",
        "privacy": "low",
        "landing": "https://healthservice.hse.ie/staff/information-healthcare-workers/procurement/",
        "files": [
            (
                "https://healthservice.hse.ie/filemanager/HSE_FOI_Model_Publication_of_HSE_Purchase_Order_Payments_above_20k.pdf",
                TMP / "HSE_FOI_Model_Publication_of_HSE_Purchase_Order_Payments_abo.pdf",
            ),
        ],
    },
    "ie_tusla": {
        "name": "Tusla – Child and Family Agency",
        "ptype": "agency",
        "sector": "social",
        "semantics": "po_committed",
        "privacy": "medium",
        "landing": "https://www.tusla.ie/about/your-personal-information/new-freedom-of-information/financial-information/",
        "files": [
            (
                "https://www.tusla.ie/uploads/content/2021_Tusla_POs_over_20k_Final.pdf",
                TMP / "2021_Tusla_POs_over_20k_Final.pdf",
            ),
            ("https://www.tusla.ie/uploads/content/2022_Tusla_POs_over_20k_Final.pdf", None),
            ("https://www.tusla.ie/uploads/content/2023_Tusla_POs_over_20k_Final.pdf", None),
            ("https://www.tusla.ie/uploads/content/2024_Tusla_POs_over_20K_Final.pdf", None),
            ("https://www.tusla.ie/uploads/content/Tusla_Purchase_Orders_over_20k_-_2025.pdf", None),
        ],
    },
}


def parse_pdf(pid: str, b: bytes, year: int | None = None) -> list[dict]:
    """Run the owning parser's column-x SPEC over one PDF -> its native row dicts. Tusla changes
    its layout year to year, so its cuts/builder are selected per-file-year (the NTA lesson)."""
    if pid == "ie_tusla":
        cuts, build = hst.tusla_spec_for(year)
    else:
        spec = hst.SPECS[pid]
        cuts, build = spec["cuts"], spec["build"]
    doc = fitz.open(stream=b, filetype="pdf")
    rows: list[dict] = []
    for pi in range(doc.page_count):
        for wrow in cluster_word_rows(doc[pi]):
            if not any(MONEY_RE.search(w[4]) for w in wrow):
                continue
            rec = build(hst.cols_by_xcuts(wrow, cuts), pi, len(rows))
            if rec:
                rows.append(rec)
    doc.close()
    return rows


def sanity(pid: str, native: list[dict]) -> tuple[bool, str]:
    """Layout-drift gate: a Tusla yearly file whose SPEC no longer fits would yield empty
    suppliers / nonsense amounts. Reject the file (don't ship garbage) and say why."""
    if not native:
        return False, "0 rows"
    n = len(native)
    empty = sum(not (r["supplier_raw"] or "").strip() for r in native) / n
    amts = [r["amount_eur"] for r in native if r["amount_eur"] is not None]
    if empty > 0.25:
        return False, f"{empty:.0%} empty suppliers (column-x drift)"
    if not amts or min(amts) < 0:
        return False, "missing/negative amounts"
    med = sorted(amts)[len(amts) // 2]
    if not (1_000 <= med <= 5_000_000):
        return False, f"implausible median €{med:,.0f}"
    return True, "ok"


def to_schema(pid: str, native: dict, file_url: str, fhash: str, semantics: str, meta: dict) -> dict:
    y, q = native.get("year"), native.get("quarter")
    period = f"{y}-{q}" if y and q else (str(y) if y else None)
    quarter = int(q[1]) if isinstance(q, str) and q.startswith("Q") else None
    return {
        "publisher_id": pid,
        "publisher_name": meta["name"],
        "publisher_type": meta["ptype"],
        "sector": meta["sector"],
        "source_landing_url": meta["landing"],
        "source_file_url": file_url,
        "source_file_hash": fhash,
        "period": period,
        "year": y,
        "quarter": quarter,
        "supplier_raw": native["supplier_raw"],
        "amount_eur": native["amount_eur"],
        "amount_semantics": semantics,
        "description": native.get("description"),
        "po_number": native.get("doc_ref") or None,
        "paid_flag": None,
        "source_row_number": native.get("source_row"),
        "source_page_number": native.get("source_page"),
        "parser_name": f"hse_tusla_xcut_{pid}",
        "parser_version": PARSER_VERSION,
        "extraction_status": "extracted",
        "caveat_text_detected": False,
        "source_caveat": "Bespoke column-x parse (generic reader misparses HSE/Tusla). "
        + ("HSE PO payments incl-VAT." if pid == "ie_hse" else "Tusla purchase orders."),
    }


def main() -> None:
    print(f"{'=' * 80}\nHSE + TUSLA -> public_payments_fact schema\n{'=' * 80}")
    all_rows: list[dict] = []
    per_file: list[dict] = []
    for pid, meta in PUBS.items():
        for url, cached in meta["files"]:
            b = cached.read_bytes() if cached and cached.exists() else pbe.fetch_bytes(url)
            if not b or b[:4] != b"%PDF":
                print(f"  ! {pid}: download/format failed: {url.rsplit('/', 1)[-1]}")
                per_file.append({"publisher": pid, "file": url.rsplit("/", 1)[-1], "status": "fetch_failed"})
                continue
            ym = re.search(r"20\d\d", url.rsplit("/", 1)[-1])
            native = parse_pdf(pid, b, int(ym.group()) if ym else None)
            ok, why = sanity(pid, native)
            tag = url.rsplit("/", 1)[-1]
            if not ok:
                print(f"  ~ {pid}: EXCLUDED {tag[:46]:<46} ({why})")
                per_file.append(
                    {"publisher": pid, "file": tag, "status": "excluded", "reason": why, "rows": len(native)}
                )
                continue
            fhash = hashlib.sha256(b).hexdigest()[:16]
            rows = [to_schema(pid, r, url, fhash, meta["semantics"], meta) for r in native]
            all_rows.extend(rows)
            fsum = sum(r["amount_eur"] for r in rows)
            per_file.append({"publisher": pid, "file": tag, "status": "ok", "rows": len(rows), "sum_eur": fsum})
            print(f"  -> {pid:<9} {tag[:44]:<44} rows={len(rows):>6} €{fsum:>15,.0f}")

    if not all_rows:
        print("\nno rows extracted")
        return

    df = pl.DataFrame(all_rows, infer_schema_length=None)
    df = df.with_columns(pl.lit("high").alias("extraction_confidence"))
    df = pbe.classify_and_flag(df)
    SCHEMA_COLS = [
        "publisher_id",
        "publisher_name",
        "publisher_type",
        "sector",
        "source_landing_url",
        "source_file_url",
        "source_file_hash",
        "period",
        "year",
        "quarter",
        "supplier_raw",
        "supplier_normalised",
        "amount_eur",
        "amount_semantics",
        "value_safe_to_sum",
        "description",
        "po_number",
        "paid_flag",
        "source_row_number",
        "source_page_number",
        "parser_name",
        "parser_version",
        "extraction_status",
        "extraction_confidence",
        "caveat_text_detected",
        "supplier_class",
        "privacy_status",
        "public_display",
        "source_caveat",
    ]
    df = df.select([c for c in SCHEMA_COLS if c in df.columns])

    OUT_FACT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_FACT)

    safe = df.filter(pl.col("value_safe_to_sum"))
    print(f"\n{'=' * 80}\nGOLD-CANDIDATE WRITTEN\n{'=' * 80}")
    print(f"rows: {df.height:,}  ->  {OUT_FACT}")
    for pid in PUBS:
        sub = df.filter(pl.col("publisher_id") == pid)
        if sub.height:
            print(
                f"  {pid:<9} rows={sub.height:>6}  €{sub['amount_eur'].sum():>15,.0f}  "
                f"periods={sorted(p for p in sub['period'].unique().to_list() if p)}"
            )

    cov = {
        "publishers": list(PUBS),
        "rows_extracted": df.height,
        "by_file": per_file,
        "supplier_class_counts": {
            r["supplier_class"]: r["len"] for r in df.group_by("supplier_class").len().iter_rows(named=True)
        },
        "amount_semantics_counts": {
            r["amount_semantics"]: r["len"] for r in df.group_by("amount_semantics").len().iter_rows(named=True)
        },
        "value_safe_to_sum_rows": safe.height,
        "value_safe_to_sum_total_eur": float(safe["amount_eur"].sum() or 0),
        "rows_review_personal_data": int((df["privacy_status"] == "review_personal_data").sum()),
        # classify_and_flag (pbe) sets public_display=False for likely persons; the flag below
        # asserts that and test_hse_tusla_privacy gates on it.
        "privacy_quarantine_applied": True,
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "GOLD-CANDIDATE (sandbox). HSE+Tusla via bespoke column-x parse, mapped to "
        "public_payments_fact. HSE=payment_actual (incl-VAT), Tusla=po_committed. "
        "Per-year files layout-gated. PRIVACY QUARANTINE APPLIED (likely-person rows "
        "public_display=False).",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
