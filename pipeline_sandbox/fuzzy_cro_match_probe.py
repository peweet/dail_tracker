"""SANDBOX PROBE (read-only, no writes) — can the payment-fact CRO match (company-class 52.7%)
be improved by fuzzy matching against the CRO register? Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/fuzzy_cro_match_probe.py

FINDINGS (2026-06-13, against gold procurement_payments_fact + silver cro/companies):
  • "44% CRO" is misleading: it's company-class 52.7%; sole_trader/foreign/public_body/id_code
    (47% of rows) legitimately have NO Irish CRO — a correct floor, not a failure.
  • The current join is EXACT name_norm=name_norm, so it cannot match suffix/spacing/abbrev variants
    and cannot collapse fragments (0 CROs map to >1 name).
  • Fuzzy lift over the 11,180 unmatched COMPANY norms:
      - strip legal/geo suffix + exact core ....... +161  (1.6%)  precise
      - de-space + exact core .................... +565  (5.4%)  precise (BEARING POINT→BEARINGPOINT)
      - de-space + containment (len>=8) .......... +4,676 (45%)  HIGH false-positive risk — DO NOT ship blind
  • ROOT CAUSE of the ceiling is NOT the algorithm — it is SUPPLIER-NAME PARSE POLLUTION:
      - 1,561 names >40 chars carry the spend-category/description bled onto the name
        ("AILESBURY CONTRACT CLEANING T A AILESBURY SERVICES MINOR CONTRACTS TRADE SERVICES OTHER WORKS")
      - 1,490 carry "T/A" (registered name ≠ trading name)
      - ~280 carry VAT/date/footnote text
      - 8,517 (76%) are clean-ish names simply NOT in the Irish CRO (foreign/trading/unregistered) = the floor
      - pollution concentrates in LOCAL AUTHORITY parsers: 1,710 of 3,844 unmatched LA names (44%).

CONCLUSION — improvable, but the levers in order of leverage are:
  1. FIX the LA/council parser supplier-name pollution (strip TRAILING category bleed; the consolidate's
     _strip_leading_ref only strips LEADING refs). Cleans ~2,000 names → lifts CRO match AND fragmentation
     AND over-quarantine at once. UPSTREAM extractor fix, not a matching fix.
  2. Handle "T/A": split "X T A Y" and match BOTH the registered (X) and trading (Y) name. ~1,490 names.
  3. Add de-space exact fuzzy CRO match: +5.4% clean. Modest, precise, safe.
  Realistic company-class ceiling after all three: ~52.7% → ~65-70%. NOT ~100% — foreign/trading/unregistered
  names are a legitimate ~76% floor among the currently-unmatched. So: do not rest on laurels, but the win is
  a parser-cleanup, not a clever matcher.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
G = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
CRO = ROOT / "data/silver/cro/companies.parquet"

SUF = r"( (LTD|LIMITED|DAC|PLC|UC|ULC|TEO|TEORANTA|CLG|LLP|LP|CO|COMPANY|GROUP|HOLDINGS|IRELAND|IRL|INTERNATIONAL|INC|LLC|GMBH|TA))+$"


def _core(col: str) -> str:
    return f"trim(regexp_replace({col}, '{SUF}', '', 'g'))"


def _despace(col: str) -> str:
    return f"regexp_replace({_core(col)}, ' ', '', 'g')"


def main() -> None:
    if not G.exists() or not CRO.exists():
        sys.exit("gold payment fact or CRO register missing")
    con = duckdb.connect()
    con.execute(
        f"""CREATE TEMP TABLE unm AS SELECT DISTINCT supplier_normalised sn,
              {_core('supplier_normalised')} core, {_despace('supplier_normalised')} ds, publisher_type pt
            FROM '{G.as_posix()}'
            WHERE supplier_class='company' AND cro_company_num IS NULL AND length(supplier_normalised)>=5"""
    )
    con.execute(
        f"""CREATE TEMP TABLE crc AS SELECT DISTINCT {_despace('name_norm')} ds
            FROM '{CRO.as_posix()}' WHERE length(name_norm)>=5"""
    )
    tot = con.execute("SELECT count(*) FROM unm").fetchone()[0]
    ds_m = con.execute("SELECT count(DISTINCT u.sn) FROM unm u JOIN crc c ON u.ds=c.ds WHERE length(u.ds)>=6").fetchone()[0]
    pol = con.execute(
        "SELECT count(*) FILTER (WHERE length(sn)>40) long_bleed, count(*) FILTER (WHERE sn LIKE '% T A %') trading_as FROM unm"
    ).fetchone()
    print(f"unmatched company norms: {tot:,}")
    print(f"  de-space exact CRO match (precise lift): +{ds_m:,} ({100 * ds_m / tot:.1f}%)")
    print(f"  parse-pollution: {pol[0]:,} category-bled names (>40 chars), {pol[1]:,} trading-as names")
    print("  -> biggest lever is the LA parser cleanup, not a fuzzier matcher (see module docstring).")


if __name__ == "__main__":
    main()
