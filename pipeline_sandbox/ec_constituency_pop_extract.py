"""Electoral Commission constituency-population extractor.

Pulls Appendix 2 — "Statistics Relating to Recommended Dáil Constituencies" —
from the Electoral Commission's Constituency Review Report 2023. This is the
authoritative Census 2022 population for each of the **current 43 Dáil
constituencies** (2023 Electoral Commission boundaries), as used from the
November 2024 general election onward.

Why this exists alongside cso_fy005:
  CSO PxStat FY005 ("Population of each Constituency of Dáil Éireann") is the
  only natively constituency-keyed PxStat table, but it is drawn on the *2017*
  boundaries (39 constituencies). The 34th Dáil sits on the 2023 boundaries
  (43 constituencies), so FY005 leaves the four split/new constituencies
  (Dublin Fingal East/West, Tipperary North/South, Laois, Offaly,
  Wicklow-Wexford) with no clean row. The Commission redrew the boundaries to
  balance Census 2022 population, so its own report carries the 2022 headcount
  on the *current* 43 boundaries — a 43/43 clean join to v_member_registry.

Source : Constituency Review Report 2023, Appendix 2 (PDF, ~30 MB).
         https://www.electoralcommission.ie/publications/constituency-review-reports/
         (mirror used for fetch: rte.ie documents archive)
Writes : data/gold/parquet/ec_constituency_pop_2022.parquet  (--write)

Integrity self-checks (all must pass before --write):
  * exactly 43 constituency rows
  * population_2022 sums to the report national total 5,149,139
  * derived seats (population / population_per_td) sum to 174
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_OUT = _ROOT / "data" / "gold" / "parquet"
_PDF_URL = "https://www.rte.ie/documents/news/2023/08/constituency-review-report-2023.pdf"

# The 43 recommended constituencies, in the exact row order they appear in
# Appendix 2's table. Names are the canonical Oireachtas spellings and match
# v_member_registry.constituency 43/43 with no aliasing required.
_NAMES = [
    "Carlow-Kilkenny", "Cavan-Monaghan", "Clare", "Cork East", "Cork North-Central",
    "Cork North-West", "Cork South-Central", "Cork South-West", "Donegal", "Dublin Bay North",
    "Dublin Bay South", "Dublin Central", "Dublin Fingal East", "Dublin Fingal West",
    "Dublin Mid-West", "Dublin North-West", "Dublin Rathdown", "Dublin South-Central",
    "Dublin South-West", "Dublin West", "Dún Laoghaire", "Galway East", "Galway West", "Kerry",
    "Kildare North", "Kildare South", "Laois", "Limerick City", "Limerick County",
    "Longford-Westmeath", "Louth", "Mayo", "Meath East", "Meath West", "Offaly",
    "Roscommon-Galway", "Sligo-Leitrim", "Tipperary North", "Tipperary South", "Waterford",
    "Wexford", "Wicklow", "Wicklow-Wexford",
]

_NATIONAL_TOTAL = 5_149_139  # report's own Appendix-2 total; used as a checksum
_TOTAL_SEATS = 174
_APPENDIX2_PAGE = 133  # 0-based; "STATISTICS RELATING TO RECOMMENDED DÁIL CONSTITUENCIES"


def fetch_pdf(dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(_PDF_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=120)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


def parse_appendix2(pdf_path: Path) -> pl.DataFrame:
    """Extract the 43-row population table from Appendix 2.

    The page renders three stacked numeric columns (Population 2022, Population
    per TD, % variance). Only the first two are comma-grouped integers; the
    header years ("2022"/"2023") and variance percentages are not, so a
    comma-grouped regex isolates the figures cleanly. The national total row
    (5,149,139) marks the boundary between the two integer columns.
    """
    import fitz  # PyMuPDF — lazy import so the module loads without it

    page_text = fitz.open(pdf_path)[_APPENDIX2_PAGE].get_text()
    nums = [int(n.replace(",", "")) for n in re.findall(r"\d{1,3}(?:,\d{3})+", page_text)]

    try:
        tot_idx = nums.index(_NATIONAL_TOTAL)
    except ValueError as e:
        raise RuntimeError(
            "Appendix-2 layout changed: national total 5,149,139 not found. "
            "Re-check the page index / table format before trusting the parse."
        ) from e

    populations = nums[:tot_idx]
    per_td = nums[tot_idx + 1: tot_idx + 1 + len(_NAMES)]

    if len(populations) != len(_NAMES) or len(per_td) != len(_NAMES):
        raise RuntimeError(
            f"Expected {len(_NAMES)} populations and per-TD values, got "
            f"{len(populations)} / {len(per_td)} — PDF layout drift."
        )

    return pl.DataFrame({
        "constituency_name": _NAMES,
        "population_2022": populations,
        "population_per_td_2022": per_td,
        "td_seats_2024": [round(p / t) for p, t in zip(populations, per_td)],
        "boundaries_label": ["Census 2022 (2023 boundaries)"] * len(_NAMES),
        "source_key": ["Electoral Commission Constituency Review 2023, App. 2"] * len(_NAMES),
    })


def integrity_check(df: pl.DataFrame) -> dict:
    pop_sum = int(df["population_2022"].sum())
    seat_sum = int(df["td_seats_2024"].sum())
    checks = {
        "row_count_43": len(df) == 43,
        "population_sums_to_national_total": pop_sum == _NATIONAL_TOTAL,
        "seats_sum_to_174": seat_sum == _TOTAL_SEATS,
        "no_null_population": df["population_2022"].null_count() == 0,
    }
    return {"checks": checks, "pop_sum": pop_sum, "seat_sum": seat_sum,
            "green": all(checks.values())}


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--pdf", type=Path, default=_ROOT / "data" / "_meta" / "ec_review_2023.pdf",
                    help="Local cache for the report PDF; downloaded if absent.")
    args = ap.parse_args()

    if not args.pdf.exists():
        print(f"Downloading EC report → {args.pdf} ...")
        fetch_pdf(args.pdf)
    print(f"Parsing Appendix 2 from {args.pdf} ...")

    df = parse_appendix2(args.pdf)
    rpt = integrity_check(df)

    print(f"\n=== ec_constituency_pop_2022 — {len(df)} rows ===")
    for name, ok in rpt["checks"].items():
        print(f"  [{'GREEN' if ok else 'FAIL'}] {name}")
    print(f"  population sum: {rpt['pop_sum']:,} (expected {_NATIONAL_TOTAL:,})")
    print(f"  seat sum:       {rpt['seat_sum']} (expected {_TOTAL_SEATS})")
    print(f"  >>> overall: {'GREEN' if rpt['green'] else 'RED'}")

    if args.write and rpt["green"]:
        out = _OUT / "ec_constituency_pop_2022.parquet"
        _write_parquet(df, out)
        print(f"\n  Wrote {out.relative_to(_ROOT)}")
    elif args.write:
        print("\n  REFUSING to write — integrity checks failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
