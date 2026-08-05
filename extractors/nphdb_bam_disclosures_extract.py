"""BAM / National Children's Hospital PQ disclosure loader — schedule slippage + cost.

This is a STATIC, hand-verified loader, not a scraper: there is no live source to
poll. The figures below were checked against the raw Oireachtas debate-record XML
this session (2026-08-01) and are typed in verbatim; nothing here is inferred or
computed. Re-run only if the underlying PQ figures are re-verified/extended by a
human — it is deliberately NOT wired into pipeline.py's scheduled chains.

Two disclosures, both BAM's own reporting to the Oireachtas via Dáil written
answers (PQs):

1. SCHEDULE-SLIPPAGE LEDGER (disclosure_type='slippage') — BAM's forecast
   Substantial Completion date at each programme baseline/update, and the delay
   from the original August 2022 contractual date. The table appears verbatim
   (bar the final row's footnote wording) in TWO PQ answers:
     - https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-09-17/writtens/mul@/dbsect_1150.xml
       (Dept. of Health, "Departmental Data", answered 2025-09-17) — used here as
       the canonical source for every row.
     - https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-06-17/writtens/mul@/dbsect_691.xml
       (answered 2025-06-17) — carries an identical table, except the final row's
       delay parenthetical reads "+3 months on previous update" instead of the
       2025-09-17 answer's "+2 months" (a discrepancy in BAM's own reporting
       between the two dates — recorded in that row's `notes`, not resolved here).

2. PROJECT-COST TABLE (disclosure_type='cost') — a 2017-estimate vs 2018-estimate
   cost breakdown from PQ 2598 (Deputy Dessie Ellis), answered 2021-07-27:
   https://data.oireachtas.ie/akn/ie/debateRecord/dail/2021-07-27/writtens/mul@/dbsect_2198.xml
   This is a 2021 answer reporting on 2017/2018 ESTIMATES — NOT current project
   cost. Every row's `source_pq_ref` carries that caveat inline so it survives
   into any UI copy built from this table.

Schema (one row per ledger/cost line item — wide by construction so the page
never needs a pivot/reshape):
    disclosure_type       'slippage' | 'cost'
    sort_order            table order within disclosure_type (int)
    row_label             the baseline/update label, or the cost line item
    forecast_completion   slippage only (BAM's forecast Substantial Completion date)
    delay_from_original   slippage only (delay from the Aug 2022 original date, as
                           published — including its footnote parenthetical)
    cost_2017_eur_m       cost only (2017 estimate, €m)
    cost_2018_eur_m       cost only (2018 estimate, €m)
    source_date           the PQ answer date this row is drawn from
    source_pq_ref         PQ / department / topic citation (+ vintage caveat for cost rows)
    source_url            the exact debateRecord XML URL
    notes                 free-text caveat (source discrepancy, sub-item relationship) or null

Writes: data/gold/parquet/nphdb_bam_disclosures.parquet (20 rows: 15 slippage + 5 cost)

Usage:
    python extractors/nphdb_bam_disclosures_extract.py
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before polars loads. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

from datetime import date
from pathlib import Path

import polars as pl

from services.parquet_io import save_parquet

OUT_GOLD = Path(__file__).resolve().parents[1] / "data" / "gold" / "parquet" / "nphdb_bam_disclosures.parquet"

_SLIPPAGE_SOURCE_DATE = date(2025, 9, 17)
_SLIPPAGE_SOURCE_PQ_REF = 'Dáil PQ (Dept. of Health, "Departmental Data") answered 2025-09-17'
_SLIPPAGE_SOURCE_URL = "https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-09-17/writtens/mul@/dbsect_1150.xml"

_COST_SOURCE_DATE = date(2021, 7, 27)
_COST_SOURCE_PQ_REF = (
    'PQ 2598 (Deputy Dessie Ellis), Dáil, answered 2021-07-27 — "National Children\'s Hospital" '
    "(reports 2017 vs 2018 cost ESTIMATES, not current project cost)"
)
_COST_SOURCE_URL = "https://data.oireachtas.ie/akn/ie/debateRecord/dail/2021-07-27/writtens/mul@/dbsect_2198.xml"

_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "disclosure_type": pl.Utf8,
    "sort_order": pl.Int64,
    "row_label": pl.Utf8,
    "forecast_completion": pl.Utf8,
    "delay_from_original": pl.Utf8,
    "cost_2017_eur_m": pl.Float64,
    "cost_2018_eur_m": pl.Float64,
    "source_date": pl.Date,
    "source_pq_ref": pl.Utf8,
    "source_url": pl.Utf8,
    "notes": pl.Utf8,
}

# The 15-row schedule-slippage ledger, verbatim from the 2025-09-17 PQ answer
# (baseline_update, forecast_completion, delay_from_original). Row 1 is the
# baseline zero-point (original August 2022 contractual date, hence "" delay).
_SLIPPAGE_ROWS: list[tuple[str, str, str]] = [
    ("January 2019 – GMP Programme (ER Compliant)", "August 2022", ""),
    ("March 2020", "February 2023", "6 months"),
    (
        "February 2021 – Amended GMP Programme (ER Compliant)",
        "December 2023",
        "16 months (+10 months on previous update)",
    ),
    ("May 2021", "January 2024", "17 months (+1 month on previous update)"),
    ("January 2022 (Baseline – ER Non-compliant)", "January 2024", "17 months"),
    ("July 2022", "February 2024", "18 months (+1 month on previous update)"),
    ("August 2022", "March 2024", "19 months (+1 month on previous update)"),
    ("January 2023 (Baseline– ER Non-Compliant)", "April 2024", "20 months (+1 month on previous update)"),
    ("February 2023", "May 2024", "21 months (+1 month on previous update)"),
    ("July/September 2023 (Baseline– ER Non-Compliant)", "October 2024", "26 months (+5 months on previous update)"),
    ("January 2024", "December 2024", "28 months (+2 months on previous update)"),
    ("March 2024", "January 2025", "29 months (+1 month on previous update)"),
    ("April 2024", "February 2025", "30 Months (+1 month on previous update)"),
    ("September 2024", "June 2025", "34 Months (+4 months on previous update)"),
    ("May 2025", "September 2025", "37 Months (+2 months on previous update)"),
]

_SLIPPAGE_NOTES: dict[int, str] = {
    1: (
        "This slippage table appears verbatim (bar the final row's footnote) in both the "
        "2025-06-17 (dbsect_691) and 2025-09-17 (dbsect_1150) Dáil written-answer tables."
    ),
    15: (
        "The 2025-06-17 PQ answer's identical final row instead reads "
        '"37 Months (+3 months on previous update)" — a discrepancy in BAM\'s own reporting '
        "between the two PQ dates, not resolved here."
    ),
}

# The 2017-vs-2018 cost table from PQ 2598 (2021-07-27): (row_label, 2017 €m, 2018 €m, notes).
_COST_ROWS: list[tuple[str, float, float, str | None]] = [
    ("Capital Build Sub Total", 983.0, 1433.0, '2018 figure also reported in the source table as "€1.433bn".'),
    (
        "Sub Total (Children's Research & Innovation Centre, ICT, Children's Hospital Integration "
        "Programme, Electronic Healthcare Record, Mater Campus)",
        284.0,
        293.0,
        None,
    ),
    ("Grand Total", 1260.0, 1700.0, 'Also reported in the source table as "€1.26bn" (2017) / "€1.7bn" (2018).'),
    ("Gross Construction Costs", 717.1, 1093.8, "Sub-item within Capital Build Sub Total."),
    ("Main NCH", 637.0, 890.0, "Sub-item within Gross Construction Costs."),
]


def build() -> pl.DataFrame:
    """Assemble the tidy-wide disclosure table from the hand-verified rows above."""
    rows: list[dict] = []
    for i, (row_label, forecast_completion, delay) in enumerate(_SLIPPAGE_ROWS, start=1):
        rows.append(
            {
                "disclosure_type": "slippage",
                "sort_order": i,
                "row_label": row_label,
                "forecast_completion": forecast_completion,
                "delay_from_original": delay,
                "cost_2017_eur_m": None,
                "cost_2018_eur_m": None,
                "source_date": _SLIPPAGE_SOURCE_DATE,
                "source_pq_ref": _SLIPPAGE_SOURCE_PQ_REF,
                "source_url": _SLIPPAGE_SOURCE_URL,
                "notes": _SLIPPAGE_NOTES.get(i),
            }
        )
    for i, (row_label, v2017, v2018, notes) in enumerate(_COST_ROWS, start=1):
        rows.append(
            {
                "disclosure_type": "cost",
                "sort_order": i,
                "row_label": row_label,
                "forecast_completion": None,
                "delay_from_original": None,
                "cost_2017_eur_m": v2017,
                "cost_2018_eur_m": v2018,
                "source_date": _COST_SOURCE_DATE,
                "source_pq_ref": _COST_SOURCE_PQ_REF,
                "source_url": _COST_SOURCE_URL,
                "notes": notes,
            }
        )
    return pl.DataFrame(rows, schema=_SCHEMA)


def main() -> None:
    df = build()
    n_slippage = df.filter(pl.col("disclosure_type") == "slippage").height
    n_cost = df.filter(pl.col("disclosure_type") == "cost").height
    # Hand-authored and fixed-size (20 rows always) — the floor just guards against
    # a future edit accidentally truncating the list.
    save_parquet(df, OUT_GOLD, min_rows=20)
    print(f"wrote {OUT_GOLD.name} ({df.height} rows: {n_slippage} slippage, {n_cost} cost)")


if __name__ == "__main__":
    from services.extract_runner import run_extractor

    run_extractor(main)
