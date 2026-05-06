"""End-to-end sandbox: bronze unscoped JSON -> silver parquet -> patched views.

Implements the section-2a checklist from `legislation_unscoped_integration_plan.md`
in a parallel sandbox path so we can verify Government bills flow all the way
through to the index/detail views before graduating any of it to production.

Read-only against production:
- Reads:   data/bronze/legislation/legislation_results_unscoped.json
- Writes:  pipeline_sandbox/out/silver/sponsors.parquet
           pipeline_sandbox/out/silver/stages.parquet
           pipeline_sandbox/out/silver/debates.parquet
- Touches: nothing in legislation.py / silver/ / sql_views/ — production
           silver and views are untouched.

Section-2a items implemented here:
  1. BILL_META extended with `originHouse`.
  2. legislation.py:126 sponsor coalesce — keep rows where either
     sponsor.by.showAs or sponsor.as.showAs is populated.
  3. legislation_index.sql / legislation_detail.sql — sponsor coalesce in
     WHERE and in the `sponsor` derivation.
  4. v_legislation_index — `source` and `origin_house` added to projection.
  5. bill_phase — origin-aware (Seanad-origin bills land in 'seanad' phase
     even at low stage numbers).

Section-2a items NOT implemented (out of scope for sandbox proof):
  6, 7 — page copy + UI segmented control. Verified via SQL only.

Run
---
    python -m pipeline_sandbox.legislation_unscoped_silver_views
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

from config import LEGISLATION_DIR

sys.stdout.reconfigure(encoding="utf-8")

UNSCOPED_JSON = LEGISLATION_DIR / "legislation_results_unscoped.json"
SANDBOX_SILVER = Path(__file__).resolve().parent / "out" / "silver"
SANDBOX_SILVER.mkdir(parents=True, exist_ok=True)

# Mirrors legislation.py:12-29 + originHouse (item 1)
BILL_META = [
    ["billSort", "billShortTitleEnSort"],
    ["billSort", "billYearSort"],
    ["bill", "billNo"],
    ["bill", "billYear"],
    ["bill", "billType"],
    ["bill", "shortTitleEn"],
    ["bill", "longTitleEn"],
    ["bill", "lastUpdated"],
    ["bill", "status"],
    ["bill", "source"],
    ["bill", "method"],
    ["bill", "originHouse", "showAs"],  # NEW — needed by origin-aware bill_phase
    ["bill", "mostRecentStage", "event", "showAs"],
    ["bill", "mostRecentStage", "event", "progressStage"],
    ["bill", "mostRecentStage", "event", "stageCompleted"],
    ["bill", "mostRecentStage", "event", "house", "showAs"],
    "contextDate",
]

RENAME_BILL_FIELDS = {
    "billSort.billShortTitleEnSort": "bill_sort_short_title_en_sort",
    "billSort.billYearSort": "bill_sort_year_sort",
    "bill.billNo": "bill_no",
    "bill.billYear": "bill_year",
    "bill.billType": "bill_type",
    "bill.shortTitleEn": "short_title_en",
    "bill.longTitleEn": "long_title_en",
    "bill.lastUpdated": "last_updated",
    "bill.status": "status",
    "bill.source": "source",
    "bill.method": "method",
    "bill.originHouse.showAs": "origin_house",  # NEW
    "bill.mostRecentStage.event.showAs": "most_recent_stage_event_show_as",
    "bill.mostRecentStage.event.progressStage": "most_recent_stage_event_progress_stage",
    "bill.mostRecentStage.event.stageCompleted": "most_recent_stage_event_stage_completed",
    "bill.mostRecentStage.event.house.showAs": "most_recent_stage_event_house_show_as",
    "sponsor.as.showAs": "sponsor_as_show_as",
    "sponsor.as.uri": "sponsor_as_uri",
    "sponsor.by.showAs": "sponsor_by_show_as",
    "sponsor.by.uri": "sponsor_by_uri",
    "sponsor.isPrimary": "sponsor_is_primary",
    "contextDate": "context_date",
}


def build_silver() -> None:
    """Mirror legislation.py for sponsors/stages/debates with item-1 + item-2 fixes."""
    raw = pd.read_json(UNSCOPED_JSON)
    bills = []
    for page in raw["results"]:
        bills.extend(page)

    # Sponsors
    sponsors_df = pd.json_normalize(
        bills, record_path=["bill", "sponsors"], meta=BILL_META, errors="ignore"
    )

    # ITEM 2: coalesce — keep rows where either by.showAs or as.showAs is populated
    sponsors_df = sponsors_df.dropna(
        axis=0,
        subset=["sponsor.by.showAs", "sponsor.as.showAs"],
        how="all",
    )
    sponsors_df = sponsors_df.dropna(axis=1, how="all").rename(columns=RENAME_BILL_FIELDS)

    # Mirror legislation.py:129-130: extract member code tail from URI when present
    if "sponsor_by_uri" in sponsors_df.columns:
        sponsors_df["sponsor_by_uri"] = sponsors_df["sponsor_by_uri"].astype(str).str.split("/", n=7).str[-1]
        sponsors_df = sponsors_df.rename(columns={"sponsor_by_uri": "unique_member_code"})

    sponsors_df = sponsors_df.replace(r"[\r\n]+", " ", regex=True).replace(r"\s{2,}", " ", regex=True)
    sponsors_df["bill_url"] = sponsors_df.apply(
        lambda r: f"https://www.oireachtas.ie/en/bills/bill/{r['bill_year']}/{r['bill_no']}", axis=1
    )

    # Stages
    stages_df = pd.json_normalize(
        bills, record_path=["bill", "stages"], meta=BILL_META, errors="ignore"
    ).rename(columns=RENAME_BILL_FIELDS)

    # Debates — keep dotted bill.billNo / bill.billYear column names (legislation_debates.sql expects them)
    debates_df = pd.json_normalize(
        bills, record_path=["bill", "debates"], meta=BILL_META, errors="ignore"
    )

    sponsors_df.to_parquet(SANDBOX_SILVER / "sponsors.parquet", index=False)
    stages_df.to_parquet(SANDBOX_SILVER / "stages.parquet", index=False)
    debates_df.to_parquet(SANDBOX_SILVER / "debates.parquet", index=False)
    print(
        f"Silver written | sponsors={len(sponsors_df)} stages={len(stages_df)} "
        f"debates={len(debates_df)} -> {SANDBOX_SILVER}"
    )


def build_views(con: duckdb.DuckDBPyConnection) -> None:
    """Patched views — items 3, 4, 5 inline."""
    silver = SANDBOX_SILVER.as_posix()

    # v_legislation_index — items 3, 4, 5 applied
    con.execute(f"""
        CREATE OR REPLACE VIEW v_legislation_index AS
        WITH ranked AS (
            SELECT
                bill_year || '_' || bill_no                               AS bill_id,
                COALESCE(short_title_en, '(Untitled)')                    AS bill_title,
                COALESCE(status, '—')                                     AS bill_status,
                COALESCE(bill_type, source, '—')                          AS bill_type,
                source,                                                            -- item 4
                origin_house,                                                      -- item 4
                COALESCE(sponsor_by_show_as, sponsor_as_show_as, '—')     AS sponsor,    -- item 3
                TRY_CAST(context_date AS DATE)                            AS introduced_date,
                most_recent_stage_event_show_as                           AS current_stage,
                TRY_CAST(most_recent_stage_event_progress_stage AS INTEGER) AS stage_number,
                bill_url                                                  AS oireachtas_url,
                bill_no,
                bill_year,
                ROW_NUMBER() OVER (
                    PARTITION BY bill_year, bill_no
                    ORDER BY CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END
                ) AS rn
            FROM read_parquet('{silver}/sponsors.parquet')
            WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL  -- item 3
        )
        SELECT
            bill_id, bill_title, bill_status, bill_type,
            source, origin_house,                                                  -- item 4
            sponsor, introduced_date, current_stage, stage_number,
            oireachtas_url, bill_no, bill_year,
            -- item 5: origin-aware bill_phase
            CASE
                WHEN LOWER(bill_status) LIKE '%enact%'
                  OR LOWER(bill_status) LIKE '%sign%'
                  OR COALESCE(stage_number, 0) >= 11
                THEN 'enacted'
                WHEN origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) < 6
                THEN 'seanad'
                WHEN origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) >= 6
                THEN 'dail'
                WHEN COALESCE(stage_number, 0) >= 6 THEN 'seanad'
                ELSE 'dail'
            END AS bill_phase
        FROM ranked
        WHERE rn = 1
        ORDER BY introduced_date DESC NULLS LAST;
    """)

    # v_legislation_detail — item 3
    con.execute(f"""
        CREATE OR REPLACE VIEW v_legislation_detail AS
        WITH ranked AS (
            SELECT
                bill_year || '_' || bill_no                               AS bill_id,
                COALESCE(short_title_en, '(Untitled)')                    AS bill_title,
                COALESCE(long_title_en, '')                               AS long_title,
                COALESCE(status, '—')                                     AS bill_status,
                COALESCE(bill_type, source, '—')                          AS bill_type,
                COALESCE(sponsor_by_show_as, sponsor_as_show_as, '—')     AS sponsor,
                unique_member_code,
                TRY_CAST(context_date AS DATE)                            AS introduced_date,
                last_updated,
                source,
                origin_house,
                method,
                most_recent_stage_event_show_as                           AS current_stage,
                most_recent_stage_event_house_show_as                     AS current_house,
                most_recent_stage_event_stage_completed                   AS stage_completed,
                bill_url                                                  AS oireachtas_url,
                bill_no,
                bill_year,
                ROW_NUMBER() OVER (
                    PARTITION BY bill_year, bill_no
                    ORDER BY CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END
                ) AS rn
            FROM read_parquet('{silver}/sponsors.parquet')
            WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL
        )
        SELECT * EXCLUDE rn FROM ranked WHERE rn = 1;
    """)

    # v_legislation_timeline / v_legislation_debates — unchanged from production
    con.execute(f"""
        CREATE OR REPLACE VIEW v_legislation_timeline AS
        SELECT
            bill_year || '_' || bill_no                  AS bill_id,
            COALESCE("event.showAs", '—')                AS stage_name,
            TRY_CAST("event.dates"[1].date AS DATE)      AS stage_date,
            TRY_CAST("event.progressStage" AS INTEGER)   AS stage_number,
            COALESCE("event.stageCompleted", false)      AS is_current_stage,
            "event.house.showAs"                         AS chamber,
            bill_no, bill_year
        FROM read_parquet('{silver}/stages.parquet');
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW v_legislation_debates AS
        SELECT
            "bill.billYear" || '_' || "bill.billNo"  AS bill_id,
            TRY_CAST(date AS DATE)                   AS debate_date,
            showAs                                   AS debate_title,
            "chamber.showAs"                         AS chamber,
            "bill.billNo"                            AS bill_no,
            "bill.billYear"                          AS bill_year
        FROM read_parquet('{silver}/debates.parquet')
        WHERE "bill.billNo" IS NOT NULL AND "bill.billYear" IS NOT NULL;
    """)


def smoke_tests(con: duckdb.DuckDBPyConnection) -> None:
    print("\n--- Smoke test 1: source breakdown in v_legislation_index ---")
    print(con.execute(
        "SELECT source, COUNT(*) AS bills FROM v_legislation_index GROUP BY source ORDER BY bills DESC"
    ).df())

    print("\n--- Smoke test 2: bill_phase × source crosstab ---")
    print(con.execute("""
        SELECT bill_phase, source, COUNT(*) AS bills
        FROM v_legislation_index GROUP BY 1, 2 ORDER BY 1, 2
    """).df())

    print("\n--- Smoke test 3: index ↔ detail consistency ---")
    drift = con.execute("""
        SELECT COUNT(*) AS missing_from_detail
        FROM (
            SELECT bill_id FROM v_legislation_index
            EXCEPT SELECT bill_id FROM v_legislation_detail
        )
    """).df()
    print(drift)
    print("expect missing_from_detail = 0")

    print("\n--- Smoke test 4: every debate's bill_id exists in index ---")
    orphans = con.execute("""
        SELECT COUNT(DISTINCT bill_id) AS debate_bills_not_in_index
        FROM (
            SELECT bill_id FROM v_legislation_debates
            EXCEPT SELECT bill_id FROM v_legislation_index
        )
    """).df()
    print(orphans)
    print("(non-zero is acceptable if production debate rows include bills outside the date window)")

    print("\n--- Smoke test 5: Seanad-origin bills land in 'seanad' phase even at low stages ---")
    print(con.execute("""
        SELECT bill_id, bill_title, source, origin_house, stage_number, bill_phase
        FROM v_legislation_index
        WHERE origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) < 6
        ORDER BY introduced_date DESC NULLS LAST
        LIMIT 10
    """).df())

    print("\n--- Smoke test 6: Government bills with debate sessions ---")
    print(con.execute("""
        SELECT i.bill_id, i.bill_title, i.sponsor, COUNT(d.bill_id) AS debate_count
        FROM v_legislation_index i
        LEFT JOIN v_legislation_debates d USING (bill_id)
        WHERE i.source = 'Government'
        GROUP BY i.bill_id, i.bill_title, i.sponsor
        HAVING debate_count > 0
        ORDER BY debate_count DESC
        LIMIT 10
    """).df())

    print("\n--- Smoke test 7: end-to-end click-through for Climate Action and Low Carbon Bill 2021 ---")
    print(con.execute("""
        SELECT bill_id, bill_title, source, origin_house, sponsor, current_stage, bill_phase
        FROM v_legislation_index
        WHERE bill_title ILIKE '%Climate Action and Low Carbon%'
    """).df())
    print(con.execute("""
        SELECT bill_id, bill_title, source, sponsor, current_stage
        FROM v_legislation_detail
        WHERE bill_title ILIKE '%Climate Action and Low Carbon%'
    """).df())


def main() -> None:
    if not UNSCOPED_JSON.exists():
        raise SystemExit(
            f"Missing {UNSCOPED_JSON} — run "
            "`python -m pipeline_sandbox.legislation_unscoped_fetch` first."
        )
    build_silver()
    con = duckdb.connect()
    build_views(con)
    smoke_tests(con)


if __name__ == "__main__":
    main()
