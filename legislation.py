# --- Write new DataFrames to CSV files ---

# TODO_GOVT_BILLS: This file is the silver flattener for bills. It reads the
# per-TD-scoped bronze JSON, which excludes Government bills entirely (the
# /v1/legislation `member_id` filter only returns bills sponsored by an
# individual TD). To include Government bills:
#   1. Switch the input below to `legislation_results_unscoped.json`.
#   2. Add `["bill", "originHouse", "showAs"]` to BILL_META and
#      `"bill.originHouse.showAs": "origin_house"` to rename_bill_fields.
#   3. Change the dropna at line ~126 to keep rows where either
#      sponsor.by.showAs OR sponsor.as.showAs is populated (Government bills
#      carry sponsor identity in `as.showAs`, not `by.showAs`).
# Full proof + checklist: pipeline_sandbox/legislation_unscoped_integration_plan.md §2a
# Sandbox impl (already verified end-to-end): pipeline_sandbox/legislation_unscoped_silver_views.py

import pandas as pd
from config import LEGISLATION_DIR, SILVER_DIR
import json
import logging
from collections import Counter
from services.dail_config import API_BASE
from services.http_engine import fetch_json

logger = logging.getLogger(__name__)


# flatten the top-level results
bills = []
for page in pd.read_json(LEGISLATION_DIR / "legislation_results_unscoped.json")["results"]:
    bills.extend(page)
# bill metadata fields to carry through to the sponsors, stages, and debates datasets for joining back to members and votes data later on
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
    ["bill", "mostRecentStage", "event", "showAs"],
    ["bill", "mostRecentStage", "event", "progressStage"],
    ["bill", "mostRecentStage", "event", "stageCompleted"],
    ["bill", "mostRecentStage", "event", "house", "showAs"],
    "contextDate",
]
# --- FULL bill mapping normalizations ---
# Debates
debates_df = pd.json_normalize(
    bills,
    record_path=["bill", "debates"],
    meta=BILL_META,
    errors="ignore"
)
# Events
events_df = pd.json_normalize(
    bills,
    record_path=["bill", "events"],
    meta=BILL_META,
    errors="ignore"
)
# Most recent stage event dates
most_recent_stage_event_dates_df = pd.json_normalize(
    bills,
    record_path=["bill", "mostRecentStage", "event", "dates"],
    meta=BILL_META,
    errors="ignore"
)
# Related docs
related_docs_df = pd.json_normalize(
    bills,
    record_path=["bill", "relatedDocs"],
    meta=BILL_META,
    errors="ignore"
)
# Sponsors
sponsors_df = pd.json_normalize(
    bills,
    record_path=["bill", "sponsors"],
    meta=BILL_META,
    errors="ignore"
)
# Stages
stages_df = pd.json_normalize(
    bills,
    record_path=["bill", "stages"],
    meta=BILL_META,
    errors="ignore"
)
# Versions
versions_df = pd.json_normalize(
    bills,
    record_path=["bill", "versions"],
    meta=BILL_META,
    errors="ignore"
)

rename_bill_fields = {
    "billSort": "bill_sort",
    "billYearSort": "bill_year_sort",
    "bill.billNo": "bill_no",
    "bill.billYear": "bill_year",
    "bill.billType": "bill_type",
    "bill.shortTitleEn": "short_title_en",
    "bill.longTitleEn": "long_title_en",
    "bill.lastUpdated": "last_updated",
    "bill.status": "status",
    "sponsor.by.showAs": "sponsor_by_show_as",
    "sponsor.by.uri": "sponsor_by_uri",
    "sponsor.isPrimary": "sponsor_is_primary",
    "bill.source": "source",
    "bill.method": "method",
    "bill.mostRecentStage.event.showAs": "most_recent_stage_event_show_as",
    "bill.mostRecentStage.event.progressStage": "most_recent_stage_event_progress_stage",
    "bill.mostRecentStage.event.stageCompleted": "most_recent_stage_event_stage_completed",
    "bill.mostRecentStage.event.house.showAs": "most_recent_stage_event_house_show_as",
}

sponsor_rename = {
    "sponsor.as.showAs": "sponsor_as_show_as",
    "sponsor.as.uri": "sponsor_as_uri",
    "sponsor.by.showAs": "sponsor_by_show_as",
    "sponsor.by.uri": "sponsor_by_uri",
    "sponsor.isPrimary": "sponsor_is_primary",
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
    "bill.mostRecentStage.event.showAs": "most_recent_stage_event_show_as",
    "bill.mostRecentStage.event.progressStage": "most_recent_stage_event_progress_stage",
    "bill.mostRecentStage.event.stageCompleted": "most_recent_stage_event_stage_completed",
    "bill.mostRecentStage.event.house.showAs": "most_recent_stage_event_house_show_as",
    "contextDate": "context_date"
}


PAGE_SIZE = 1000  # API server cap — same as votes
DATE_START = "2014-01-01"  # matches build_legislation_urls
DATE_END = "2099-01-01"
OUTPUT_PATH = LEGISLATION_DIR / "legislation_results_unscoped.json"

def build_url(skip: int) -> str:
    return (
        f"{API_BASE}/legislation"
        f"?date_start={DATE_START}"
        f"&date_end={DATE_END}"
        f"&limit={PAGE_SIZE}"
        f"&skip={skip}"
        f"&chamber_id="
        f"&lang=en"
    )

def fetch_all_bills() -> tuple[list[dict], int, int]:
    """Sequential skip/limit pagination. Returns (bills, expected_count, bytes)."""
    all_bills: list[dict] = []
    total_bytes = 0
    expected: int | None = None
    skip = 0

    while True:
        page, raw_bytes = fetch_json(build_url(skip))
        total_bytes += raw_bytes

        if expected is None:
            expected = page["head"]["counts"]["resultCount"]
            logger.info(f"Bill pagination | expected={expected} | page_size={PAGE_SIZE}")

        page_results = page.get("results", [])
        all_bills.extend(page_results)
        logger.info(
            f"Bill page | skip={skip} | got={len(page_results)} | "
            f"running_total={len(all_bills)} | bytes={raw_bytes:,}"
        )

        if len(page_results) < PAGE_SIZE or len(all_bills) >= expected:
            break
        skip += PAGE_SIZE

    assert len(all_bills) >= expected, (
        f"Bill pagination drift: got {len(all_bills)} of {expected} expected"
    )
    return all_bills, expected, total_bytes


sponsors_df = sponsors_df.dropna(subset=["sponsor.by.showAs", "sponsor.as.showAs"], how="all").rename(columns=sponsor_rename)
# print(sponsors_df.columns)
sponsors_df = sponsors_df.dropna(axis=1, how="all")
sponsors_df['sponsor_by_uri'] = sponsors_df['sponsor_by_uri'].str.split('/', n=7).str[-1]
sponsors_df.rename(columns={"sponsor_by_uri": "unique_member_code"}, inplace=True)

sponsors_df = sponsors_df.replace(r"[\r\n]+", " ", regex=True).replace(r"\s{2,}", " ", regex=True)
sponsors_df = sponsors_df.rename(columns=rename_bill_fields)
stages_df = stages_df.rename(columns=rename_bill_fields)
# Bill URL enrichment
#
# Join vote history with stages.csv to attach a bill_no and Oireachtas URL to
# each vote row where the debate maps to a known bill.
#
# URL format: https://www.oireachtas.ie/en/bills/bill/{bill_year}/{bill_no}/
# Example:    Bill 75 of 2025  = https://www.oireachtas.ie/en/bills/bill/2025/75/
sponsors_df["bill_url"] = sponsors_df.apply(
    lambda row: f"https://www.oireachtas.ie/en/bills/bill/{row['bill_year']}/{row['bill_no']}", axis=1
)

sponsors_df.to_csv(SILVER_DIR / "sponsors.csv")
sponsors_df.to_parquet(SILVER_DIR / "parquet" / "sponsors.parquet", index=False)
print("Sponsors dataset created successfully.")

stages_df.dropna(axis=0, how="all").to_csv(SILVER_DIR / "stages.csv")
stages_df.to_parquet(SILVER_DIR / "parquet" / "stages.parquet", index=False)
print("Stages dataset created successfully.")

debates_df = debates_df.sort_values(by="date", axis=0, ascending=True)
debates_df["debate_url_web"] = (
    "https://www.oireachtas.ie/en/debates/debate/"
    + debates_df["chamber.uri"].str.split("/").str[-1]
    + "/"
    + debates_df["date"].astype(str)
    + "/"
    + debates_df["debateSectionId"].str.replace("dbsect_", "", regex=False)
    + "/"
)
debates_df.dropna(axis=0, how="all").to_csv(SILVER_DIR / "debates.csv")

debates_df.to_parquet(SILVER_DIR / "parquet" / "debates.parquet", index=False)
print("Debates dataset created successfully.")

events_df.to_csv(SILVER_DIR / "events.csv", index=False)
print("Events dataset created successfully.")

# TODO: Review this to_parquet step for pipeline compatibility
events_df.to_parquet(SILVER_DIR / "parquet" / "events.parquet", index=False)
print("Events Parquet dataset created (check pipeline)")

most_recent_stage_event_dates_df.to_csv(SILVER_DIR / "most_recent_stage_event_dates.csv", index=False)
print("Most recent stage event dates dataset created successfully.")

# TODO: Review this to_parquet step for pipeline compatibility
most_recent_stage_event_dates_df.to_parquet(SILVER_DIR / "parquet" / "most_recent_stage_event_dates.parquet", index=False)
print("Most recent stage event dates Parquet dataset created (check pipeline)")

related_docs_df.to_csv(SILVER_DIR / "related_docs.csv", index=False)
print("Related documents dataset created successfully.")

# TODO: Review this to_parquet step for pipeline compatibility
related_docs_df.to_parquet(SILVER_DIR / "parquet" / "related_docs.parquet", index=False)
print("Related documents Parquet dataset created (check pipeline)")


versions_df.to_csv(SILVER_DIR / "versions.csv", index=False)
print("Versions dataset created successfully.")

# TODO: Review this to_parquet step for pipeline compatibility
versions_df.to_parquet(SILVER_DIR / "parquet" / "versions.parquet", index=False)
print("Versions Parquet dataset created (check pipeline)")


