# --- Write new DataFrames to CSV files ---

import pandas as pd

from config import LEGISLATION_DIR, SILVER_DIR

# flatten the top-level results
bills = []
for page in pd.read_json(LEGISLATION_DIR / "legislation_results.json")["results"]:
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

# --- Original code below (commented out for toggling) ---
# # one row per sponsor-bill — primary join to members data via by.uri
# sponsors_df = pd.json_normalize(bills, record_path=["bill", "sponsors"], meta=BILL_META, errors="ignore")
# # one row per stage-bill — legislative progress timeline
# stages_df = pd.json_normalize(bills, record_path=["bill", "stages"], meta=BILL_META, errors="ignore")
# # one row per debate-bill — debate history per bill
# debates_df = pd.json_normalize(bills, record_path=["bill", "debates"], meta=BILL_META, errors="ignore")

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

sponsors_df = sponsors_df.dropna(axis=0, subset=["sponsor.by.showAs"], how="all").rename(columns=sponsor_rename)
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
debates_df.dropna(axis=0, how="all").to_csv(SILVER_DIR / "debates.csv")

debates_df.to_parquet(SILVER_DIR / "parquet" / "debates.parquet", index=False)
print("Debates dataset created successfully.")

events_df.to_csv(SILVER_DIR / "events.csv", index=False)
print("Events dataset created successfully.")

most_recent_stage_event_dates_df.to_csv(SILVER_DIR / "most_recent_stage_event_dates.csv", index=False)
print("Most recent stage event dates dataset created successfully.")

related_docs_df.to_csv(SILVER_DIR / "related_docs.csv", index=False)
print("Related documents dataset created successfully.")

versions_df.to_csv(SILVER_DIR / "versions.csv", index=False)
print("Versions dataset created successfully.")


