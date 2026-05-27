import logging

import polars as pl

import normalise_join_key
from config import GOLD_CSV_DIR, GOLD_DIR, GOLD_PARQUET_DIR, SILVER_DIR

# This module enriches the extracted datasets by joining them together and creating new features that can be used for analysis. It takes the cleaned and normalized datasets from the previous steps (e.g. attendance records, member metadata, committee assignments) and performs joins to create enriched datasets that combine information from multiple sources. The enriched datasets are then saved to CSV files for further analysis. This module also includes logging to track the progress of the enrichment process and any issues that may arise during the joining and feature creation steps. The resulting enriched datasets will provide a more comprehensive view of the TDs' activities and characteristics, allowing for deeper analysis of patterns and correlations across different dimensions of their work in the Dáil.
# Read the FACT table, not the raw aggregated table — attendance.py builds
# sitting_days_count / other_days_count there (and drops the raw date-string
# columns). The aggregated_td_tables CSV is still consumed directly by SQL
# views that need the per-row iso_sitting_days_attendance dates.
member_profiles_df = pl.read_csv(SILVER_DIR / "td_attendance_fact_table.csv")
members_wide_df = pl.read_csv(SILVER_DIR / "flattened_members.csv")

member_profiles_df = member_profiles_df.with_columns(
    pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")
)
members_wide_df = members_wide_df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
member_profiles_df = normalise_join_key.normalise_df_td_name(member_profiles_df, "join_key")
logging.info("normalised member_profiles_df  (PDF attendance) TD names")
members_wide_df = normalise_join_key.normalise_df_td_name(members_wide_df, "join_key")
members_wide_df = members_wide_df.unique(subset=["join_key"], keep="first")
logging.info("normalised members_wide_df  (API members) TD names")

# https://regex101.com/r/OOLuZU/1
# API master list is the driving table; PDF attendance is left-joined onto it
# members_wide_df  = members_wide_df .select(enrichment_cols_to_select)
enriched_df = members_wide_df.join(member_profiles_df, on=["join_key"], how="left")
enriched_df = enriched_df.with_columns(pl.col("unique_member_code").str.extract(r"\b\d{4}\b", 0).alias("year_elected"))


# --- Create master TD list for gold layer ---
master_cols = [
    "unique_member_code",  # identifier
    "first_name",
    "last_name",
    "full_name",
    "year_elected",
    "ministerial_office",  # position
    "join_key",
    "constituency_name",  # constituency
    "party",
    "constituency_code",
]

master_td_list = (
    members_wide_df.with_columns(pl.col("dail_number").cast(pl.Int32, strict=False))
    .sort("dail_number", descending=True, nulls_last=True)
    .select(master_cols)
    .unique(subset=["unique_member_code"], keep="first")
)
master_td_list = master_td_list.rename(
    {"unique_member_code": "identifier", "constituency_name": "constituency", "ministerial_office": "position"}
)

master_td_list.write_csv(GOLD_DIR / "master_td_list.csv")
# master_td_list.write_parquet(GOLD_DIR / "master_td_list.parquet")  # Uncomment to write Parquet

print(f"Master TD list written to {GOLD_DIR / 'master_td_list.csv'} with {master_td_list.height} rows.")

enriched_df.write_csv(GOLD_DIR / "enriched_td_attendance.csv")
logging.info("Enriched TD attendance CSV created successfully.")

# Gold attendance summary — one row per (member, year) with party and constituency
# already resolved from the join above. sitting_days_count is a year-total
# broadcast on every row; MAX extracts it without summing across duplicates.
# unique_member_code is the canonical Oireachtas code (vs member_id which is the
# silver internal LastName_FirstName format) — required by attendance SQL views.
attendance_year = (
    enriched_df.filter(pl.col("year").is_not_null())
    .group_by(["full_name", "year"])
    .agg(
        pl.col("unique_member_code").first().alias("unique_member_code"),
        pl.col("identifier").first().alias("member_id"),
        pl.col("party").first().alias("party_name"),
        pl.col("constituency_name").first().alias("constituency"),
        pl.col("ministerial_office").first().alias("is_minister"),
        pl.col("sitting_days_count").max().alias("sitting_days"),
        pl.col("other_days_count").max().alias("other_days"),
    )
    .with_columns(
        [
            (pl.col("sitting_days") + pl.col("other_days")).alias("total_days"),
            pl.col("unique_member_code").fill_null(""),
        ]
    )
    .sort(["full_name", "year"])
)
attendance_year.write_csv(GOLD_CSV_DIR / "attendance_by_td_year.csv")
attendance_year.write_parquet(
    GOLD_PARQUET_DIR / "attendance_by_td_year.parquet",
    compression="zstd",
    compression_level=3,
    statistics=True,
)
logging.info("Gold attendance_by_td_year.csv + parquet written.")


votes_df = pl.read_csv(SILVER_DIR / "pretty_votes.csv")
enrich_vote = pl.read_csv(GOLD_DIR / "enriched_td_attendance.csv")
key_data = enrich_vote.select(
    [
        "join_key",
        "unique_member_code",
        "year_elected",
        "last_name",
        "dail_term",
        "dail_number",
        "full_name",
        "first_name",
        "party",
        "constituency_name",
    ]
)
key_data = key_data.unique(subset=["unique_member_code"])
current_dail_vote_history_df = votes_df.join(key_data, on="unique_member_code", how="left")
current_dail_vote_history_df = current_dail_vote_history_df.unique(
    subset=["unique_member_code", "date", "vote_id"]
).drop("join_key")

current_dail_vote_history_df.write_csv(GOLD_DIR / "current_dail_vote_history.csv")
logging.info("Enriched TD votes CSV created successfully.")

# TODO: Review this to_parquet step for pipeline compatibility
current_dail_vote_history_df.write_parquet(
    GOLD_DIR / "parquet" / "current_dail_vote_history.parquet",
    compression="zstd",
    compression_level=3,
    statistics=True,
)
logging.info("Enriched TD votes Parquet created (check pipeline)")

# #https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/
# #https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/
# #https://learn.microsoft.com/en-us/power-bi/guidance/star-schema
# #https://docs.getdbt.com/blog/kimball-dimensional-model?version=1.12
# #https://pandas.pydata.org/docs/user_guide/merging.html?utm_source

_pay_psa = GOLD_PARQUET_DIR / "payments_full_psa.parquet"
if _pay_psa.exists():
    pay_raw = pl.read_parquet(_pay_psa)
    pay_keyed = normalise_join_key.normalise_df_td_name(
        pay_raw.filter(pl.col("date_paid") >= pl.lit("2020-01-01").str.to_date()).filter(
            pl.col("amount").is_not_null()
        ),
        "member_name",
    )
    pay_agg = pay_keyed.group_by("join_key").agg(pl.col("amount").sum().alias("total_amount_paid_since_2020"))
    member_lookup = master_td_list.select(["join_key", "identifier", "party", "constituency"]).unique(
        subset=["join_key"], keep="first"
    )
    current_rankings = (
        pay_agg.join(member_lookup, on="join_key", how="inner")
        .select(["join_key", "identifier", "party", "constituency", "total_amount_paid_since_2020"])
        .sort("total_amount_paid_since_2020", descending=True)
        .with_row_index(name="rank", offset=1)
    )
    current_rankings.write_csv(GOLD_CSV_DIR / "current_td_payment_rankings.csv")
    current_rankings.write_parquet(
        GOLD_PARQUET_DIR / "current_td_payment_rankings.parquet",
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    print(f"Current TD payment rankings written: {len(current_rankings)} TDs")
else:
    print("WARN: payments_full_psa.parquet not found — skipping current TD payment rankings")

# Lobbying gold no longer needs post-hoc enrichment here — unique_member_code is
# now joined upstream in sql_queries/most_lobbied_politicians.sql.


if __name__ == "__main__":
    print("Enriched TD datasets created successfully and saved to enriched_td_attendance.csv.")
# logging.info("normalised members_wide_df  (API members) TD names")

# enriched_df.write_csv(DATA_DIR_PLACEHOLDER / "gold" / "enriched_td_attendance.csv")
# logging.info("Enriched TD attendance CSV created successfully.")

# # committee_df.write_csv(DATA_DIR_PLACEHOLDER / "gold" / "committee_assignments.csv")
# # logging.info("Committee assignments CSV created successfully.")

# votes_df = pl.read_csv(DATA_DIR_PLACEHOLDER / "silver" / "pretty_votes.csv", encoding="utf-8")

# enrich_vote = pl.read_csv(DATA_DIR_PLACEHOLDER / "gold" / "enriched_td_attendance.csv")
# key_data = enrich_vote.select(
#     [
#         "join_key",
#         "unique_member_code",
#         # "year_elected",
#         "last_name",
#         "dail_term",
#         "dail_number",
#         "full_name",
#         "first_name",
#         "party",
#         "constituency_name",
#     ]
# )
# vote_data_df = votes_df.select(
#     "unique_member_code", "debate_title", "vote_id", "date", "vote_outcome", "vote_url"
# ).rename({"date": "vote_date"})
# sponsor_data = pl.read_csv(DATA_DIR_PLACEHOLDER / "silver" / "sponsors.csv", encoding="utf-8").select(
#     [
#         "unique_member_code",
#         "sponsor_is_primary",
#         "bill_no",
#         # "bill_year",
#         # "bill_type",
#         # "short_title_en",
#         # "long_title_en",
#         # "last_updated",
#         # "status",
#         # "source",
#         # "method",
#         # "most_recent_stage_event_show_as",
#         # "most_recent_stage_event_progress_stage",
#         # "most_recent_stage_event_stage_completed",
#         # "most_recent_stage_event_house_show_as",
#         # "context_date",
#         "bill_url",
#     ]
# ).unique()
# # .drop_nulls(subset=["unique_member_code"])

# # print(vote_data_df.estimated_size(unit="gb"))
