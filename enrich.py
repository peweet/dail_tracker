import polars as pl 
import normalise_join_key
from utility.select_drop_rename_cols_mappings import enrichment_cols_to_select, committees_cols_to_select, members_rename
import logging
from config import SILVER_DIR, GOLD_DIR, GOLD_CSV_DIR, GOLD_PARQUET_DIR


#This module enriches the extracted datasets by joining them together and creating new features that can be used for analysis. It takes the cleaned and normalized datasets from the previous steps (e.g. attendance records, member metadata, committee assignments) and performs joins to create enriched datasets that combine information from multiple sources. The enriched datasets are then saved to CSV files for further analysis. This module also includes logging to track the progress of the enrichment process and any issues that may arise during the joining and feature creation steps. The resulting enriched datasets will provide a more comprehensive view of the TDs' activities and characteristics, allowing for deeper analysis of patterns and correlations across different dimensions of their work in the Dáil.

 

small_df = pl.read_csv(SILVER_DIR / 'aggregated_td_tables.csv')
large_df = pl.read_csv(SILVER_DIR / 'flattened_members.csv')


# committee_df = large_df.select(committees_cols_to_select)
# print(committee_df.schema)
small_df = small_df.with_columns(
    pl.concat_str(
    pl.col(['first_name', 'last_name'])
    ).alias('join_key')
    )
large_df = large_df.with_columns(
    pl.concat_str(
    pl.col(['first_name', 'last_name'])
    ).alias('join_key')
    )
small_df = normalise_join_key.normalise_df_td_name(small_df, 'join_key')
logging.info('normalised small_df (PDF attendance) TD names')
large_df = normalise_join_key.normalise_df_td_name(large_df, 'join_key')
large_df = large_df.unique(subset=['join_key'], keep='first')
logging.info('normalised large_df (API members) TD names')

# https://regex101.com/r/OOLuZU/1
# API master list is the driving table; PDF attendance is left-joined onto it
# large_df = large_df.select(enrichment_cols_to_select)
enriched_df = large_df.join(small_df, on=['join_key'], how='left')
enriched_df = enriched_df.with_columns(pl.col('unique_member_code')
                                       .str.extract(r"\b\d{4}\b", 0)
                                       .alias('year_elected')
                                       )


# --- Create master TD list for gold layer ---
master_cols = [
    "unique_member_code",  # identifier
    "first_name",
    "last_name",
    "full_name",
    "year_elected",
    "ministerial_office",  # position
    "join_key",
    "constituency_name",   # constituency
    "party",
    "constituency_code"
]

master_td_list = large_df.select(master_cols).unique(subset=["unique_member_code"], keep="first")
master_td_list = master_td_list.rename({"unique_member_code": "identifier", "constituency_name": "constituency", "ministerial_office": "position"})

master_td_list.write_csv(GOLD_DIR / "master_td_list.csv")
# master_td_list.write_parquet(GOLD_DIR / "master_td_list.parquet")  # Uncomment to write Parquet

print(f"Master TD list written to {GOLD_DIR / 'master_td_list.csv'} with {master_td_list.height} rows.")

# enriched_df= enriched_df.with_columns(
#     pl.when(pl.col('ministerial_office') != 'Null')
#     .then(pl.lit('true'))
#     .otherwise(pl.lit('false'))
#     .alias('ministerial_office_filled')
#     )


enriched_df.write_csv(GOLD_DIR / 'enriched_td_attendance.csv')
logging.info("Enriched TD attendance CSV created successfully.")

# Gold attendance summary — one row per (member, year) with party and constituency
# already resolved from the join above. sitting_days_count is a year-total
# broadcast on every row; MAX extracts it without summing across duplicates.
(
    enriched_df
    .filter(pl.col("year").is_not_null())
    .group_by(["full_name", "year"])
    .agg(
        pl.col("identifier").first().alias("member_id"),
        pl.col("party").first().alias("party_name"),
        pl.col("constituency_name").first().alias("constituency"),
        pl.col("ministerial_office").first().alias("is_minister"),
        pl.col("sitting_days_count").max().alias("sitting_days"),
        pl.col("other_days_count").max().alias("other_days"),
    )
    .with_columns((pl.col("sitting_days") + pl.col("other_days")).alias("total_days"))
    .sort(["full_name", "year"])
    .write_csv(GOLD_CSV_DIR / "attendance_by_td_year.csv")
)
logging.info("Gold attendance_by_td_year.csv written.")


# committee_df.write_csv(GOLD_DIR / 'committee_assignments.csv')
# logging.info("Committee assignments CSV created successfully.")



votes_df = pl.read_csv(SILVER_DIR / 'pretty_votes.csv')
enrich_vote = pl.read_csv(GOLD_DIR / 'enriched_td_attendance.csv')
key_data = enrich_vote.select(['join_key', 'unique_member_code', 'year_elected', 'last_name', 'dail_term', 'dail_number', 'full_name', 'first_name', 'party', 'constituency_name'])
key_data = key_data.unique(subset=['unique_member_code'])
current_dail_vote_history_df = votes_df.join(key_data, on='unique_member_code', how='left')
current_dail_vote_history_df = current_dail_vote_history_df.unique(subset=['unique_member_code', 'vote_id']).drop('join_key')

current_dail_vote_history_df.write_csv(GOLD_DIR / 'current_dail_vote_history.csv')
logging.info("Enriched TD votes CSV created successfully.")

# TODO: Review this to_parquet step for pipeline compatibility
current_dail_vote_history_df.write_parquet(GOLD_DIR / 'parquet' / 'current_dail_vote_history.parquet')
logging.info("Enriched TD votes Parquet created (check pipeline)")

# #JOIN is too massive
# #ISSUE: the join is creating a cartesian product and blowing up the dataset size, which is likely due to duplicate join keys in one or both datasets. Need to investigate the join keys and ensure 
# # they are unique or handle duplicates appropriately before joining.
# #print("starting to enrich votes with TD metadata and committee assignments...")

# # https://regex101.com/r/OOLuZU/1


# #https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/
# #https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/
# #https://learn.microsoft.com/en-us/power-bi/guidance/star-schema
# #https://docs.getdbt.com/blog/kimball-dimensional-model?version=1.12
# #https://pandas.pydata.org/docs/user_guide/merging.html?utm_source
# # current_dail_vote_history_df = vote_data_df.join(enrich_vote, on=["unique_member_code"], how="left", validate="m:m").drop(
# #   "vote_id", "join_key"
# # ).unique()
# print("starting to enrich votes with sponsor and bill data...")
# current_dail_vote_history_df = current_dail_vote_history_df.join(sponsor_data, on=["unique_member_code"], how="left").unique()
# print("enriched votes with sponsor and bill data successfully.")
# print("writing enriched votes and legislation CSV...")
# current_dail_vote_history_df.write_csv(DATA_DIR_PLACEHOLDER / "gold" / "current_dail_vote_history.csv")
# logging.info("Enriched TD votes and legislationCSV created successfully.")

# ── Current TD payment rankings — 34th Dáil TDs only ──────────────────────────
_pay_src = GOLD_DIR / "top_tds_by_payment_since_2020.csv"
if _pay_src.exists():
    _pay_raw = pl.read_csv(
        _pay_src,
        schema_overrides={"total_amount_paid_since_2020": pl.Float64},
        ignore_errors=True,
    )
    _pay_dedup = (
        _pay_raw
        .filter(pl.col("total_amount_paid_since_2020").is_not_null())
        .sort("total_amount_paid_since_2020", descending=True)
        .unique(subset=["join_key"], keep="first")
        .with_columns(
            pl.col("Full_Name")
              .str.strip_chars()
              .str.replace(r"^([^,]+),\s*(.+)$", "$2 $1")
              .alias("member_name")
        )
    )
    _current_rankings = (
        _pay_dedup
        .join(master_td_list.select(["join_key"]), on="join_key", how="inner")
        .select(["member_name", "join_key", "total_amount_paid_since_2020"])
        .sort("total_amount_paid_since_2020", descending=True)
        .with_row_index(name="rank", offset=1)
    )
    _current_rankings.write_csv(GOLD_CSV_DIR / "current_td_payment_rankings.csv")
    _current_rankings.write_parquet(GOLD_PARQUET_DIR / "current_td_payment_rankings.parquet")
    print(f"Current TD payment rankings written: {len(_current_rankings)} TDs (34th Dáil only)")
else:
    print("WARN: top_tds_by_payment_since_2020.csv not found — skipping current TD payment rankings")


if __name__ == "__main__":
    print("Enriched TD datasets created successfully and saved to enriched_td_attendance.csv.")
# logging.info("normalised large_df (API members) TD names")

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
