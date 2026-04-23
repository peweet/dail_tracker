import logging

import polars as pl

import normalise_join_key
from config import DATA_DIR
from utility.select_drop_rename_cols_mappings import (
    committees_cols_to_select,
    enrichment_cols_to_select,
)

# This module enriches the extracted datasets by joining them together
# and creating new features that can be used for analysis.
# It takes the cleaned and normalized datasets from the previous steps (e.g. attendance records, member metadata, committee assignments) and performs joins to create enriched datasets that combine information from multiple sources. The enriched datasets are then saved to CSV files for further analysis. This module also includes logging to track the progress of the enrichment process and any issues that may arise during the joining and feature creation steps. The resulting enriched datasets will provide a more comprehensive view of the TDs' activities and characteristics, allowing for deeper analysis of patterns and
# correlations across different dimensions of their work in the Dáil.
small_df = pl.read_csv(DATA_DIR / "silver" / "aggregated_td_tables.csv")
large_df = pl.read_csv(DATA_DIR / "silver" / "flattened_members.csv")

committee_df = large_df.select(committees_cols_to_select)
# print(committee_df.schema)
small_df = small_df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))

small_df = normalise_join_key.normalise_df_td_name(small_df, "join_key")
logging.info("normalised small_df (PDF attendance) TD names")

large_df = large_df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
large_df = normalise_join_key.normalise_df_td_name(large_df, "join_key")
logging.info("normalised large_df (API members) TD names")

large_df = large_df.unique(subset=["join_key"], keep="first")


large_df = large_df.select(enrichment_cols_to_select)
enriched_df = large_df.join(small_df, on=["join_key"], how="left")














logging.info("normalised large_df (API members) TD names")

# https://regex101.com/r/OOLuZU/1


enriched_df.write_csv(DATA_DIR / "gold" / "enriched_td_attendance.csv")
logging.info("Enriched TD attendance CSV created successfully.")

# committee_df.write_csv(DATA_DIR / "gold" / "committee_assignments.csv")
# logging.info("Committee assignments CSV created successfully.")

votes_df = pl.read_csv(DATA_DIR / "silver" / "pretty_votes.csv", encoding="utf-8")

enrich_vote = pl.read_csv(DATA_DIR / "gold" / "enriched_td_attendance.csv")
key_data = enrich_vote.select(
    [
        "join_key",
        "unique_member_code",
        # "year_elected",
        "last_name",
        "dail_term",
        "dail_number",
        "full_name",
        "first_name",
        "party",
        "constituency_name",
    ]
)
vote_data_df = votes_df.select(
    "unique_member_code", "debate_title", "vote_id", "date", "vote_outcome", "vote_url"
).rename({"date": "vote_date"})
sponsor_data = pl.read_csv(DATA_DIR / "silver" / "sponsors.csv", encoding="utf-8").select(
    [
        "unique_member_code",
        "sponsor_is_primary",
        "bill_no",
        # "bill_year",
        # "bill_type",
        # "short_title_en",
        # "long_title_en",
        # "last_updated",
        # "status",
        # "source",
        # "method",
        # "most_recent_stage_event_show_as",
        # "most_recent_stage_event_progress_stage",
        # "most_recent_stage_event_stage_completed",
        # "most_recent_stage_event_house_show_as",
        # "context_date",
        "bill_url",
    ]
).unique()
# .drop_nulls(subset=["unique_member_code"])

# print(vote_data_df.estimated_size(unit="gb"))
# print(sponsor_data.estimated_size(unit="gb"))

#JOIN is too massive
#ISSUE: the join is creating a cartesian product and blowing up the dataset size, which is likely due to duplicate join keys in one or both datasets. Need to investigate the join keys and ensure 
# they are unique or handle duplicates appropriately before joining.
#print("starting to enrich votes with TD metadata and committee assignments...")

#https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/
#https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/
#https://learn.microsoft.com/en-us/power-bi/guidance/star-schema
#https://docs.getdbt.com/blog/kimball-dimensional-model?version=1.12
#https://pandas.pydata.org/docs/user_guide/merging.html?utm_source
# current_dail_vote_history_df = vote_data_df.join(enrich_vote, on=["unique_member_code"], how="left", validate="m:m").drop(
#   "vote_id", "join_key"
# ).unique()
print("starting to enrich votes with sponsor and bill data...")
# current_dail_vote_history_df = current_dail_vote_history_df.join(sponsor_data, on=["unique_member_code"], how="left").unique()
print("enriched votes with sponsor and bill data successfully.")
print("writing enriched votes and legislation CSV...")
# current_dail_vote_history_df.write_csv(DATA_DIR / "gold" / "current_dail_vote_history.csv")
logging.info("Enriched TD votes and legislationCSV created successfully.")
