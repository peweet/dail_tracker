import polars as pl 
import normalise_join_key
from utility.select_drop_rename_cols_mappings import enrichment_cols_to_select, committees_cols_to_select, members_rename
import logging
from config import DATA_DIR

#TODO: use impeccable to try create a better UI 
#https://chatgpt.com/c/69e2b380-004c-83eb-b3b6-feaed861c6df

small_df = pl.read_csv(DATA_DIR /"silver" / 'aggregated_td_tables.csv')
large_df = pl.read_csv(DATA_DIR /"silver" / 'flattened_members.csv')

committee_df = large_df.select(committees_cols_to_select)
print(committee_df.schema)
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
# enriched_df= enriched_df.with_columns(
#     pl.when(pl.col('ministerial_office') != 'Null')
#     .then(pl.lit('true'))
#     .otherwise(pl.lit('false'))
#     .alias('ministerial_office_filled')
#     )


enriched_df.write_csv(DATA_DIR / "gold" / 'enriched_td_attendance.csv')
logging.info("Enriched TD attendance CSV created successfully.")


committee_df.write_csv(DATA_DIR / "gold" / 'committee_assignments.csv')
logging.info("Committee assignments CSV created successfully.")



votes_df = pl.read_csv(DATA_DIR / "silver" / 'pretty_votes.csv')
enrich_vote = pl.read_csv(DATA_DIR / "gold" / 'enriched_td_attendance.csv')
key_data = enrich_vote.select(['join_key', 'unique_member_code', 'year_elected', 'last_name','dail_term','dail_number', 'full_name', 'first_name'])
key_data = key_data.unique(subset=['unique_member_code'])
current_dail_vote_history_df = votes_df.join(key_data, on='unique_member_code', how='left')
current_dail_vote_history_df = current_dail_vote_history_df.unique(subset=['unique_member_code', 'vote_id']).drop('join_key')
current_dail_vote_history_df.write_csv(DATA_DIR / "gold" / 'current_dail_vote_history.csv')
logging.info("Enriched TD votes CSV created successfully.")
if __name__ == "__main__":
    print("Enriched TD datasets created successfully and saved to enriched_td_attendance.csv.")