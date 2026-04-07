import polars as pl 
import normalise_join_key
from utility.select_cols_drop_cols import enrich_cols_to_select
import logging
small_df = pl.read_csv('members/aggregated_td_tables.csv')
large_df = pl.read_csv('members/flattened_members.csv')

small_df = normalise_join_key.normalise_df_td_name(small_df)
logging.info('normalised small_df (PDF attendance) TD names')
large_df = normalise_join_key.normalise_df_td_name(large_df)
large_df = large_df.unique(subset=['join_key'], keep='first')
logging.info('normalised large_df (API members) TD names')

# https://regex101.com/r/OOLuZU/1
# API master list is the driving table; PDF attendance is left-joined onto it
large_df = large_df.select(enrich_cols_to_select)
enriched_df = large_df.join(small_df, on=['join_key'], how='left')
enriched_df = enriched_df.with_columns(pl.col('unique_member_code').str.extract(r"\b\d{4}\b", 0).alias('year_elected')
                                       )
enriched_df= enriched_df.with_columns(
    pl.when(pl.col('ministerial_office') != 'Null').then(pl.lit('true')).otherwise(pl.lit('false')).alias('ministerial_office_filled')
    )
# .drop('join_key')
enriched_df.write_csv('members/enriched_td_attendance.csv')
logging.info("Enriched TD attendance CSV created successfully.")