import polars as pl 
import normalise_join_key
cols_to_select = ['join_key',
                  'primary_key',
                  'party', 
                  'first_name', 
                  'last_name', 
                  'member_constituency', 
                  'dail_term', 
                  'ministerial_office',
                  'committee_1_name_english',
                  'member_memberships_0_membership_committees_0_status',
                #   'committee_1_name_irish',
                  'committee_2_name_english',
                  'committee_2_status',
                  'committee_3_name_english',
                  'committee_3_status',
                  'committee_4_name_english',
                  'committee_4_status',
                  'committee_5_name_english',
                  'committee_5_status',
                #   'committee_4_service_unit'
                  ]

small_df = pl.read_csv('members/td_tables.csv')
large_df = pl.read_csv('members/flattened_members.csv')

small_df = normalise_join_key.normalise_df_td_name(small_df)
print('normalised small_df TD names')
large_df = normalise_join_key.normalise_df_td_name(large_df)
print('normalised small_df TD names')

# https://regex101.com/r/OOLuZU/1
large_df = large_df.select(cols_to_select)
enriched_df = small_df.join(large_df, on=['join_key'], how='left')
enriched_df = enriched_df.with_columns(pl.col('primary_key').str.extract(r"\b\d{4}\b", 0).alias('year_elected')
                                       )
enriched_df= enriched_df.with_columns(
    pl.when(pl.col('ministerial_office') != 'Null').then(pl.lit('true')).otherwise(pl.lit('false')).alias('ministerial_office_filled')
    ).drop('join_key')
enriched_df.write_csv('members/enriched_td_attendance.csv')
print("Enriched TD attendance CSV created successfully.")