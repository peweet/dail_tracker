import polars as pl

df =pl.read_csv("C://Users//pglyn//PycharmProjects//dail_extractor//lobbyist//output//lobby_break_down_by_politician.csv")

most_represnted_politicians = df.select('primary_key', 'full_name').unique().drop('primary_key')

most_represnted_politicians = most_represnted_politicians.select(pl.col('full_name').value_counts()).unnest('full_name').sort('full_name', descending=True)
most_represnted_politicians.write_csv("C://Users//pglyn//PycharmProjects//dail_extractor//lobbyist//output//with_lobbyist_most_represented_politicians.csv")
df = df.select(
                'primary_key',	
                'lobbyist_name', 
                'public_policy_area',	
                'specific_details', 
                'intended_results', 
                'person_primarily_responsible', 
                'dpos_or_former_dpos_who_carried_out_lobbying', 
                'dpos_or_former_dpos_who_carried_out_lobbying_name', 
                'was_this_a_grassroots_campaign', 
                'grassroots_directive', 
                'was_this_lobbying_done_on_behalf_of_a_client',
                'client_name', 'current_or_former_dpos_position', 
                'current_or_former_dpos_chamber'
# ).filter(pl.col('was_this_lobbying_done_on_behalf_of_a_client')=='Yes'
# ).filter(pl.col('current_or_former_dpos_position').is_not_null()
).unique()

df.write_csv("C://Users//pglyn//PycharmProjects//dail_extractor//lobbyist//output//repeat_offenders.csv")
# full_name	position, delivery

most_represented_dpos = df.select(pl.col('dpos_or_former_dpos_who_carried_out_lobbying_name').value_counts()).unnest('dpos_or_former_dpos_who_carried_out_lobbying_name').sort('dpos_or_former_dpos_who_carried_out_lobbying_name', descending=True)
print(most_represented_dpos) 
most_represented_dpos.write_csv("C://Users//pglyn//PycharmProjects//dail_extractor//lobbyist//output//most_represented_dpos.csv")

most_represented_companies = df.select(pl.col('client_name').value_counts()).unnest('client_name').sort('client_name', descending=True)
most_represented_companies.write_csv("C://Users//pglyn//PycharmProjects//dail_extractor//lobbyist//output//most_represented_companies.csv")