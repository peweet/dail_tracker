import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from config import LOBBY_OUTPUT_DIR

df = pl.read_csv(LOBBY_OUTPUT_DIR / "lobby_break_down_by_politician.csv")

most_represnted_politicians = df.select("primary_key", "full_name").unique().drop("primary_key")

most_represnted_politicians = (
    most_represnted_politicians.select(pl.col("full_name").value_counts())
    .unnest("full_name")
    .sort("full_name", descending=True)
)
most_represnted_politicians.write_csv(LOBBY_OUTPUT_DIR / "with_lobbyist_most_represented_politicians.csv")
df = df.select(
    "primary_key",
    "lobbyist_name",
    "public_policy_area",
    "specific_details",
    "intended_results",
    "person_primarily_responsible",
    "dpos_or_former_dpos_who_carried_out_lobbying",
    "dpos_or_former_dpos_who_carried_out_lobbying_name",
    "was_this_a_grassroots_campaign",
    "grassroots_directive",
    "was_this_lobbying_done_on_behalf_of_a_client",
    "client_name",
    "current_or_former_dpos_position",
    "current_or_former_dpos_chamber",
    # ).filter(pl.col('was_this_lobbying_done_on_behalf_of_a_client')=='Yes'
    # ).filter(pl.col('current_or_former_dpos_position').is_not_null()
).unique()

df.write_csv(LOBBY_OUTPUT_DIR / "repeat_offenders.csv")
# full_name	position, delivery

most_represented_dpos = (
    df.select(pl.col("dpos_or_former_dpos_who_carried_out_lobbying_name").value_counts())
    .unnest("dpos_or_former_dpos_who_carried_out_lobbying_name")
    .sort("dpos_or_former_dpos_who_carried_out_lobbying_name", descending=True)
)
print(most_represented_dpos)
most_represented_dpos.write_csv(LOBBY_OUTPUT_DIR / "most_represented_dpos.csv")

most_represented_companies = (
    df.select(pl.col("client_name").value_counts()).unnest("client_name").sort("client_name", descending=True)
)
most_represented_companies.write_csv(LOBBY_OUTPUT_DIR / "most_represented_companies.csv")

# data investigation
# select_cols = ['last_name','first_name','unique_member_code']

# member_interest_2025 = pl.read_csv("C://Users//pglyn//PycharmProjects//dail_extractor//members//member_interests_grouped_2025.csv")

# members_interests_2026 = pl.read_csv("C://Users//pglyn//PycharmProjects//dail_extractor//members//member_interests_grouped_2026.csv")

# member_interest_2025 = member_interest_2025.select(select_cols).unique()
# members_interests_2026 = members_interests_2026.select(select_cols).unique()
# print(member_interest_2025.count())

# print(members_interests_2026.count())

# concat_df = pl.concat([member_interest_2025, members_interests_2026]).unique()
# print(concat_df.count())

# concat_df.write_csv("C://Users//pglyn//PycharmProjects//dail_extractor//members//unique_td_list.csv")
