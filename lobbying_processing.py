import polars as pl
from utility.select_drop_rename_cols_mappings import lobbying_rename
import polars.selectors as cs
df = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_returns_results.csv")
df = df.rename(lobbying_rename)


split_df = df.with_columns(pl.col("dpo_lobbied").str.split("::").alias("lobbyists")
        ).explode("lobbyists").with_columns(
        pl.col("lobbyists").str.split("|").alias("parts")
    )
split_df = split_df.with_columns(
        pl.col("parts").list.get(0).alias("full_name"),
        pl.col("parts").list.get(1).alias("position"),
        pl.col("parts").list.get(2).alias("chamber"),
    ).drop("lobbyists", "parts", "dpo_lobbied")
split_df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/filtered_lobby.csv')
most_prolific_lobbyist = df.select(
        pl.col('primary_key'),
        pl.col("lobbyist_name"),
        pl.col("dpo_lobbied").str.split("::").list.len().alias("politicians_involved_count")
    )
counts = most_prolific_lobbyist.group_by(pl.col("lobbyist_name")).agg(pl.len().alias("lobby_requests_count"
        ))
most_prolific_lobbyist = most_prolific_lobbyist.join(counts, on="lobbyist_name")
most_prolific_lobbyist= most_prolific_lobbyist.sort("politicians_involved_count",  descending=True)

# .group_by("lobbyist_name"
#         ).agg(
#         pl.len().alias("count"),
#         pl.col("politicians_count").sum().alias("avg_politicians_involved"),
#     ).sort("count", descending=True)
# most_prolific_lobbyist= most_prolific_lobbyist.sort('count', descending=True)
most_prolific_lobbyist.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_count.csv')
# Barry Harrington|Adviser to Minister of State|Department of Finance
# 