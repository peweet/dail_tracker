import polars as pl

df = pl.read_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\lobbyist\\lobby_break_down_by_politician.csv")

# Segmented count by full_name and chamber
segmented = (
    df.group_by(["full_name", "chamber"])
      .agg(pl.count().alias("segmented_count"))
)

# Total count by full_name
total = (
    df.group_by("full_name")
      .agg(pl.count().alias("total_count"))
)

# Join total count onto segmented count
result = segmented.join(total, on="full_name")

# Write to CSV (CSV-compliant)
result.write_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\lobbyist\\lobby_count_by_politician.csv")