import glob
import json
import logging

import pandas as pd

from config import DATA_DIR, VOTES_RAW_DIR

"""
This module transforms the raw vote data extracted from the Oireachtas API into a clean, 
structured format suitable for analysis.
It reads the raw JSON files containing vote records,
normalizes the nested structures to create a flat DataFrame,
and enriches the data by creating new features such as vote URLs.
"""
votes = glob.glob(str(VOTES_RAW_DIR / "*.json"))
results = []
for json_file in votes:
    print(f"Loading votes from {json_file}...")
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)[0]["results"]
        results.extend(data)

logging.info(f"Total votes loaded: {len(results)}")


def normalize_vote_data(result: dict) -> pd.DataFrame:
    division = result["division"]
    date = division.get("date")
    house_number = division.get("house", {}).get("houseNo")
    outcome = division.get("outcome")
    vote_id = division.get("voteId")
    debate_title = division.get("debate", {}).get("showAs")
    subject = division.get("subject", {}).get("showAs")
    different_vote_types = []
    for tally_key in ["taVotes", "nilVotes", "staonVotes"]:
        tallies = division.get("tallies")
        if tallies is None:
            continue

        tally = tallies.get(tally_key)
        if tally is None:
            continue

        members = tally.get("members", [])
        if not members:
            continue
        df = pd.json_normalize(members)
        df["vote_date"] = date
        df["vote_outcome"] = outcome
        df["vote_id"] = vote_id
        df["debate_title"] = debate_title
        df["vote_type"] = tally_key
        df["subject"] = subject
        df["house_number"] = house_number
        different_vote_types.append(df)
    return different_vote_types


dfs = []
for result in results:
    normalized_df = normalize_vote_data(result)
    dfs.extend(normalized_df)
df = pd.concat(dfs, ignore_index=True)
df = (
    df.rename(
        columns={
            "member.showAs": "member_name",
            "member.memberCode": "unique_member_code",
            "member.uri": "member_uri",
        }
    )
    .drop_duplicates()
    .drop("member_uri", axis=1)
)

# Votes URL enrichment
# URL format: https://www.oireachtas.ie/en/bills/bill/{house_number}/{vote_date}/{vote_id}/
# Example:    nternational Protection Bill 2026: From the Seanad = https://www.oireachtas.ie/en/debates/vote/dail/34/2026-04-15/80/
df["vote_id"] = df["vote_id"].str.split("_").str[-1]
#https://www.oireachtas.ie/en/debates/vote/dail/34/2026-04-21/90/
df["vote_url"] = df.apply(
    lambda row: (
        f"https://www.oireachtas.ie/en/debates/vote/dail/{row['house_number']}/{row['vote_date']}/{row['vote_id']}/"
    ),
    axis=1,
)
df["date"] = pd.to_datetime(df["vote_date"], errors="coerce").dt.date
df = df.drop("member_name", axis=1).drop("vote_date", axis=1)
df = df.replace({"nilVotes": "Voted No", "taVotes": "Voted Yes", "staonVotes": "Abstained"})
df.to_csv(DATA_DIR / "silver" / "pretty_votes.csv", index=False)
print("Votes data normalized and saved to pretty_votes.csv")
