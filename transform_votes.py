"""
transform_votes.py — normalise raw Oireachtas vote JSON into silver CSV.

Reads VOTES_RAW_DIR/*.json (written by services/oireachtas_api_main.py),
flattens the nested division/tally/member structure, attaches the vote URL
and a globally-unique vote_id (date-prefixed), writes
silver/pretty_votes.csv.

Behaviour:
  - Skipped cleanly (exit 0) when VOTES_RAW_DIR has no JSON or no vote rows.
"""
from __future__ import annotations

import glob
import json
import logging
import sys

import pandas as pd

from config import SILVER_DIR, VOTES_RAW_DIR

logger = logging.getLogger(__name__)


def _load_results(votes_dir) -> list[dict]:
    """Walk every *.json in votes_dir, flatten the page→results structure."""
    votes = glob.glob(str(votes_dir / "*.json"))
    results: list[dict] = []
    for json_file in votes:
        print(f"Loading votes from {json_file}...")
        with open(json_file, encoding="utf8") as f:
            for payload in json.load(f):
                results.extend(payload["results"])
    logging.info(f"Total votes loaded: {len(results)}")
    return results


def normalize_vote_data(result: dict) -> list[pd.DataFrame]:
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


def main() -> int:
    """Normalise raw vote JSON → silver/pretty_votes.csv.

    Exit codes:
        0 — ok, or skipped cleanly (no JSON / no rows extracted)
    """
    results = _load_results(VOTES_RAW_DIR)
    if not results:
        logger.warning("No vote results in %s — skipping pretty_votes write.", VOTES_RAW_DIR)
        print(f"No vote JSON in {VOTES_RAW_DIR} — skipping.")
        return 0

    dfs: list[pd.DataFrame] = []
    for result in results:
        normalized = normalize_vote_data(result)
        dfs.extend(normalized)
    if not dfs:
        logger.warning("Vote results present but no tally rows extracted — skipping.")
        print("No vote tallies extracted — skipping.")
        return 0

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
    df["vote_id"] = df["vote_id"].str.split("_").str[-1]
    df["vote_url"] = df.apply(
        lambda row: (
            f"https://www.oireachtas.ie/en/debates/vote/dail/{row['house_number']}/{row['vote_date']}/{row['vote_id']}/"
        ),
        axis=1,
    )
    # vote_id from the API is a per-day counter (1, 2, ... 90) and collides across
    # dates. Make it globally unique by prefixing the date so every join, GROUP BY,
    # and DISTINCT downstream is correct without per-view fixes. URL above already
    # uses the bare counter and is unaffected.
    df["vote_id"] = df["vote_date"].astype(str) + "_" + df["vote_id"].astype(str)
    df["date"] = pd.to_datetime(df["vote_date"], errors="coerce").dt.date
    df = df.drop("member_name", axis=1).drop("vote_date", axis=1)
    df = df.replace({"nilVotes": "Voted No", "taVotes": "Voted Yes", "staonVotes": "Abstained"})
    df.to_csv(SILVER_DIR / "pretty_votes.csv", index=False)
    print("Votes data normalized and saved to pretty_votes.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
