"""
Generator for SQL-view fixture parquets.

Run this to (re)create the committed fixtures under
`test/fixtures/sql_views/{silver,gold}/parquet/`. The output parquets are
committed; this script exists so the schema is human-readable and
regeneration is reproducible if the pipeline schema drifts.

Fixtures generated:
  - silver/parquet/flattened_members.parquet         (3 rows, satisfies member_registry view)
  - silver/parquet/member_external_links.parquet    (3 rows, satisfies v_member_external_links view)
  - gold/parquet/pretty_votes.parquet                (12 rows = 3 votes × 4 members,
                                                      satisfies 7 vote_* views)

Run:
    python test/fixtures/sql_views/_generate.py
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

FIXTURES_ROOT = Path(__file__).resolve().parent
SILVER_PQ = FIXTURES_ROOT / "silver" / "parquet"
GOLD_PQ = FIXTURES_ROOT / "gold" / "parquet"

# ---------------------------------------------------------------------------
# flattened_members.parquet — drives v_member_registry
# Columns: unique_member_code, full_name, constituency_name, party,
#          ministerial_office, year_elected, membership_start_date, membership_end_date
# ---------------------------------------------------------------------------

FLATTENED_MEMBERS = pl.DataFrame(
    {
        "unique_member_code": [
            "Mary-Murphy.D.2020-02-08",
            "Sean-OBrien.D.2016-02-26",
            "Aoife-NiBhroin.D.2024-11-29",
        ],
        "full_name": ["Mary Murphy", "Sean O'Brien", "Aoife Ní Bhroin"],
        "constituency_name": ["Dublin South", "Cork North-West", "Galway West"],
        "party": ["Fianna Fáil", "Sinn Féin", "Green Party"],
        # ministerial_office stored as string 'true'/'false' to match the
        # CASE LOWER(CAST(... AS VARCHAR)) = 'true' pattern in member_registry.sql.
        "ministerial_office": ["false", "true", "false"],
        "year_elected": [2020, 2016, 2024],
        "membership_start_date": [date(2020, 2, 8), date(2016, 2, 26), date(2024, 11, 29)],
        "membership_end_date": [None, None, None],
    }
)

# ---------------------------------------------------------------------------
# pretty_votes.parquet — drives 7 v_vote_* views
# Columns: vote_id, date, debate_title, vote_outcome, vote_type, subject,
#          vote_url, unique_member_code, full_name, party, constituency_name
# Shape: 3 distinct votes × 4 members each = 12 rows, mix of Yes/No/Abstained
# across two years so vote_td_year_summary has multiple year groups.
# ---------------------------------------------------------------------------

# Build the cross product programmatically to keep the data block readable.
_VOTE_HEADER = [
    (
        "v_001",
        date(2025, 3, 15),
        "Climate Action Bill 2025 — Second Stage",
        "Passed",
        "climate",
        "https://www.oireachtas.ie/en/divisions/2025-03-15/1/",
    ),
    (
        "v_002",
        date(2025, 6, 22),
        "Housing Emergency Measures (Amendment) — Final Stage",
        "Defeated",
        "housing",
        "https://www.oireachtas.ie/en/divisions/2025-06-22/2/",
    ),
    (
        "v_003",
        date(2026, 1, 30),
        "Health Service Reform — Committee Stage",
        "Passed",
        "health",
        "https://www.oireachtas.ie/en/divisions/2026-01-30/3/",
    ),
]

# (member_code, full_name, party, constituency)
_MEMBERS = [
    ("Mary-Murphy.D.2020-02-08", "Mary Murphy", "Fianna Fáil", "Dublin South"),
    ("Sean-OBrien.D.2016-02-26", "Sean O'Brien", "Sinn Féin", "Cork North-West"),
    ("Aoife-NiBhroin.D.2024-11-29", "Aoife Ní Bhroin", "Green Party", "Galway West"),
    ("John-Walsh.D.2020-02-08", "John Walsh", "Fine Gael", "Dublin Rathdown"),
]

# Each member's stance on each vote — varied so aggregations produce non-trivial results.
_STANCES = {
    "v_001": ["Voted Yes", "Voted No", "Voted Yes", "Abstained"],
    "v_002": ["Voted No", "Voted Yes", "Abstained", "Voted No"],
    "v_003": ["Voted Yes", "Voted Yes", "Voted No", "Voted Yes"],
}

_rows = []
for vote_id, vote_date, debate_title, outcome, subject, url in _VOTE_HEADER:
    for (code, name, party, constituency), stance in zip(_MEMBERS, _STANCES[vote_id], strict=True):
        _rows.append(
            {
                "vote_id": vote_id,
                "date": vote_date,
                "debate_title": debate_title,
                "vote_outcome": outcome,
                "vote_type": stance,
                "subject": subject,
                "vote_url": url,
                "unique_member_code": code,
                "full_name": name,
                "party": party,
                "constituency_name": constituency,
            }
        )

PRETTY_VOTES = pl.DataFrame(_rows)


# ---------------------------------------------------------------------------
# member_external_links.parquet — drives v_member_external_links
# Output of wikidata_socials_etl.py; columns mirror that script's schema.
# Mary has every chip populated, Sean is Twitter-only, Aoife is Wikipedia-only —
# enough variation to exercise the "skip-null" branch in the UI chip loop.
# ---------------------------------------------------------------------------

MEMBER_EXTERNAL_LINKS = pl.DataFrame(
    {
        "unique_member_code": [
            "Mary-Murphy.D.2020-02-08",
            "Sean-OBrien.D.2016-02-26",
            "Aoife-NiBhroin.D.2024-11-29",
        ],
        "wikidata_qid": ["Q111111", "Q222222", "Q333333"],
        "wikipedia_url": [
            "https://en.wikipedia.org/wiki/Mary_Murphy",
            None,
            "https://en.wikipedia.org/wiki/Aoife_N%C3%AD_Bhro%C3%ADn",
        ],
        "twitter_handle": ["MaryMurphyTD", "SeanOBrienTD", None],
        "twitter_url": [
            "https://x.com/MaryMurphyTD",
            "https://x.com/SeanOBrienTD",
            None,
        ],
        "bluesky_handle": ["marymurphy.bsky.social", None, None],
        "bluesky_url": ["https://bsky.app/profile/marymurphy.bsky.social", None, None],
        "facebook_id": ["marymurphytd", None, None],
        "facebook_url": ["https://www.facebook.com/marymurphytd", None, None],
        "instagram_handle": ["marymurphytd", None, None],
        "instagram_url": ["https://www.instagram.com/marymurphytd/", None, None],
        "website_url": ["https://marymurphytd.ie", None, None],
    }
)


def main() -> None:
    SILVER_PQ.mkdir(parents=True, exist_ok=True)
    GOLD_PQ.mkdir(parents=True, exist_ok=True)

    members_path = SILVER_PQ / "flattened_members.parquet"
    votes_path = GOLD_PQ / "pretty_votes.parquet"
    ext_links_path = SILVER_PQ / "member_external_links.parquet"

    FLATTENED_MEMBERS.write_parquet(members_path, compression="zstd", compression_level=3, statistics=True)
    PRETTY_VOTES.write_parquet(votes_path, compression="zstd", compression_level=3, statistics=True)
    MEMBER_EXTERNAL_LINKS.write_parquet(
        ext_links_path, compression="zstd", compression_level=3, statistics=True
    )

    print(f"Wrote {members_path} ({FLATTENED_MEMBERS.height} rows)")
    print(f"Wrote {votes_path} ({PRETTY_VOTES.height} rows)")
    print(f"Wrote {ext_links_path} ({MEMBER_EXTERNAL_LINKS.height} rows)")


if __name__ == "__main__":
    main()
