"""
validate_interest_recovery.py  (SANDBOX)
----------------------------------------
Proof that the wide roster recovers the dropped historic declarers.

For each already-downloaded Dáil register PDF, parse it with the PROD parser,
build the same join_key, and count how many distinct declarers match:
  - the CURRENT roster  (data/silver/flattened_members.csv  — today's behaviour)
  - the WIDE roster     (_out/member_roster_wide.parquet     — proposed)

`recovered` = declarers that exist in the PDF and the wide roster but are
silently dropped today. No files written; read-only measurement.

Run:  python -m pipeline_sandbox.historic_members.validate_interest_recovery
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import polars as pl

from members.member_interests import (
    CATEGORIES_PATTERN,
    MEMBER_NAME_PATTERN,
    PDF_PATHS,
    clean_interests,
    extract_raw_lines,
    group_lines,
    parse_members,
    split_embedded_names,
)
from shared import normalise_join_key

OUT_DIR = Path(__file__).parent / "_out"
CURRENT_MASTER = Path("data/silver/flattened_members.csv")
WIDE_MASTER = OUT_DIR / "member_roster_wide.parquet"


def _norm_keys(df: pl.DataFrame) -> set[str]:
    df = df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    df = normalise_join_key.normalise_df_td_name(df, "join_key")
    return set(df.get_column("join_key").to_list())


def _parsed_declarers(year_key: str) -> set[str]:
    numeric_year = int(year_key.split("_")[0])
    lines = extract_raw_lines(PDF_PATHS[year_key])
    lines = split_embedded_names(lines)
    grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)
    members = parse_members(grouped, MEMBER_NAME_PATTERN)
    df = pl.DataFrame(members)
    df = clean_interests(df, numeric_year)
    df = normalise_join_key.normalise_df_td_name(df, "join_key")
    return set(df.get_column("join_key").to_list())


def main() -> None:
    con = duckdb.connect()
    cur = pl.from_pandas(
        con.execute(f"select first_name, last_name from read_csv('{CURRENT_MASTER.as_posix()}')").df()
    )
    wide = pl.from_pandas(
        con.execute(f"select first_name, last_name from read_parquet('{WIDE_MASTER.as_posix()}')").df()
    )
    cur_keys = _norm_keys(cur)
    wide_keys = _norm_keys(wide)
    print(f"current roster keys: {len(cur_keys)} | wide roster keys: {len(wide_keys)}")

    rows = []
    for year_key in [k for k in PDF_PATHS if k.endswith("_dail")]:
        declarers = _parsed_declarers(year_key)
        declarers.discard("")
        m_cur = declarers & cur_keys
        m_wide = declarers & wide_keys
        rows.append(
            {
                "year": year_key,
                "parsed_declarers": len(declarers),
                "matched_current": len(m_cur),
                "matched_wide": len(m_wide),
                "recovered": len(m_wide) - len(m_cur),
                "still_unmatched": len(declarers - wide_keys),
            }
        )
        print(rows[-1])

    (OUT_DIR / "recovery_report.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
