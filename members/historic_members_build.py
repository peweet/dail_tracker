"""
historic_members_build.py — backfill former TDs/Senators from past terms.

Why this exists
    members/flatten_members_json_to_csv.py builds the canonical CURRENT roster
    (flattened_members.parquet = current term only). The Register-of-Interests
    parser then joins `master LEFT JOIN interests`, so any 2016-2023 declarer who
    is no longer a sitting member is silently dropped. (Measured: ~60 declarers
    per year, 2020-2023.)

What this builds (all ADDITIVE — flattened_members.parquet is left untouched)
    data/silver/parquet/member_terms.parquet
        one row per member × term (unique_member_code, house, dail_number,
        dail_term, start, end, is_current) — powers the "filter by Dáil" UI and
        the is_current flag on the registry view.
    data/silver/historic_members_{dail,seanad}.csv  (+ parquet)
        members who served in a PAST term but are NOT in the current roster —
        i.e. former members only. The interests parser unions this with the
        current master so historic declarers stop being dropped. Returning
        members are excluded here (they already match the current master via
        their stable memberCode), so the union never double-counts them.

Run:  python -m members.historic_members_build
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from flatten_json import flatten

from config import SILVER_DIR
from members.members_api_service import fetch_members
from services.parquet_io import save_parquet
from shared.select_drop_rename_cols_mappings import members_drop_cols, members_rename

logger = logging.getLogger(__name__)

PARQUET_DIR = SILVER_DIR / "parquet"

# Columns the interests join (members/member_interests.py:join_master_list)
# selects from the master. The historic roster must carry all of them.
_JOIN_COLS = [
    "unique_member_code",
    "first_name",
    "last_name",
    "constituency_name",
    "full_name",
    "party",
    "ministerial_office",
    "year_elected",
]


@dataclass(frozen=True)
class Term:
    house: str
    no: int
    date_start: str
    date_end: str


# Terms cover every year the Register of Members' Interests exists (first
# register = 1995 declarations, published April 1996, during the 27th Dáil /
# 20th Seanad). Current terms (34th Dáil / 27th Seanad) are pulled too so
# member_terms is complete and is_current is derivable, but they are dropped
# from the historic roster (they already live in flattened_members).
#
# date_start MUST precede the term's first membership: the API filters members by
# membership_start >= date_start, so a late start silently drops early-sworn
# members (Seanad 26 returned 19/68 with a 2020-06-29 start — fixed to 2020-01-01).
# Use a generous window per term (well before the election → well after dissolution).
TERMS: list[Term] = [
    Term("dail", 27, "1992-01-01", "1997-12-31"),
    Term("dail", 28, "1997-01-01", "2002-12-31"),
    Term("dail", 29, "2002-01-01", "2007-12-31"),
    Term("dail", 30, "2007-01-01", "2011-06-01"),
    Term("dail", 31, "2011-01-01", "2016-06-01"),
    Term("dail", 32, "2016-01-01", "2020-06-01"),
    Term("dail", 33, "2020-01-01", "2024-12-31"),
    Term("dail", 34, "2024-09-01", "2099-01-01"),
    Term("seanad", 20, "1993-01-01", "1997-12-31"),
    Term("seanad", 21, "1997-01-01", "2002-12-31"),
    Term("seanad", 22, "2002-01-01", "2007-12-31"),
    Term("seanad", 23, "2007-01-01", "2011-12-31"),
    Term("seanad", 24, "2011-01-01", "2016-12-31"),
    Term("seanad", 25, "2016-01-01", "2020-12-31"),
    Term("seanad", 26, "2020-01-01", "2025-12-31"),
    Term("seanad", 27, "2025-01-01", "2099-01-01"),
]


def _flatten(results: list[dict]) -> pd.DataFrame:
    """Mirror flatten_members_json_to_csv so columns match silver exactly."""
    df = pd.DataFrame([flatten(m) for m in results])
    df = df.rename(members_rename, axis=1)
    df = df.drop(columns=members_drop_cols, errors="ignore")
    if "office_1_name" in df.columns:
        mask = df["office_1_name"].notna() & df["office_1_name"].str.contains("Minister", case=False, na=False)
        df["ministerial_office"] = mask.astype(str).replace({"True": "true", "False": "false"})
    else:
        df["ministerial_office"] = "false"
    df["year_elected"] = df["unique_member_code"].str.extract(r"(\b\d{4}\b)", expand=False).astype("Int64")
    return df


def _current_codes(house: str) -> set[str]:
    """unique_member_codes already in the canonical current roster."""
    name = "flattened_members.parquet" if house == "dail" else "flattened_seanad_members.parquet"
    path = PARQUET_DIR / name
    if not path.exists():
        logger.warning("current roster missing (%s) — historic roster will include all members", path)
        return set()
    return set(pd.read_parquet(path, columns=["unique_member_code"])["unique_member_code"])


def build() -> dict:
    per_term: list[pd.DataFrame] = []
    for t in TERMS:
        resp = fetch_members(t.house, house_no=t.no, date_start=t.date_start, date_end=t.date_end)
        df = _flatten(resp.get("results", []))
        df["house_tag"] = t.house
        df["dail_number"] = df.get("dail_number")  # carried from flatten; kept explicit
        per_term.append(df)
        logger.info("[%s %d] %d members", t.house, t.no, len(df))

    long = pd.concat(per_term, ignore_index=True)
    long["membership_start_date"] = pd.to_datetime(long["membership_start_date"], errors="coerce")
    long["membership_end_date"] = pd.to_datetime(long["membership_end_date"], errors="coerce")

    # ── member_terms sidecar (one row per member × term) ─────────────────────
    terms = (
        long[
            [
                "unique_member_code",
                "full_name",
                "house_tag",
                "dail_number",
                "dail_term",
                "membership_start_date",
                "membership_end_date",
                "party",
                "constituency_name",
            ]
        ]
        .drop_duplicates(subset=["unique_member_code", "house_tag", "dail_number"])
        .rename(columns={"house_tag": "house"})
        .copy()
    )
    terms["is_current"] = terms["membership_end_date"].isna()
    save_parquet(terms, PARQUET_DIR / "member_terms.parquet")

    # dails_served per member (for the future registry view)
    dails = (
        terms.groupby("unique_member_code")["dail_number"]
        .apply(lambda s: ",".join(sorted({str(int(x)) for x in s if pd.notna(x)})))
        .rename("dails_served")
    )

    report: dict = {"member_terms_rows": int(len(terms)), "houses": {}}

    # ── per-house historic-only roster (former members) ──────────────────────
    for house in ("dail", "seanad"):
        sub = long[long["house_tag"] == house]
        # one row per member, kept at most-recent term so the row is the freshest
        wide = sub.sort_values("membership_start_date").drop_duplicates(
            subset=["unique_member_code"], keep="last"
        )
        current = _current_codes(house)
        historic = wide[~wide["unique_member_code"].isin(current)].copy()
        historic["is_current"] = False
        historic = historic.merge(dails, on="unique_member_code", how="left")

        # keep join columns (+ a couple useful extras) and guarantee they exist
        keep = [c for c in (_JOIN_COLS + ["dail_number", "dails_served", "is_current"]) if c in historic.columns]
        out = historic[keep].drop_duplicates(subset=["unique_member_code"])

        csv_path = SILVER_DIR / f"historic_members_{house}.csv"
        out.to_csv(csv_path, index=False, encoding="utf-8")
        save_parquet(out, PARQUET_DIR / f"historic_members_{house}.parquet")
        report["houses"][house] = {
            "terms_members": int(sub["unique_member_code"].nunique()),
            "current_excluded": len(current),
            "historic_only": int(len(out)),
        }
        logger.info("[%s] historic-only roster: %d former members", house, len(out))

    return report


def main() -> None:
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("historic_members_build")
    import json

    print(json.dumps(build(), indent=2))


if __name__ == "__main__":
    main()
