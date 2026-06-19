"""
historic_member_pull.py  (SANDBOX — pipeline_sandbox/)
------------------------------------------------------
Phase 0/1 of the "historic TDs" backfill.

Root cause this addresses
    members/members_api_service.py hardcodes chamber_id=house/dail/34, so the
    master roster (data/silver/flattened_members.parquet) is the 34th Dáil only.
    members/member_interests.py then joins `master LEFT JOIN interests`, which
    silently DROPS every 2016-2023 declarer who isn't in the current roster.

What this script does (sandbox-only, writes NOTHING to data/silver)
    1. Pulls the members API scoped to each historic term (chamber_id + date
       window) — the lever the prod service is missing.
    2. Flattens each pull with the SAME rename/drop mapping the prod flattener
       uses, so columns are schema-identical to flattened_members.parquet.
    3. Emits two artifacts under ./_out/:
         - member_roster_wide.parquet   one row per member (deduped to the
           member's most-recent term). CURRENT members keep their 34th-Dáil row
           byte-for-byte, so promoting this only ADDS historic rows.
         - member_terms.parquet         normalised member × term sidecar
           (unique_member_code, dail_number, dail_term, house, start, end,
           is_current) — powers the "filter by Dáil" UI.

Run:  python -m pipeline_sandbox.historic_members.historic_member_pull
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from flatten_json import flatten

from config import API_BASE
from services.parquet_io import save_parquet
from shared.select_drop_rename_cols_mappings import members_drop_cols, members_rename

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).parent / "_out"

# Each term = one scoped API pull. Date windows bracket the term loosely; the
# API filters memberships to the chamber, and (verified) returns memberships[0]
# as THAT term's membership, so dail_number/start/end are the term's own values.
# "go back to 2016" => 32nd Dáil onward; 31st included so 2016-register declarers
# from the outgoing Dáil still match (cheap, maximises recovery).


@dataclass(frozen=True)
class Term:
    house: str  # "dail" | "seanad"
    no: int  # house number (e.g. 33)
    date_start: str
    date_end: str


TERMS: list[Term] = [
    Term("dail", 31, "2011-03-09", "2016-03-09"),
    Term("dail", 32, "2016-03-10", "2020-02-07"),
    Term("dail", 33, "2020-02-08", "2024-11-28"),
    Term("dail", 34, "2024-11-29", "2099-01-01"),
    # Seanad equivalents (terms run alongside each Dáil). Included so former
    # Senators' declarations are recoverable too; Dáil is the primary deliverable.
    Term("seanad", 24, "2011-04-25", "2016-04-24"),
    Term("seanad", 25, "2016-04-25", "2020-06-28"),
    Term("seanad", 26, "2020-06-29", "2025-01-22"),
    Term("seanad", 27, "2025-01-23", "2099-01-01"),
]


def fetch_term(term: Term) -> list[dict]:
    """Scoped members pull for one term — the missing lever from the prod service."""
    chamber = f"%2Fie%2Foireachtas%2Fhouse%2F{term.house}%2F{term.no}"
    url = (
        f"{API_BASE}/members?chamber_id={chamber}"
        f"&date_start={term.date_start}&date_end={term.date_end}&limit=600"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    results = r.json().get("results", [])
    logger.info("[%s %d] %d members", term.house, term.no, len(results))
    return results


def flatten_results(results: list[dict]) -> pd.DataFrame:
    """Mirror members/flatten_members_json_to_csv.py so columns match silver exactly."""
    df = pd.DataFrame([flatten(m) for m in results])
    df = df.rename(members_rename, axis=1)
    df = df.drop(columns=members_drop_cols, errors="ignore")
    if "office_1_name" in df.columns:
        mask = df["office_1_name"].notna() & df["office_1_name"].str.contains("Minister", case=False, na=False)
        df["ministerial_office"] = mask.astype(str).replace({"True": "true", "False": "false"})
    df["year_elected"] = df["unique_member_code"].str.extract(r"(\b\d{4}\b)", expand=False).astype("Int64")
    return df


def main() -> None:
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("historic_member_pull")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    per_term: list[pd.DataFrame] = []
    for term in TERMS:
        df = flatten_results(fetch_term(term))
        # Stamp the house tag for the sidecar (dail_number alone is ambiguous
        # across houses once Seanad terms are unioned in).
        df["house_tag"] = term.house
        per_term.append(df)

    long = pd.concat(per_term, ignore_index=True)
    long["membership_start_date"] = pd.to_datetime(long["membership_start_date"], errors="coerce")
    long["membership_end_date"] = pd.to_datetime(long["membership_end_date"], errors="coerce")

    # ── member_terms sidecar: one row per member × term ──────────────────────
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
        .copy()
    )
    terms["is_current"] = terms["membership_end_date"].isna()
    save_parquet(terms, OUT_DIR / "member_terms.parquet")

    # ── wide roster: one row per member, kept at most-recent term ─────────────
    # Sort so the latest term wins -> current members keep their 34th-Dáil row.
    wide = (
        long.sort_values("membership_start_date")
        .drop_duplicates(subset=["unique_member_code"], keep="last")
        .copy()
    )
    wide["is_current"] = wide["membership_end_date"].isna()
    dails = (
        terms.groupby("unique_member_code")["dail_number"]
        .apply(lambda s: ",".join(sorted({str(int(x)) for x in s if pd.notna(x)})))
        .rename("dails_served")
    )
    wide = wide.merge(dails, on="unique_member_code", how="left")
    save_parquet(wide.drop(columns=["house_tag"]), OUT_DIR / "member_roster_wide.parquet")

    # ── report ───────────────────────────────────────────────────────────────
    dail_terms = terms[terms["house_tag"] == "dail"]
    report = {
        "terms_pulled": [f"{t.house}/{t.no}" for t in TERMS],
        "distinct_members_total": int(long["unique_member_code"].nunique()),
        "distinct_dail_members": int(dail_terms["unique_member_code"].nunique()),
        "wide_rows": int(len(wide)),
        "is_current_true": int(wide["is_current"].sum()),
        "is_current_false_historic": int((~wide["is_current"]).sum()),
        "dail_members_per_term": dail_terms.groupby("dail_number")["unique_member_code"].nunique().to_dict(),
        "dails_served_distribution": wide["dails_served"].value_counts().head(12).to_dict(),
    }
    (OUT_DIR / "roster_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
