"""
Generator for SQL-view fixture parquets.

Run this to (re)create the committed fixtures under
`test/fixtures/sql_views/{silver,gold}/parquet/`. The output parquets are
committed; this script exists so the schema is human-readable and
regeneration is reproducible if the pipeline schema drifts.

Two fixture layouts:
  - Template-path views (member registry, votes, external links) read fixtures
    via {…_PATH} substitution, so they live directly under silver/ and gold/:
      - silver/parquet/flattened_members.parquet      (member_registry)
      - silver/parquet/member_external_links.parquet  (v_member_external_links)
      - gold/parquet/pretty_votes.parquet             (7 vote_* views)
  - Hardcoded-path views embed read_parquet('data/...') literals with no hook,
    so their fixtures mirror the real project layout under data/ (the test's
    _load rewrites 'data/ → this tree in CI mode):
      - data/gold/parquet/public_appointments.parquet        (v_public_appointments)
      - data/gold/parquet/ec_constituency_pop_2022.parquet   (constituency demographics)
      - data/silver/parquet/dail_/seanad_member_interests_combined.parquet (6 interests views)
      - data/silver/committees/committee_assignments.parquet + office_holders.parquet (4 committee views)
      - data/silver/parquet/questions.parquet                (6 member-question views)

Run:
    python test/fixtures/sql_views/_generate.py
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

FIXTURES_ROOT = Path(__file__).resolve().parent
SILVER_PQ = FIXTURES_ROOT / "silver" / "parquet"
GOLD_PQ = FIXTURES_ROOT / "gold" / "parquet"

# Hardcoded-path views read literals like read_parquet('data/gold/parquet/x').
# The test loader (_load) rewrites 'data/ → this fixture tree in CI mode, so the
# layout below MIRRORS the real project data/ layout under a fixtures root.
FIX_DATA = FIXTURES_ROOT / "data"


def _wp(df: pl.DataFrame, rel: str) -> Path:
    """Write a fixture parquet under FIX_DATA/<rel> (project convention: zstd)."""
    path = FIX_DATA / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)
    return path


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


# ---------------------------------------------------------------------------
# public_appointments.parquet — drives v_public_appointments (straight passthrough)
# ---------------------------------------------------------------------------

PUBLIC_APPOINTMENTS = pl.DataFrame(
    {
        "notice_ref": ["IO-APP-001", "IO-APP-002", "IO-APP-003"],
        "issue_date": ["2025-01-15", "2025-02-20", "2025-03-10"],
        "appointing_authority": ["Minister for Finance", "Government", "Minister for Health"],
        "appointment_type": ["board", "board", "authority"],
        "body": ["Central Bank Commission", "An Bord Pleanála", "HSE Board"],
        "appointee": ["Aoife Ní Bhroin", "John Walsh", "Mary Murphy"],
        "appointee_count": [1, 1, 1],
        "role": ["Member", "Member", "Chairperson"],
        "portfolio": ["Finance", "Planning", "Health"],
        "english_summary": [
            "Appointment of a member to the Central Bank Commission.",
            "Appointment of a member to An Bord Pleanála.",
            "Appointment of the Chairperson of the HSE Board.",
        ],
        "lang": ["en", "en", "en"],
        "title": ["Central Bank Commission", "An Bord Pleanála", "HSE Board"],
        "iris_source_pdf": ["iris_2025_005.pdf", "iris_2025_015.pdf", "iris_2025_020.pdf"],
    },
    schema_overrides={"appointee_count": pl.Int64},
)

# ---------------------------------------------------------------------------
# ec_constituency_pop_2022.parquet — drives v_member_constituency_demographics
# ---------------------------------------------------------------------------

EC_CONSTITUENCY_POP = pl.DataFrame(
    {
        "constituency_name": ["Dublin South", "Cork North-West", "Galway West"],
        "population_2022": [120000, 95000, 110000],
        "population_per_td_2022": [30000, 31667, 27500],
        "td_seats_2024": [4, 3, 4],
        "boundaries_label": ["2023 boundaries"] * 3,
        "source_key": ["EC2023_appendix2"] * 3,
    },
    schema_overrides={
        "population_2022": pl.Int64,
        "population_per_td_2022": pl.Int64,
        "td_seats_2024": pl.Int64,
    },
)

# ---------------------------------------------------------------------------
# dail_/seanad_member_interests_combined.parquet — drives the 6 interests views.
# Category labels matter: v_member_interests_index keys 'Land (including
# property)' and 'Shares' for property_count/share_count; category '15' is the
# excluded sentinel (none used here so nothing is filtered out).
# ---------------------------------------------------------------------------

_INTERESTS_SCHEMA = {
    "year_elected": pl.Int64,
    "interest_code": pl.Int64,
    "interest_count": pl.Int64,
    "year_declared": pl.Int32,
}


def _interest_row(
    code, fn, ln, party, constituency, category, desc, year, *, landlord=False, owner=False, occupation=False
):
    return {
        "unique_member_code": code,
        "first_name": fn,
        "last_name": ln,
        "constituency_name": constituency,
        "full_name": f"{fn} {ln}",
        "party": party,
        "ministerial_office": False,
        "year_elected": 2020,
        "constituency": constituency,
        "interest_code": 1,
        "interest_description_cleaned": desc,
        "interest_category": category,
        "is_landlord": landlord,
        "is_property_owner": owner,
        "is_occupation": occupation,
        "occupation_description": "Solicitor" if occupation else "",
        "registration_status": "registered",
        "interest_count": 1,
        "year_declared": year,
    }


_DAIL_INTERESTS = pl.DataFrame(
    [
        _interest_row(
            "Mary-Murphy.D.2020-02-08",
            "Mary",
            "Murphy",
            "Fianna Fáil",
            "Dublin South",
            "Land (including property)",
            "Rental property in Dublin 6",
            2024,
            landlord=True,
            owner=True,
        ),
        _interest_row(
            "Mary-Murphy.D.2020-02-08",
            "Mary",
            "Murphy",
            "Fianna Fáil",
            "Dublin South",
            "Shares",
            "Shares in Acme plc",
            2024,
        ),
        _interest_row(
            "Mary-Murphy.D.2020-02-08",
            "Mary",
            "Murphy",
            "Fianna Fáil",
            "Dublin South",
            "Directorships",
            "Director of Murphy Holdings Ltd",
            2024,
        ),
        _interest_row(
            "Mary-Murphy.D.2020-02-08",
            "Mary",
            "Murphy",
            "Fianna Fáil",
            "Dublin South",
            "Occupations",
            "Practising solicitor",
            2023,
            occupation=True,
        ),
        _interest_row(
            "Sean-OBrien.D.2016-02-26",
            "Sean",
            "O'Brien",
            "Sinn Féin",
            "Cork North-West",
            "Land (including property)",
            "Family farm in Cork",
            2024,
            owner=True,
        ),
        _interest_row(
            "Sean-OBrien.D.2016-02-26",
            "Sean",
            "O'Brien",
            "Sinn Féin",
            "Cork North-West",
            "Shares",
            "Shares in Beta Energy",
            2024,
        ),
        _interest_row(
            "Sean-OBrien.D.2016-02-26",
            "Sean",
            "O'Brien",
            "Sinn Féin",
            "Cork North-West",
            "Gifts",
            "Hospitality at conference",
            2023,
        ),
    ],
    schema_overrides=_INTERESTS_SCHEMA,
)

_SEANAD_INTERESTS = pl.DataFrame(
    [
        _interest_row(
            "Aoife-NiBhroin.S.2024-11-29",
            "Aoife",
            "Ní Bhroin",
            "Green Party",
            "Agricultural Panel",
            "Land (including property)",
            "Holiday home in Galway",
            2024,
            owner=True,
        ),
        _interest_row(
            "Aoife-NiBhroin.S.2024-11-29",
            "Aoife",
            "Ní Bhroin",
            "Green Party",
            "Agricultural Panel",
            "Shares",
            "Shares in Gamma Renewables",
            2024,
        ),
    ],
    schema_overrides=_INTERESTS_SCHEMA,
)

# ---------------------------------------------------------------------------
# committee_assignments.parquet + office_holders.parquet — drive the 4 committee
# views. Each committee needs exactly one is_chair=True row so chair_name resolves.
# ---------------------------------------------------------------------------


def _assign_row(name, party, constituency, committee, role, is_chair):
    return {
        "chamber": "Dáil",
        "name": name,
        "party": party,
        "constituency": constituency,
        "dail_number": 34,
        "committee": committee,
        "committee_url": f"https://www.oireachtas.ie/en/committees/34/{committee.lower().replace(' ', '-')}/",
        "type": "Standing",
        "status": "active",
        "role": role,
        "is_chair": is_chair,
        "start": datetime(2024, 12, 18, 0, 0, 0),
        "end": None,
    }


COMMITTEE_ASSIGNMENTS = pl.DataFrame(
    [
        _assign_row("Mary Murphy", "Fianna Fáil", "Dublin South", "Committee on Finance", "Chair", True),
        _assign_row("Sean O'Brien", "Sinn Féin", "Cork North-West", "Committee on Finance", "Member", False),
        _assign_row("John Walsh", "Fine Gael", "Dublin Rathdown", "Committee on Finance", "Member", False),
        _assign_row("Sean O'Brien", "Sinn Féin", "Cork North-West", "Committee on Health", "Chair", True),
        _assign_row("Aoife Ní Bhroin", "Green Party", "Galway West", "Committee on Health", "Member", False),
        _assign_row("Mary Murphy", "Fianna Fáil", "Dublin South", "Committee on Health", "Member", False),
    ],
    schema_overrides={"dail_number": pl.Int32, "start": pl.Datetime, "end": pl.Datetime},
)

COMMITTEE_OFFICES = pl.DataFrame(
    {
        "chamber": ["Dáil", "Dáil"],
        "name": ["Mary Murphy", "John Walsh"],
        "party": ["Fianna Fáil", "Fine Gael"],
        "office": ["Ceann Comhairle", "Leas-Cheann Comhairle"],
        "start": [datetime(2024, 12, 18), datetime(2024, 12, 18)],
        "end": [None, None],
    },
    schema_overrides={"start": pl.Datetime, "end": pl.Datetime},
)

# ---------------------------------------------------------------------------
# questions.parquet — drives 6 member-question views. v_member_question_focus_shift
# needs ≥30 questions in BOTH the pre-2024-11-29 ("past") and post ("recent")
# windows for one member, with a DIFFERENT top ministry in each, to emit a row.
# Mary gets 30 past (Health) + 30 recent (Justice); Sean gets a small spread.
# ---------------------------------------------------------------------------

_QUESTIONS_COLS = [
    "context_date",
    "td_name",
    "unique_member_code",
    "question_date",
    "question_number",
    "house_no",
    "question.house.houseCode",
    "house",
    "question_type",
    "debate_section_id",
    "topic",
    "ministry",
    "question_text",
    "question_ref",
    "year",
]


def _q_row(code, name, dt: datetime, ministry, topic, n):
    ds = dt.strftime("%Y-%m-%d")
    return {
        "context_date": ds,
        "td_name": name,
        "unique_member_code": code,
        "question_date": dt,
        "question_number": n,
        "house_no": "34",
        "question.house.houseCode": "dail",
        "house": "Dáil",
        "question_type": "written" if n % 2 else "oral",
        "debate_section_id": f"sec-{ds}-{(n % 3) + 1}",
        "topic": topic,
        "ministry": ministry,
        "question_text": f"To ask the Minister for {ministry} about {topic}. [{n}/{dt.year % 100}]",
        "question_ref": f"{n}/{dt.year % 100}",
        "year": dt.year,
    }


def _build_questions() -> pl.DataFrame:
    rows = []
    n = 1000
    # Mary: 30 past (Health, 2023), 30 recent (Justice, 2025) → focus-shift row.
    for i in range(30):
        rows.append(
            _q_row(
                "Mary-Murphy.D.2020-02-08",
                "Mary Murphy",
                datetime(2023, 1, 10) + _days(i * 3),
                "Health",
                "Hospital waiting lists",
                n,
            )
        )
        n += 1
    for i in range(30):
        rows.append(
            _q_row(
                "Mary-Murphy.D.2020-02-08",
                "Mary Murphy",
                datetime(2025, 1, 10) + _days(i * 3),
                "Justice",
                "Garda staffing levels",
                n,
            )
        )
        n += 1
    # Sean: a smaller mixed spread across both windows.
    for i in range(6):
        rows.append(
            _q_row(
                "Sean-OBrien.D.2016-02-26",
                "Sean O'Brien",
                datetime(2023, 3, 1) + _days(i * 5),
                "Housing",
                "Social housing delivery",
                n,
            )
        )
        n += 1
    for i in range(4):
        rows.append(
            _q_row(
                "Sean-OBrien.D.2016-02-26",
                "Sean O'Brien",
                datetime(2025, 3, 1) + _days(i * 5),
                "Housing",
                "Rent pressure zones",
                n,
            )
        )
        n += 1
    return pl.DataFrame(
        rows,
        schema_overrides={
            "question_date": pl.Datetime,
            "question_number": pl.Int64,
            "year": pl.Int32,
        },
    ).select(_QUESTIONS_COLS)


def _days(n: int):
    from datetime import timedelta

    return timedelta(days=n)


# ---------------------------------------------------------------------------
# statutory_instruments.parquet — base for v_statutory_instruments.
# si_current_state.parquet — drives v_si_current_state and the LEFT-JOIN that
# adds legal-state to v_statutory_instruments. Three SIs:
#   2024-001  revoked (whole)        → has a state row
#   2024-002  in force as made       → has a state row
#   2024-003  not in the directory   → NO state row, exercises the LEFT-JOIN
#                                       NULL path ("status not checked")
# Joins on si_id; the state table covers 2 of the 3 base SIs so the contract
# test can assert both the matched and the unchecked (NULL) branches.
# ---------------------------------------------------------------------------

STATUTORY_INSTRUMENTS = pl.DataFrame(
    {
        "si_id": ["2024-001", "2024-002", "2024-003"],
        "si_year": [2024, 2024, 2024],
        "si_number": [1, 2, 3],
        "si_title": [
            "Sea-Fisheries (Quota Management) Regulations 2024",
            "Health (Pricing) (Amendment) Regulations 2024",
            "Local Government (Boundary) Order 2024",
        ],
        "si_signed_date": [date(2024, 1, 12), date(2024, 1, 18), date(2024, 2, 3)],
        "si_operation": ["amends", "amends", "establishes"],
        "si_operation_flags": ["amends|commences", "amends", "establishes"],
        "si_form": ["regulations", "regulations", "order"],
        "si_eu_relationship": ["gives_effect", "none_detected", "none_detected"],
        "si_is_eu": [True, False, False],
        "si_policy_domain": ["Agriculture", "Health", "Local Government"],
        "si_policy_domains_all": ["Agriculture|Marine", "Health", "Local Government"],
        "si_responsible_actor": [
            "Minister for Agriculture, Food and the Marine",
            "Minister for Health",
            "Minister for Housing, Local Government and Heritage",
        ],
        "si_signatory_name": ["Mary Murphy", None, None],
        "si_department": ["agriculture", "health", "housing"],
        "si_department_label": [
            "Agriculture, Food and the Marine",
            "Health",
            "Housing, Local Government and Heritage",
        ],
        "si_minister_member_code": ["Mary-Murphy.D.2020-02-08", None, None],
        "si_minister_name": ["Mary Murphy", "Sean O'Brien", None],
        "si_parent_legislation": ["Sea-Fisheries Act 2006", "Health Act 2013", None],
        "bill_id": ["bill-101-of-2023", None, None],
        "bill_short_title": ["Sea-Fisheries Bill 2023", None, None],
        "eisb_url": [
            "https://www.irishstatutebook.ie/eli/2024/si/1/made/en/html",
            "https://www.irishstatutebook.ie/eli/2024/si/2/made/en/html",
            "https://www.irishstatutebook.ie/eli/2024/si/3/made/en/html",
        ],
        "iris_source_pdf": ["iris_2024_002.pdf", "iris_2024_004.pdf", "iris_2024_009.pdf"],
        "si_taxonomy_confidence": [0.91, 0.84, 0.77],
    },
    schema_overrides={"si_year": pl.Int64, "si_number": pl.Int64, "si_taxonomy_confidence": pl.Float64},
)

SI_CURRENT_STATE = pl.DataFrame(
    {
        "si_id": ["2024-001", "2024-002"],
        "si_year": [2024, 2024],
        "si_number": [1, 2],
        "directory_title": [
            "Sea-Fisheries (Quota Management) Regulations 2024",
            "Health (Pricing) (Amendment) Regulations 2024",
        ],
        "current_state": ["revoked", "in_force_as_made"],
        "affecting_sis": [["318/2025"], []],
        "affecting_si_urls": [
            ["https://www.irishstatutebook.ie/eli/2025/si/318/made/en/html"],
            [],
        ],
        "this_si_eli_url": [
            "https://www.irishstatutebook.ie/eli/2024/si/1/made/en/html",
            "https://www.irishstatutebook.ie/eli/2024/si/2/made/en/html",
        ],
        "how_affected_raw": [
            "Revoked on 1 September 2025 || S.I. No. 318 of 2025 , reg. 9",
            "Not affected",
        ],
        "state_source": ["eISB Legislation Directory", "eISB Legislation Directory"],
        "state_source_url": [
            "https://www.irishstatutebook.ie/isbc/si2024_1-50.html",
            "https://www.irishstatutebook.ie/isbc/si2024_1-50.html",
        ],
        "directory_updated_to": ["29 May 2026", "29 May 2026"],
        "confidence": [0.85, 0.95],
    },
    schema_overrides={
        "si_year": pl.Int64,
        "si_number": pl.Int64,
        "affecting_sis": pl.List(pl.String),
        "affecting_si_urls": pl.List(pl.String),
        "confidence": pl.Float64,
    },
)


# ---------------------------------------------------------------------------
# procurement_awards.parquet — base for v_procurement_awards and the supplier/
# authority/cpv summaries. Source: pipeline_sandbox/procurement_etenders_extract.py.
# The rows are chosen to exercise the VALUE-IS-NOT-SPEND semantics and the privacy/
# quality filters, so the value tests can assert exact aggregates:
#   - Acme Construction Ltd: 2 clean standalone awards, 2 authorities → summable
#   - Bigco Services Ltd:     a framework/DPS CEILING → counted, NEVER summed
#   - Sharedco A/B Ltd:       2 suppliers share ONE tender → value repeated, not summable
#   - Nullid Co Ltd:          NULL Tender ID → sharing unverifiable, not summable (the
#                             2026-06-03 fix: literal "NULL" is now a real null)
#   - Joe Murphy:             sole_trader_or_individual → excluded from supplier ranking
#   - eloitte Ireland Ltd:    name_truncated (dropped leading capital) → excluded from ranking
#   - Mason & Sons Ltd:       name with '&' survives WHOLE (the entity-split fix) — one row,
#                             not fragmented into "Mason" + "Sons Ltd"
#   - Lobbyco Ltd:            on the lobbying register (see overlap fixture)
# ---------------------------------------------------------------------------

_AWARDS_COLS = [
    "Tender ID", "supplier", "supplier_norm", "supplier_class", "name_truncated",
    "Contracting Authority", "Main Cpv Code", "Main Cpv Code Description",
    "Competition Type", "Notice Published Date/Contract Created Date",
    "value_eur", "value_kind", "is_framework_or_dps",
    "value_shared_across_suppliers", "value_safe_to_sum",
]


def _award(tender, supplier, norm, cls, trunc, auth, cpv, cpv_desc, comp, date_str,
           value, kind, fw, shared, safe):
    return dict(zip(_AWARDS_COLS, [
        tender, supplier, norm, cls, trunc, auth, cpv, cpv_desc, comp, date_str,
        value, kind, fw, shared, safe,
    ], strict=True))


PROCUREMENT_AWARDS = pl.DataFrame(
    [
        _award("T001", "Acme Construction Ltd", "acmeconstructionltd", "company", False,
               "Dublin City Council", "45000000", "Construction work", "Open", "01/03/2023",
               100000.0, "contract_award_value", False, False, True),
        _award("T002", "Acme Construction Ltd", "acmeconstructionltd", "company", False,
               "Cork County Council", "45000000", "Construction work", "Open", "15/06/2023",
               200000.0, "contract_award_value", False, False, True),
        _award("T003", "Bigco Services Ltd", "bigcoservicesltd", "company", False,
               "Health Service Executive", "79000000", "Business services", "Framework", "20/01/2024",
               5000000.0, "framework_or_dps_ceiling", True, False, False),
        _award("T004", "Sharedco A Ltd", "sharedcoaltd", "company", False,
               "Office of Public Works (OPW)", "71000000", "Architectural services", "Open", "10/10/2022",
               1000000.0, "contract_award_value", False, True, False),
        _award("T004", "Sharedco B Ltd", "sharedcobltd", "company", False,
               "Office of Public Works (OPW)", "71000000", "Architectural services", "Open", "10/10/2022",
               1000000.0, "contract_award_value", False, True, False),
        _award(None, "Nullid Co Ltd", "nullidcoltd", "company", False,
               "Revenue Commissioners", "48000000", "Software", "Open", "05/05/2021",
               999999.0, "contract_award_value", False, False, False),
        _award("T005", "Joe Murphy", "joemurphy", "sole_trader_or_individual", False,
               "Mayo County Council", "60000000", "Transport services", "Open", "01/01/2023",
               50000.0, "contract_award_value", False, False, True),
        _award("T006", "eloitte Ireland Ltd", "eloittetruncnorm", "company", True,
               "Department of Finance", "79000000", "Business services", "Open", "02/02/2023",
               75000.0, "contract_award_value", False, False, True),
        _award("T007", "Lobbyco Ltd", "lobbycoltd", "company", False,
               "Dublin City Council", "79000000", "Business services", "Open", "03/03/2024",
               400000.0, "contract_award_value", False, False, True),
        _award("T008", "Mason & Sons Ltd", "masonsonsltd", "company", False,
               "Galway City Council", "45000000", "Construction work", "Open", "04/04/2023",
               150000.0, "contract_award_value", False, False, True),
    ],
    schema_overrides={
        "Tender ID": pl.String,
        "value_eur": pl.Float64,
        "name_truncated": pl.Boolean,
        "is_framework_or_dps": pl.Boolean,
        "value_shared_across_suppliers": pl.Boolean,
        "value_safe_to_sum": pl.Boolean,
    },
).select(_AWARDS_COLS)

# ---------------------------------------------------------------------------
# procurement_supplier_cro_match.parquet — exact normalised-name → CRO register.
# Covers 3 of the company-class suppliers; the rest LEFT-JOIN to NULL company_num.
# ---------------------------------------------------------------------------

PROCUREMENT_CRO_MATCH = pl.DataFrame(
    {
        "supplier": ["Acme Construction Ltd", "Bigco Services Ltd", "Lobbyco Ltd"],
        "supplier_norm": ["acmeconstructionltd", "bigcoservicesltd", "lobbycoltd"],
        "n_cro": [1, 1, 1],
        "company_num": [123456, 234567, 345678],
        "company_status": ["Normal", "Dissolved", "Normal"],
        "match_method": ["exact_unique", "exact_unique", "exact_unique"],
        "match_confidence": [0.9, 0.9, 0.9],
    },
    schema_overrides={"n_cro": pl.UInt32, "company_num": pl.Int64, "match_confidence": pl.Float64},
)

# ---------------------------------------------------------------------------
# procurement_lobbying_overlap.parquet — suppliers also on the lobbying register.
# Lobbyco appears under TWO lobby_name variants (registrant + client) keyed to the
# SAME supplier_norm — the variant-key duplication (anomaly #3): summing
# awarded_value_safe_eur across ROWS double-counts (2×400000), so the registrant-only
# v_lobbying_org_procurement and the supplier-summary ov CTE must dedup/group correctly.
# ---------------------------------------------------------------------------

PROCUREMENT_LOBBYING_OVERLAP = pl.DataFrame(
    {
        "lobby_name": ["Lobbyco Limited", "LOBBYCO LTD"],
        "lobby_side": ["registrant", "client"],
        "supplier": ["Lobbyco Ltd", "Lobbyco Ltd"],
        "supplier_norm": ["lobbycoltd", "lobbycoltd"],
        "n_lobby_returns": [5, 3],
        "n_award_rows": [1, 1],
        "n_authorities": [1, 1],
        "awarded_value_safe_eur": [400000.0, 400000.0],
    },
    schema_overrides={
        "n_lobby_returns": pl.UInt32,
        "n_award_rows": pl.UInt32,
        "n_authorities": pl.UInt32,
        "awarded_value_safe_eur": pl.Float64,
    },
)


def main() -> None:
    SILVER_PQ.mkdir(parents=True, exist_ok=True)
    GOLD_PQ.mkdir(parents=True, exist_ok=True)

    members_path = SILVER_PQ / "flattened_members.parquet"
    votes_path = GOLD_PQ / "pretty_votes.parquet"
    ext_links_path = SILVER_PQ / "member_external_links.parquet"

    FLATTENED_MEMBERS.write_parquet(members_path, compression="zstd", compression_level=3, statistics=True)
    PRETTY_VOTES.write_parquet(votes_path, compression="zstd", compression_level=3, statistics=True)
    MEMBER_EXTERNAL_LINKS.write_parquet(ext_links_path, compression="zstd", compression_level=3, statistics=True)

    print(f"Wrote {members_path} ({FLATTENED_MEMBERS.height} rows)")
    print(f"Wrote {votes_path} ({PRETTY_VOTES.height} rows)")
    print(f"Wrote {ext_links_path} ({MEMBER_EXTERNAL_LINKS.height} rows)")

    # Hardcoded-'data/'-path view fixtures, mirroring the real project layout.
    questions = _build_questions()
    hardcoded = [
        (PUBLIC_APPOINTMENTS, "gold/parquet/public_appointments.parquet"),
        (EC_CONSTITUENCY_POP, "gold/parquet/ec_constituency_pop_2022.parquet"),
        (_DAIL_INTERESTS, "silver/parquet/dail_member_interests_combined.parquet"),
        (_SEANAD_INTERESTS, "silver/parquet/seanad_member_interests_combined.parquet"),
        (COMMITTEE_ASSIGNMENTS, "silver/committees/committee_assignments.parquet"),
        (COMMITTEE_OFFICES, "silver/committees/office_holders.parquet"),
        (questions, "silver/parquet/questions.parquet"),
        (STATUTORY_INSTRUMENTS, "gold/parquet/statutory_instruments.parquet"),
        (SI_CURRENT_STATE, "gold/parquet/si_current_state.parquet"),
        (PROCUREMENT_AWARDS, "gold/parquet/procurement_awards.parquet"),
        (PROCUREMENT_CRO_MATCH, "gold/parquet/procurement_supplier_cro_match.parquet"),
        (PROCUREMENT_LOBBYING_OVERLAP, "gold/parquet/procurement_lobbying_overlap.parquet"),
    ]
    for df, rel in hardcoded:
        path = _wp(df, rel)
        print(f"Wrote {path} ({df.height} rows)")


if __name__ == "__main__":
    main()
