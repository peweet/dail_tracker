"""
Dáil Tracker — central configuration.

Domain reference data, shared member lists, and project-level data paths.
Import from here; do not define these constants in page or data-access files.
"""
from __future__ import annotations

from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────────────────

PROJECT_ROOT       = Path(__file__).resolve().parent.parent
SILVER_DIR         = PROJECT_ROOT / "data" / "silver"
SILVER_PARQUET_DIR = PROJECT_ROOT / "data" / "silver" / "parquet"
GOLD_PARQUET_DIR   = PROJECT_ROOT / "data" / "gold"   / "parquet"

SILVER_INTERESTS_CSV: dict[str, Path] = {
    "Dáil":   SILVER_DIR / "dail_member_interests_combined.csv",
    "Seanad": SILVER_DIR / "seanad_member_interests_combined.csv",
}

SILVER_MEMBERS_CSV: dict[str, Path] = {
    "Dáil":   SILVER_DIR / "flattened_members.csv",
    "Seanad": SILVER_DIR / "flattened_seanad_members.csv",
}

GOLD_VOTE_HISTORY_PARQUET      = GOLD_PARQUET_DIR / "current_dail_vote_history.parquet"
GOLD_INTERESTS_RANKING_PARQUET = GOLD_PARQUET_DIR / "interests_member_ranking.parquet"

# ── Notable members ─────────────────────────────────────────────────────────────
# Quick-select sidebar chips on member-level pages.
# render_notable_chips() already filters this list to members present in the dataset,
# so it is safe to keep the full union here.

NOTABLE_TDS: list[str] = [
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
    "Leo Varadkar",
    "Pearse Doherty",
    "Eamon Ryan",
    "Michael Healy-Rae",
    "Danny Healy-Rae",
    "Michael Collins",
    "Michael Lowry",
    "Marian Harkin",
    "Holly Cairns",
    "Robert Troy",
    "Pauline Tully",
]

NOTABLE_SENATORS: list[str] = []

# ── Attendance ──────────────────────────────────────────────────────────────────
# Official plenary sitting-day counts from the Houses of the Oireachtas Commission
# annual reports. The se are fixed historical facts; pipeline routing is
# disproportionate for values that change at most once per year.

SITTING_DAYS_BY_YEAR: dict[int, int] = {
    2020: 82,
    2021: 94,
    2022: 106,
    2023: 100,
    2024: 83,
    2025: 82,
}

# ── Payments / TAA ──────────────────────────────────────────────────────────────
# TAA = Travel and Accommodation Allowance, a component of the Parliamentary
# Standard Allowance. Band distances and deduction rules are defined by
# Oireachtas Standing Orders.

TAA_BAND_TABLE = """\
| Band | Distance from Leinster House |
|---|---|
| Dublin | Under 25 km — no Travel & Accommodation Allowance |
| Band 1 | 25–60 km |
| Band 2 | 60–80 km |
| Band 3 | 80–100 km |
| Band 4 | 100–130 km |
| Band 5 | 130–160 km |
| Band 6 | 160–190 km |
| Band 7 | 190–210 km |
| Band 8 | Over 210 km — highest TAA rate |
"""

TAA_DEDUCTIONS_NOTE = (
    "PSA payments are linked to attendance. Under Oireachtas rules, members must attend "
    "a minimum of **120 sitting days per year** to receive the full TAA. For each day "
    "below that threshold, **1% of the annual TAA is deducted**. Certain absences are "
    "excused — committee work, official duties abroad, certified ill-health — so a lower "
    "TAA does not necessarily mean a member was absent."
)

# ── Legislation ─────────────────────────────────────────────────────────────────
# Stage numbers are defined by the Oireachtas legislative process.
# Dáil: stages 1–5.  Seanad: stages 6–10.  Enacted: stage 11+.

BILL_STAGE_SEANAD_MIN  = 6
BILL_STAGE_ENACTED_MIN = 11

BILL_STATUS_CSS: dict[str, str] = {
    "enacted":   "leg-status-enacted",
    "signed":    "leg-status-enacted",
    "lapsed":    "leg-status-lapsed",
    "withdrawn": "leg-status-withdrawn",
    "defeated":  "leg-status-lapsed",
}

# ── Register of Members' Interests ──────────────────────────────────────────────
# Canonical category order and display labels as published in the Oireachtas register.

INTEREST_CATEGORY_ORDER: list[str] = [
    "Occupations",
    "Directorships",
    "Remunerated Position",
    "Shares",
    "Land (including property)",
    "Contracts",
    "Gifts",
    "Travel Facilities",
    "Property supplied or lent or a Service supplied",
]

INTEREST_CATEGORY_LABELS: dict[str, str] = {
    "Occupations":                                       "Occupations & Employment",
    "Directorships":                                     "Directorships & Company Roles",
    "Remunerated Position":                              "Remunerated Positions",
    "Shares":                                            "Shareholdings",
    "Land (including property)":                         "Land & Property",
    "Contracts":                                         "Contracts",
    "Gifts":                                             "Gifts Received",
    "Travel Facilities":                                 "Travel Facilities",
    "Property supplied or lent or a Service supplied":   "Property or Services Supplied",
}

# ── Committees ──────────────────────────────────────────────────────────────────
# Canonical committee type taxonomy used for filtering and display.

COMMITTEE_TYPES: dict[str, str] = {
    "Policy":                                                    "Policy",
    "Oversight":                                                 "Oversight",
    "Statutory":                                                 "Statutory",
    "Shadow Department":                                         "Shadow Department",
    "Parliamentary Regulation and Reform":                       "Parl. Regulation & Reform",
    "The Committee System and Parliamentary Administration":     "Parl. Administration",
    "Parliamentary Business and Committee Membership":           "Parl. Business",
}
