"""Participation & absence pipeline — the honest replacement for the censored
TAA "attendance count".

The TAA sitting-day count censors at the 120-day allowance threshold and is
reconcilable, so it cannot measure chamber participation (see
doc/ATTENDANCE_PARTICIPATION_REDESIGN.md). This extractor derives, per
(member, house, year), four verifiable signals from data that already exists:

  1. division_participation — votes voted / missed / turnout% (unfakeable: a row
     exists per member-division, absence = no row).
  2. absence_gaps — longest run of consecutive PLENARY SITTING DAYS a member was
     PHYSICALLY absent (not recorded present at Leinster House at all) — from the
     TAA attendance PDFs, NOT the votes. The vote-gap conflated absence with
     present-but-not-voting (pairing); physical presence disambiguates. Recess-proof.
  3. divergence — TAA days present (from attendance gold) vs votes cast. The
     headline "badged in, didn't vote" number.
  4. taa_compliance — below-120 cohort + 1%/day allowance deduction (the money).

Plus role flags (Ceann Comhairle / Leas-Cheann Comhairle / Cathaoirleach /
minister / party leader) so structurally-low voters are CONTEXTUALISED, never
shamed; and a code-keyed news lookup so a publicly-explained absence (illness,
maternity) is vindicated with a sourced link.

Scope: CURRENT TERM ONLY (year >= 2025). Earlier years are survivor-biased in the
vote source (current members only) and 2024 spans a dissolution — both unsafe to
rank (proven in the stress test).

Polars base pipeline. Writes gold parquet; the sql_views read them thin.
Run:  python -m extractors.participation_extract
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold" / "parquet"
META = ROOT / "data" / "_meta"

DAIL_VOTES = GOLD / "current_dail_vote_history.parquet"
SEANAD_VOTES = GOLD / "current_seanad_vote_history.parquet"
DAIL_ATT = GOLD / "attendance_by_td_year.parquet"
SEANAD_ATT = GOLD / "seanad_attendance_by_year.parquet"
DAIL_MEMBERS = ROOT / "data" / "silver" / "parquet" / "flattened_members.parquet"
SEANAD_MEMBERS = ROOT / "data" / "silver" / "parquet" / "flattened_seanad_members.parquet"
# Daily badge-in dates from the TAA attendance PDFs — the basis for PHYSICAL absence.
DAIL_ATT_FACT = ROOT / "data" / "silver" / "parquet" / "td_attendance_fact_table.parquet"
SEANAD_ATT_FACT = ROOT / "data" / "silver" / "parquet" / "seanad_attendance_fact_table.parquet"
LEADERS_CSV = META / "oireachtas_special_roles.csv"
EXPLANATIONS_CSV = META / "member_absence_explanations.csv"

_CHAIR_OFFICES = {"Ceann Comhairle", "Leas-Cheann Comhairle", "Cathaoirleach", "Leas-Chathaoirleach"}

CURRENT_TERM_FROM_YEAR = 2025  # 34th Dáil / 27th Seanad
TAA_MINIMUM_DAYS = 120
TAA_BASIS_DAYS = 150


# ── load ────────────────────────────────────────────────────────────────────


def _votes(path: Path, house: str) -> pl.DataFrame:
    df = pl.read_parquet(path).with_columns(
        d=pl.col("date").str.to_date(strict=False), house=pl.lit(house)
    )
    df = df.filter(
        pl.col("full_name").is_not_null()
        & pl.col("unique_member_code").is_not_null()
        & pl.col("d").is_not_null()
    ).with_columns(year=pl.col("d").dt.year())
    return df.filter(pl.col("year") >= CURRENT_TERM_FROM_YEAR)


def _attendance(path: Path, house: str) -> pl.DataFrame:
    df = pl.read_parquet(path)
    if "house" not in df.columns:
        df = df.with_columns(house=pl.lit(house))
    # is_minister arrives as bool or 'true'/'false' text across the two sources.
    return df.with_columns(
        is_minister=pl.col("is_minister").cast(pl.Utf8).str.to_lowercase() == "true",
        year=pl.col("year").cast(pl.Int64),
    ).filter(pl.col("year") >= CURRENT_TERM_FROM_YEAR)


def _office_flags() -> pl.DataFrame:
    """Reliable current-office flags per member, DERIVED from the Oireachtas member
    feed (flattened_members) — NOT the TAA gold's `is_minister`, which is false for
    the Taoiseach/Tánaiste. A "current office" is an office slot with no end_date;
    those cleanly cover Taoiseach/Ministers/Ministers of State AND the chair roles
    (Ceann Comhairle / Leas-Cheann Comhairle / Cathaoirleach)."""
    frames = []
    for path in (DAIL_MEMBERS, SEANAD_MEMBERS):
        if not path.exists():
            continue
        df = pl.read_parquet(path)
        for n in range(1, 7):
            nm, ed = f"office_{n}_name", f"office_{n}_end_date"
            if nm not in df.columns:
                continue
            slot = df.select(
                "unique_member_code",
                pl.col(nm).alias("office_name"),
                (pl.col(ed) if ed in df.columns else pl.lit(None)).cast(pl.Utf8).alias("end_date"),
            ).filter(
                pl.col("office_name").is_not_null()
                & (pl.col("end_date").is_null() | (pl.col("end_date") == ""))
            )
            frames.append(slot)
    if not frames:
        return pl.DataFrame(schema={"unique_member_code": pl.Utf8, "office_name": pl.Utf8,
                                    "holds_office": pl.Boolean, "is_minister": pl.Boolean, "is_chair": pl.Boolean})
    offices = pl.concat(frames).with_columns(
        is_chair=pl.col("office_name").is_in(list(_CHAIR_OFFICES)),
        is_minister=(
            pl.col("office_name").str.starts_with("Minister")
            | pl.col("office_name").is_in(["Taoiseach", "Tánaiste"])
        ),
    )
    # one row per member: holds_office, the most senior office name, the flags
    return offices.group_by("unique_member_code").agg(
        holds_office=pl.lit(True),  # noqa: FBT003
        is_minister=pl.col("is_minister").any(),
        is_chair=pl.col("is_chair").any(),
        # prefer a minister/chair title for the displayed note
        office_name=pl.col("office_name").sort_by(pl.col("is_minister") | pl.col("is_chair"), descending=True).first(),
    )


def _leaders() -> pl.DataFrame:
    """Party leaders are NOT in the office feed (they hold no government office) yet
    vote less via pairing — curated, small, with a sourced note column."""
    r = pl.read_csv(LEADERS_CSV).filter(pl.col("role") == "party_leader")
    return r.select("full_name", "house", pl.lit(True).alias("is_leader"), pl.col("note").alias("leader_note"))


# ── 1. division participation ────────────────────────────────────────────────


def build_participation(votes: pl.DataFrame, office: pl.DataFrame, leaders: pl.DataFrame) -> pl.DataFrame:
    totals = votes.group_by(["house", "year"]).agg(total_divisions=pl.col("vote_id").n_unique())
    per = votes.group_by(["house", "year", "unique_member_code", "full_name", "party"]).agg(
        voted_in=pl.col("vote_id").n_unique(),
        abstentions=(pl.col("vote_type") == "Abstained").sum(),
        first_vote=pl.col("d").min(),
        last_vote=pl.col("d").max(),
    )
    out = per.join(totals, on=["house", "year"], how="left").with_columns(
        missed=(pl.col("total_divisions") - pl.col("voted_in")),
        turnout_pct=(100.0 * pl.col("voted_in") / pl.col("total_divisions")).round(1),
    )
    out = out.join(office, on="unique_member_code", how="left").join(
        leaders, on=["full_name", "house"], how="left"
    )
    return out.with_columns(
        holds_office=pl.col("holds_office").fill_null(False),  # noqa: FBT003
        is_minister=pl.col("is_minister").fill_null(False),  # noqa: FBT003
        is_chair=pl.col("is_chair").fill_null(False),  # noqa: FBT003
        is_leader=pl.col("is_leader").fill_null(False),  # noqa: FBT003
    ).with_columns(
        role=pl.when(pl.col("is_chair")).then(pl.lit("chair"))
        .when(pl.col("is_minister")).then(pl.lit("minister"))
        .when(pl.col("is_leader")).then(pl.lit("party_leader"))
        .otherwise(pl.lit("")),
        role_note=pl.coalesce(pl.col("office_name"), pl.col("leader_note"), pl.lit("")),
    ).drop(["office_name", "leader_note"])


# ── 2. absence gaps (PHYSICAL absence, from the TAA attendance PDFs) ──────────


def _attendance_dates() -> pl.DataFrame:
    """Per-member badge-in DATES from the TAA attendance PDFs (silver fact), melted to
    one row per (identifier, house, year, present_date, is_plenary). ``is_plenary`` marks
    a chamber sitting day; the other rows are committee / non-sitting days. Current term."""
    frames = []
    for path, house in ((DAIL_ATT_FACT, "Dáil"), (SEANAD_ATT_FACT, "Seanad")):
        if not path.exists():
            continue
        df = pl.read_parquet(path).with_columns(year=pl.col("year").cast(pl.Int64))
        sitting = df.filter(pl.col("iso_sitting_days_attendance").is_not_null()).select(
            "identifier", "year",
            present_date=pl.col("iso_sitting_days_attendance").str.to_date(strict=False),
            is_plenary=pl.lit(True),  # noqa: FBT003
        )
        other = df.filter(pl.col("iso_other_days_attendance").is_not_null()).select(
            "identifier", "year",
            present_date=pl.col("iso_other_days_attendance").str.to_date(strict=False),
            is_plenary=pl.lit(False),  # noqa: FBT003
        )
        frames.append(pl.concat([sitting, other]).with_columns(house=pl.lit(house)))
    out = pl.concat(frames).filter(pl.col("year") >= CURRENT_TERM_FROM_YEAR)
    return out.filter(pl.col("present_date").is_not_null()).unique(
        ["identifier", "house", "year", "present_date", "is_plenary"]
    )


def build_absence_gaps(att_dates: pl.DataFrame, code_map: pl.DataFrame) -> pl.DataFrame:
    """Longest run of consecutive PLENARY SITTING DAYS a member was PHYSICALLY absent
    from Leinster House — from the TAA attendance PDFs, NOT the votes.

    "Notable absence" = the chamber sat in plenary and the member was not recorded
    present at all that day (no sitting AND no committee badge-in). Measured against the
    distinct plenary sitting dates, so it is recess-proof (no plenary sittings in recess).
    INTERIOR runs only (bracketed by two days the member WAS present) → always real, never
    the 120-day TAA trailing-censoring. This is what "absence" means: the old vote-gap
    conflated it with present-but-not-voting (pairing) — a member could be in the building
    every day yet show a vote-gap. See doc/ATTENDANCE_PARTICIPATION_REDESIGN.md."""
    cal = (
        att_dates.filter(pl.col("is_plenary"))
        .select("house", "year", "present_date").unique()
        .sort(["house", "year", "present_date"])
        .with_columns(idx=pl.int_range(pl.len()).over(["house", "year"]))
        .rename({"present_date": "sit_date"})
    )
    # present on a sitting date = badged in at all that day (sitting OR committee)
    present = att_dates.select("identifier", "house", "year", "present_date").unique()
    attended = present.join(
        cal, left_on=["house", "year", "present_date"], right_on=["house", "year", "sit_date"], how="inner"
    ).sort(["identifier", "house", "year", "present_date"])
    grp = ["identifier", "house", "year"]
    attended = attended.with_columns(
        prev_idx=pl.col("idx").shift(1).over(grp),
        prev_date=pl.col("present_date").shift(1).over(grp),
    ).with_columns(
        sitting_days_missed=(pl.col("idx") - pl.col("prev_idx") - 1),
        gap_calendar_days=(pl.col("present_date") - pl.col("prev_date")).dt.total_days(),
    )
    gaps = attended.filter(pl.col("sitting_days_missed").is_not_null())
    longest = (
        gaps.sort("sitting_days_missed", descending=True)
        .group_by(grp)
        .agg(
            longest_run_sitting_days=pl.col("sitting_days_missed").first(),
            run_calendar_days=pl.col("gap_calendar_days").first(),
            run_start=pl.col("prev_date").first(),
            run_end=pl.col("present_date").first(),
        )
    )
    # attach unique_member_code + full_name via the gold roster (member_id == identifier)
    out = longest.join(code_map, on=["identifier", "house", "year"], how="left").with_columns(
        longest_run_sitting_days=pl.col("longest_run_sitting_days").fill_null(0)
    )
    return out.filter(pl.col("unique_member_code").is_not_null()).select(
        "unique_member_code", "full_name", "house", "year",
        "longest_run_sitting_days", "run_calendar_days", "run_start", "run_end",
    )


# ── 3 + 4. divergence + TAA compliance (from attendance gold) ────────────────


def build_presence(att: pl.DataFrame, part: pl.DataFrame, office: pl.DataFrame, leaders: pl.DataFrame) -> pl.DataFrame:
    """TAA presence joined to vote turnout → divergence + allowance compliance.

    holds_office (minister OR chair, derived from the member feed) gates both:
    office-holders aren't paid TAA on the attendance basis (so they're excluded
    from the compliance set) and structurally vote less (so they're excluded from
    the divergence headline — divergence is a backbencher signal)."""
    a = att.select(
        "unique_member_code", "house", "year", "full_name", "party_name", "constituency",
        "sitting_days", "other_days", "total_days",
    ).filter(pl.col("unique_member_code").is_not_null() & (pl.col("unique_member_code") != ""))
    p = part.select("unique_member_code", "house", "year", "voted_in", "total_divisions", "turnout_pct")
    off = office.select("unique_member_code", "holds_office", "is_minister")
    out = (
        a.join(p, on=["unique_member_code", "house", "year"], how="left")
        .join(off, on="unique_member_code", how="left")
        .join(leaders.select("full_name", "house", "is_leader"), on=["full_name", "house"], how="left")
        .with_columns(
            holds_office=pl.col("holds_office").fill_null(False),  # noqa: FBT003
            is_minister=pl.col("is_minister").fill_null(False),  # noqa: FBT003
            is_leader=pl.col("is_leader").fill_null(False),  # noqa: FBT003
        )
        .with_columns(
            meets_120=pl.col("total_days") >= TAA_MINIMUM_DAYS,
            days_below_minimum=(TAA_MINIMUM_DAYS - pl.col("total_days")).clip(lower_bound=0),
        )
        .with_columns(
            deduction_pct=pl.col("days_below_minimum").clip(upper_bound=100),  # 1%/day, capped at 100%
            divergence_present_low_vote=(
                (pl.col("total_days") >= 100)
                & (pl.col("turnout_pct").fill_null(0) < 50)
                & (~pl.col("holds_office"))
                & (~pl.col("is_leader"))  # leaders vote less via pairing — structural, not divergence
            ),
        )
    )
    return out


# ── 5. news vindication (code-keyed recent leave/health article) ─────────────


_NEWS_SCHEMA = {
    "unique_member_code": pl.Utf8, "year": pl.Int64, "reason_label": pl.Utf8,
    "source_title": pl.Utf8, "source_url": pl.Utf8, "outlet": pl.Utf8, "is_curated": pl.Boolean,
}


def _curated_explanations(name_to_code: pl.DataFrame) -> pl.DataFrame:
    """Hand-sourced, durable explanations (survive the live feed's rolling window).

    Keyed by (full_name, house, year) → resolved to unique_member_code. These are
    DISPLAYED facts with a source link, never an inferred verdict."""
    if not EXPLANATIONS_CSV.exists():
        return pl.DataFrame(schema=_NEWS_SCHEMA)
    cur = pl.read_csv(EXPLANATIONS_CSV)
    cur = cur.join(name_to_code, on=["full_name", "house"], how="left").filter(
        pl.col("unique_member_code").is_not_null()
    )
    return cur.select(
        "unique_member_code",
        pl.col("year").cast(pl.Int64),
        pl.col("reason_label"),
        pl.col("source_title"),
        pl.col("source_url"),
        pl.lit("curated").alias("outlet"),
        pl.lit(True).alias("is_curated"),  # noqa: FBT003
    )


def build_absence_news(name_to_code: pl.DataFrame) -> pl.DataFrame:
    """Code-keyed explanation lookup — CURATED, hand-verified entries ONLY.

    The live Google-News fallback was REMOVED 2026-06-23 (user request). A per-member
    name search returns any namesake — a different person of the same name in the
    headline (an obituary, a GAA player, a historical figure) — so it surfaced
    flatly wrong "explanations". Only the small curated set (each source manually
    verified) is trustworthy enough to display as an absence reason; every other
    member shows the honest "No public explanation found"."""
    return _curated_explanations(name_to_code)


# ── run ──────────────────────────────────────────────────────────────────────


def run() -> dict[str, int]:
    votes = pl.concat([_votes(DAIL_VOTES, "Dáil"), _votes(SEANAD_VOTES, "Seanad")])
    att = pl.concat([_attendance(DAIL_ATT, "Dáil"), _attendance(SEANAD_ATT, "Seanad")], how="diagonal_relaxed")
    office = _office_flags()
    leaders = _leaders()

    participation = build_participation(votes, office, leaders)
    # enrich the turnout rows with constituency from the member record (is_minister
    # already set reliably from the office feed above).
    att_slim = att.select("unique_member_code", "house", "year", "constituency").unique(
        ["unique_member_code", "house", "year"]
    )
    participation = participation.join(att_slim, on=["unique_member_code", "house", "year"], how="left").with_columns(
        constituency=pl.col("constituency").fill_null("")
    )
    # PHYSICAL-absence gaps from the attendance PDFs (NOT votes — the vote-gap conflated
    # absence with present-but-not-voting). code_map links the silver fact's `identifier`
    # to unique_member_code via the gold roster's member_id.
    code_map = (
        att.select(pl.col("member_id").alias("identifier"), "unique_member_code", "full_name", "house", "year")
        .filter(pl.col("identifier").is_not_null() & pl.col("unique_member_code").is_not_null())
        .unique(["identifier", "house", "year"])
    )
    gaps = build_absence_gaps(_attendance_dates(), code_map)
    presence = build_presence(att, participation, office, leaders)
    # name→code map for resolving curated explanations (union of both sources so a
    # member who barely votes but is in the TAA record still resolves).
    name_to_code = (
        pl.concat([
            participation.select("full_name", "house", "unique_member_code"),
            presence.select("full_name", "house", "unique_member_code"),
        ])
        .filter(pl.col("unique_member_code").is_not_null() & (pl.col("unique_member_code") != ""))
        .unique(["full_name", "house"])
    )
    news = build_absence_news(name_to_code)

    save_parquet(participation, GOLD / "participation_member_year.parquet")
    save_parquet(gaps, GOLD / "participation_absence_gaps.parquet")
    save_parquet(presence, GOLD / "participation_presence_year.parquet")
    save_parquet(news, GOLD / "participation_absence_news.parquet")

    counts = {
        "participation": participation.height,
        "absence_gaps": gaps.height,
        "presence": presence.height,
        "news": news.height,
    }
    logger.info("participation pipeline wrote %s", counts)
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run())
