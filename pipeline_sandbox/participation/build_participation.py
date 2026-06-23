"""EXPERIMENTAL sandbox — rebuild 'attendance' as participation + absence signals.

The TAA presence record is right-censored at the 120-day allowance threshold, so
its COUNT is meaningless as an attendance metric (97% pile at exactly 120). This
keeps the EXTRACT but derives three honest signals instead:

  1. absence_gaps         — longest run of consecutive PLENARY SITTING days a
                            member missed (daily resolution, recess-proof,
                            led by the calendar date-diff). Headline feature.
  2. division_participation — votes voted / missed / turnout %, off the per-
                            member-division vote history (uncensored).

Polars base pipeline. Sandbox only — reads existing silver/gold parquet, writes
under pipeline_sandbox/participation/_out. No gold writes; vet before promotion.

Run:  python pipeline_sandbox/participation/build_participation.py
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "_out"
OUT.mkdir(exist_ok=True)

DAIL_ATT = ROOT / "data/silver/parquet/td_attendance_fact_table.parquet"
SEANAD_ATT = ROOT / "data/silver/parquet/seanad_attendance_fact_table.parquet"
DAIL_VOTES = ROOT / "data/gold/parquet/current_dail_vote_history.parquet"
SEANAD_VOTES = ROOT / "data/gold/parquet/current_seanad_vote_history.parquet"


# ── helpers ────────────────────────────────────────────────────────────────────


def _load_attendance() -> pl.DataFrame:
    """One long frame of badge-in dates across both houses.

    Each source row carries EITHER a sitting date OR an other (committee/non-
    sitting) date. We melt to (member, house, year, present_date, is_plenary).
    """
    frames = []
    for path, house in ((DAIL_ATT, "Dáil"), (SEANAD_ATT, "Seanad")):
        df = pl.read_parquet(path)
        df = df.with_columns(
            member=pl.col("identifier"),
            display_name=(pl.col("first_name") + " " + pl.col("last_name")),
            house=pl.lit(house),
            total_days=pl.col("sitting_total_days"),
        )
        sitting = df.filter(pl.col("iso_sitting_days_attendance").is_not_null()).with_columns(
            present_date=pl.col("iso_sitting_days_attendance").str.to_date(),
            is_plenary=pl.lit(True),  # noqa: FBT003
        )
        other = df.filter(pl.col("iso_other_days_attendance").is_not_null()).with_columns(
            present_date=pl.col("iso_other_days_attendance").str.to_date(),
            is_plenary=pl.lit(False),  # noqa: FBT003
        )
        cols = ["member", "display_name", "house", "year", "total_days", "present_date", "is_plenary"]
        frames.append(pl.concat([sitting.select(cols), other.select(cols)]))
    return pl.concat(frames).unique(["member", "house", "year", "present_date", "is_plenary"])


# ── 1. absence gaps ─────────────────────────────────────────────────────────────


def build_absence_gaps(att: pl.DataFrame) -> pl.DataFrame:
    """Longest interior run of consecutive plenary sitting days a member missed.

    Calendar = distinct PLENARY sitting dates per (house, year) — the days the
    chamber actually sat. Recess-proof: no plenary sittings in recess, so a
    summer break is not an absence. A member is 'present' on a sitting date if
    they badged in at all that day (sitting OR other). The gap is the run of
    consecutive sitting dates between two attended ones — interior by
    construction, so always real (never the 120-day trailing censoring).
    """
    # Plenary sitting calendar per (house, year), indexed in date order.
    cal = (
        att.filter(pl.col("is_plenary"))
        .select("house", "year", "present_date")
        .unique()
        .sort(["house", "year", "present_date"])
        .with_columns(idx=pl.int_range(pl.len()).over(["house", "year"]))
        .rename({"present_date": "sit_date"})
    )
    n_sit = cal.group_by(["house", "year"]).agg(total_plenary_sit_days=pl.len())

    # Days a member was present, restricted to plenary sitting dates.
    present = att.select("member", "display_name", "house", "year", "total_days", "present_date").unique()
    attended = present.join(
        cal, left_on=["house", "year", "present_date"], right_on=["house", "year", "sit_date"], how="inner"
    ).sort(["member", "house", "year", "present_date"])

    # Interior gaps: consecutive attended sitting dates → run of missed dates between.
    grp = ["member", "house", "year"]
    attended = attended.with_columns(
        prev_idx=pl.col("idx").shift(1).over(grp),
        prev_date=pl.col("present_date").shift(1).over(grp),
    ).with_columns(
        sitting_days_missed=(pl.col("idx") - pl.col("prev_idx") - 1),
        gap_calendar_days=(pl.col("present_date") - pl.col("prev_date")).dt.total_days(),
    )

    present_sit = attended.group_by(grp).agg(
        present_sit_days=pl.len(),
        first_sit=pl.col("present_date").min(),
        last_sit=pl.col("present_date").max(),
        display_name=pl.col("display_name").first(),
        total_days=pl.col("total_days").first(),
    )

    # Pick the longest run per member.
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

    out = (
        present_sit.join(longest, on=grp, how="left")
        .join(n_sit, on=["house", "year"], how="left")
        .with_columns(
            total_sitting_days_missed=(pl.col("total_plenary_sit_days") - pl.col("present_sit_days")),
            hit_120=(pl.col("total_days") >= 120),
            longest_run_sitting_days=pl.col("longest_run_sitting_days").fill_null(0),
        )
        .sort(["house", "year", "longest_run_sitting_days"], descending=[False, False, True])
    )
    return out


# ── 2. division participation ────────────────────────────────────────────────────


def build_participation() -> pl.DataFrame:
    """Votes voted / missed / turnout %, per (member, house, year).

    Off the per-member-division vote history (a row exists iff the member voted),
    so absence = no row. Denominator = distinct divisions in that (house, year).
    NOTE v1: denominator is the whole-year division count; mid-term entrants are
    flagged (first_vote well after term start) rather than window-bounded — the
    member_terms join is the promotion-stage refinement.
    """
    frames = []
    for path, house in ((DAIL_VOTES, "Dáil"), (SEANAD_VOTES, "Seanad")):
        df = pl.read_parquet(path).with_columns(
            d=pl.col("date").str.to_date(strict=False), house=pl.lit(house)
        )
        df = df.filter(pl.col("full_name").is_not_null() & pl.col("d").is_not_null())
        df = df.with_columns(year=pl.col("d").dt.year())
        totals = df.group_by(["house", "year"]).agg(total_divisions=pl.col("vote_id").n_unique())
        per = df.group_by(["house", "year", "full_name", "party"]).agg(
            voted_in=pl.col("vote_id").n_unique(),
            abstentions=(pl.col("vote_type") == "Abstained").sum(),
            first_vote=pl.col("d").min(),
            last_vote=pl.col("d").max(),
        )
        out = per.join(totals, on=["house", "year"], how="left").with_columns(
            missed=(pl.col("total_divisions") - pl.col("voted_in")),
            turnout_pct=(100.0 * pl.col("voted_in") / pl.col("total_divisions")).round(1),
        )
        frames.append(out)
    return pl.concat(frames).sort(["house", "year", "turnout_pct"])


# ── run + validate ───────────────────────────────────────────────────────────────


def _check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}")


def main() -> None:
    att = _load_attendance()
    gaps = build_absence_gaps(att)
    part = build_participation()

    gaps.write_parquet(OUT / "absence_gaps.parquet")
    part.write_parquet(OUT / "division_participation.parquet")

    print("\n=== absence_gaps: Dáil 2025 worst 8 ===")
    d25 = gaps.filter((pl.col("house") == "Dáil") & (pl.col("year") == 2025)).head(8)
    print(
        d25.select(
            "display_name", "present_sit_days", "total_plenary_sit_days",
            "longest_run_sitting_days", "run_calendar_days", "run_start", "run_end", "hit_120",
        ).to_pandas().to_string()
    )

    print("\n=== division_participation: Dáil 2025 worst 8 ===")
    p25 = part.filter((pl.col("house") == "Dáil") & (pl.col("year") == 2025)).head(8)
    print(p25.select("full_name", "party", "voted_in", "missed", "total_divisions", "turnout_pct").to_pandas().to_string())

    print("\n=== validation vs ground truth ===")
    rbb = gaps.filter(pl.col("display_name").str.contains("Richard Boyd")).filter(pl.col("year") == 2025)
    _check(
        "RBB 2025 longest run == 56 sitting days",
        not rbb.is_empty() and int(rbb["longest_run_sitting_days"][0]) == 56,
        f"got {None if rbb.is_empty() else int(rbb['longest_run_sitting_days'][0])}",
    )
    cairns = gaps.filter(pl.col("display_name") == "Holly Cairns").filter(pl.col("year") == 2025)
    _check(
        "Holly Cairns 2025 longest run == 18 (maternity)",
        not cairns.is_empty() and int(cairns["longest_run_sitting_days"][0]) == 18,
        f"got {None if cairns.is_empty() else int(cairns['longest_run_sitting_days'][0])}",
    )
    inv = part.filter(pl.col("turnout_pct") > 100)
    _check("turnout_pct never exceeds 100%", inv.is_empty(), f"{inv.height} rows over 100%")
    bad = part.filter((pl.col("voted_in") + pl.col("missed")) != pl.col("total_divisions"))
    _check("voted + missed == total_divisions", bad.is_empty(), f"{bad.height} mismatches")
    # Blaney 2024 ground truth (Donegal News/Irish Times "Has anyone seen Niall Blaney"):
    # the TAA count was reconciled to 120 (looks compliant) but votes are unfakeable —
    # worst Seanad turnout + a 26-sitting-day absence run. The vote record is the signal.
    bv = part.filter(pl.col("full_name").str.contains("Blaney")).filter(pl.col("year") == 2024)
    if not bv.is_empty():
        _check(
            "Blaney 2024 Seanad WORST-tier turnout (~20%)",
            float(bv["turnout_pct"][0]) < 25,
            f"{bv['turnout_pct'][0]}% ({bv['voted_in'][0]}/{bv['total_divisions'][0]})",
        )
    bg = gaps.filter(pl.col("display_name").str.contains("Blaney")).filter(pl.col("year") == 2024)
    if not bg.is_empty():
        _check(
            "Blaney 2024 absence run == 26 sitting days (Mar7-Jun12)",
            int(bg["longest_run_sitting_days"][0]) == 26,
            f"got {int(bg['longest_run_sitting_days'][0])}",
        )

    print(f"\nwrote {OUT/'absence_gaps.parquet'} ({gaps.height} rows)")
    print(f"wrote {OUT/'division_participation.parquet'} ({part.height} rows)")


if __name__ == "__main__":
    main()
