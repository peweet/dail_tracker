"""Seanad full ETL — EXPERIMENTAL end-to-end preview.

# DELETE AFTER PROMOTION: this orchestrator only exists to preview the outputs
# that integration would produce. Promotion = the small per-file changes below,
# NOT shipping this script.

Produces the Senator equivalents of every Dáil gold dataset, by REUSING the
production functions verbatim (validated against real data 2026-06-01):

  votes       reuse transform_votes.normalize_vote_data (chamber-agnostic);
              only the /seanad/ URL segment differs  → seanad_pretty_votes.csv
  attendance  reuse attendance._extract_pdf_tables / _build_silver_csv /
              _build_fact_table  (works UNCHANGED on Senator PDFs)
  payments    reuse payments_full_psa_etl._iter_rows_from_pdf; _split_position
              is monkeypatched here to recognise 'Senator'/'Senaotr' — this
              simulates the ONE ~2-line production change needed.
  gold        reuse enrich._build_members_and_master / _build_enriched_attendance
              / _build_attendance_by_year / _build_vote_history /
              _build_payment_rankings  (all already fully arg-parameterised)

All outputs land in pipeline_sandbox/_seanad_output/ so nothing in data/gold is
touched. Assess there, then decide integration.

Usage:
  python pipeline_sandbox/seanad_full_etl_experimental.py
"""
from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path

import pandas as pd
import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import attendance  # noqa: E402  production parser (reused unchanged)
import enrich  # noqa: E402  production gold builder (helpers reused)
import payments_full_psa_etl as PAY  # noqa: E402
import transform_votes  # noqa: E402
from config import SILVER_DIR  # noqa: E402
from seanad_votes_etl_experimental import fetch_seanad_divisions  # noqa: E402  (reuse sandbox fetch)

_PDF = _ROOT / "data" / "bronze" / "pdfs"
_ATT_PDF = _PDF / "attendance_seanad_experimental"
_PAY_PDF = _PDF / "payments_seanad_experimental"
_MEMBERS_CSV = SILVER_DIR / "flattened_seanad_members.csv"

OUT = _ROOT / "pipeline_sandbox" / "_seanad_output"
OUT.mkdir(parents=True, exist_ok=True)

_ZSTD = dict(compression="zstd", compression_level=3, statistics=True)


# ── Payments: simulate the one production change (_split_position += Senator) ──
_orig_split = PAY._split_position


def _senator_aware_split(name_cell: str):
    """Drop-in for _split_position that also knows Senator (+ 'Senaotr' typo).

    Mirrors the proposed 1-line production change. Everything else in the
    payments parser is reused untouched.
    """
    if name_cell:
        parts = name_cell.strip().split(" ", 1)
        if len(parts) == 2 and (parts[0].startswith("Sen") or parts[0] == "Cathaoirleach"):
            pos = "Senator" if parts[0].startswith("Sen") else parts[0]
            return pos, parts[1].strip()
    return _orig_split(name_cell)


PAY._split_position = _senator_aware_split


# ── Step 1: votes silver ──────────────────────────────────────────────────────
def build_votes() -> Path:
    print("\n[1/5] votes — fetch + reuse transform_votes.normalize_vote_data")
    results = fetch_seanad_divisions()
    frames: list[pd.DataFrame] = []
    for r in results:  # small data; reuse production flatten per division
        frames.extend(transform_votes.normalize_vote_data(r))
    df = pd.concat(frames, ignore_index=True)
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
    df["vote_id"] = df["vote_id"].str.split("_").str[-1]
    # Only chamber-specific line vs transform_votes.main(): /seanad/ not /dail/.
    df["vote_url"] = df.apply(
        lambda row: f"https://www.oireachtas.ie/en/debates/vote/seanad/{row['house_number']}/{row['vote_date']}/{row['vote_id']}/",
        axis=1,
    )
    df["vote_id"] = df["vote_date"].astype(str) + "_" + df["vote_id"].astype(str)
    df["date"] = pd.to_datetime(df["vote_date"], errors="coerce").dt.date
    df = df.drop(["member_name", "vote_date"], axis=1)
    df = df.replace({"nilVotes": "Voted No", "taVotes": "Voted Yes", "staonVotes": "Abstained"})
    df["house"] = "Seanad"
    out = OUT / "seanad_pretty_votes.csv"
    df.to_csv(out, index=False)
    print(f"      -> {out.name}  rows={len(df):,}")
    return out


# ── Step 2: attendance silver + fact (reuse attendance.py verbatim) ───────────
def build_attendance() -> Path:
    print("\n[2/5] attendance — reuse attendance._extract/_build (unchanged)")
    silver_csv = OUT / "seanad_aggregated_tables.csv"
    fact_csv = OUT / "seanad_attendance_fact_table.csv"
    fact_parquet = OUT / "seanad_attendance_fact_table.parquet"
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):  # extractor is per-line chatty
        dfs, _ = attendance._extract_pdf_tables(_ATT_PDF)
        attendance._build_silver_csv(dfs, silver_csv)
        attendance._build_fact_table(silver_csv, fact_csv, fact_parquet)
    n = pd.read_csv(fact_csv).shape[0]
    print(f"      -> {fact_csv.name} + .parquet  rows={n:,}")
    return fact_csv


# ── Step 3: payments gold (reuse _iter_rows_from_pdf + monkeypatched split) ───
def build_payments() -> Path:
    print("\n[3/5] payments — reuse payments_full_psa_etl._iter_rows_from_pdf")
    rows = [r for pdf in sorted(_PAY_PDF.glob("*.pdf")) for r in PAY._iter_rows_from_pdf(pdf)]
    df = pl.DataFrame(
        {
            "member_name": [r.member_name for r in rows],
            "position": [r.position for r in rows],
            "payment_kind": [r.payment_kind for r in rows],
            "taa_band_raw": [r.taa_band_raw for r in rows],
            "taa_band_label": [r.taa_band_label for r in rows],
            "date_paid": [r.date_paid for r in rows],
            "narrative": [r.narrative for r in rows],
            "amount": [r.amount for r in rows],
            "source_pdf": [r.source_pdf for r in rows],
            "schema": [r.schema for r in rows],
        }
    ).unique(subset=["member_name", "date_paid", "amount", "payment_kind"], keep="first")
    is_clean = (
        pl.col("amount").is_not_null()
        & pl.col("amount").is_between(1, 100_000)
        & pl.col("date_paid").is_not_null()
        & (pl.col("payment_kind") != "UNKNOWN")
        & pl.col("member_name").str.len_chars().gt(0)
    )
    clean = df.filter(is_clean).with_columns(pl.lit("Seanad").alias("house"))
    out = OUT / "seanad_payments_full_psa.parquet"
    clean.write_parquet(out, **_ZSTD)
    print(f"      -> {out.name}  clean={clean.height:,}  quarantined={df.height - clean.height}")
    return out


# ── Step 4 + 5: gold via enrich._build_* helpers (reused as-is) ───────────────
def build_gold(votes_csv: Path, fact_csv: Path, payments_parquet: Path) -> dict[str, Path]:
    print("\n[4/5] members + attendance gold — reuse enrich._build_* ")
    members_wide, master = enrich._build_members_and_master(_MEMBERS_CSV)
    master.write_csv(OUT / "seanad_master_list.csv")

    enriched = enrich._build_enriched_attendance(members_wide, fact_csv)
    enriched_csv = OUT / "seanad_enriched_attendance.csv"
    enriched.write_csv(enriched_csv)
    enrich._build_attendance_by_year(
        enriched, OUT / "seanad_attendance_by_year.csv", OUT / "seanad_attendance_by_year.parquet"
    )

    print("\n[5/5] vote history + payment rankings — reuse enrich._build_* ")
    enrich._build_vote_history(
        votes_csv, enriched_csv,
        OUT / "current_seanad_vote_history.csv", OUT / "current_seanad_vote_history.parquet",
    )
    enrich._build_payment_rankings(
        master, payments_parquet,
        OUT / "current_senator_payment_rankings.csv", OUT / "current_senator_payment_rankings.parquet",
    )
    return {
        "master": OUT / "seanad_master_list.csv",
        "vote_history": OUT / "current_seanad_vote_history.parquet",
        "rankings": OUT / "current_senator_payment_rankings.parquet",
        "attendance_year": OUT / "seanad_attendance_by_year.parquet",
    }


def summarise(paths: dict[str, Path]) -> None:
    print("\n" + "=" * 74 + "\nOUTPUT SUMMARY (pipeline_sandbox/_seanad_output/)\n" + "=" * 74)
    for label, p in paths.items():
        if not p.exists():
            print(f"  {label:18} MISSING")
            continue
        df = pl.read_parquet(p) if p.suffix == ".parquet" else pl.read_csv(p)
        print(f"  {label:18} {p.name:42} rows={df.height:,} cols={df.width}")

    # Headline cross-check: how many of the 60 current senators surface in gold
    master = pl.read_csv(paths["master"])
    cur = master.select("join_key").unique()
    rank = pl.read_parquet(paths["rankings"])
    votes = pl.read_parquet(paths["vote_history"])
    print(f"\n  current senators in master: {cur.height}")
    print(f"  senators with a payment ranking: {rank.height}")
    print(f"  senators in vote history (resolved member): "
          f"{votes.filter(pl.col('unique_member_code').is_not_null())['unique_member_code'].n_unique()}")
    print("\n  Sample payment rankings (top 5):")
    with pl.Config(tbl_cols=-1, fmt_str_lengths=30):
        print(rank.sort("total_amount_paid_since_2020", descending=True).head(5))


def main() -> int:
    if not any(_ATT_PDF.glob("*.pdf")) or not any(_PAY_PDF.glob("*.pdf")):
        print("Senator PDFs missing — run seanad_pdf_poll_experimental.py --limit 0 first.")
        return 1
    votes_csv = build_votes()
    fact_csv = build_attendance()
    pay_parquet = build_payments()
    paths = build_gold(votes_csv, fact_csv, pay_parquet)
    summarise(paths)
    print("\nDone. Outputs are experimental; assess before integration.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
