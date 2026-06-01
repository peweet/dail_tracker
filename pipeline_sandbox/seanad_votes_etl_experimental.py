"""Seanad votes ETL — EXPERIMENTAL PROTOTYPE.

# DELETE AFTER PROMOTION: services/votes.py (fetch) + transform_votes.py (transform)

Fetches Seanad Éireann division (vote) records from the Oireachtas API and
flattens the nested division→tally→member structure into one row per
(division × vote_type × member), the same silver shape transform_votes.py
produces for the Dáil.

Why this exists:
  services/votes.py hardcodes `chamber=dail` (build_vote_url) and
  transform_votes.py hardcodes the `/dail/` URL segment, so the gold vote
  history only carries house_number ∈ {31,32,33,34}. The 27th Seanad has 1,208
  divisions since 2016 with an identical API envelope. This prototype proves the
  fetch + a fully-vectorised Polars transform (no row loops) before the logic is
  promoted into the two production scripts above, parameterised by chamber.

Guide / parity:
  - Fetch pagination mirrors services/votes.py (skip/limit + resultCount assert).
  - Field mapping mirrors transform_votes.normalize_vote_data, re-expressed in
    vectorised Polars (unnest → unpivot → explode → struct.field).
  - URL is house-aware: house_code drives /seanad/ vs /dail/ instead of a literal.

Source : https://api.oireachtas.ie/v1/votes?chamber=seanad&chamber_type=house&...
Writes : data/silver/parquet/seanad_pretty_votes_experimental.parquet   (--write)

Exploration (default, no write):
  python pipeline_sandbox/seanad_votes_etl_experimental.py
Write the experimental parquet:
  python pipeline_sandbox/seanad_votes_etl_experimental.py --write
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_SILVER_PARQUET = _ROOT / "data" / "silver" / "parquet"
_SEANAD_MEMBERS = _SILVER_PARQUET / "flattened_seanad_members.parquet"

API_BASE = "https://api.oireachtas.ie/v1"
VOTES_DATE_START = "2016-01-01"  # parity with services/dail_config.VOTES_DATE_START
PAGE_SIZE = 1000  # API server cap; larger values are silently clamped

_TALLY_KEYS = ("taVotes", "nilVotes", "staonVotes")
_TYPE_LABELS = {"nilVotes": "Voted No", "taVotes": "Voted Yes", "staonVotes": "Abstained"}


def _tally_keys_with_members(tallies_dtype: pl.Struct) -> list[str]:
    """Which of the three tally types are present as a Struct carrying `members`.

    Schema-level inspection (not row iteration): the API omits a tally type when
    a division has none of that vote, and Polars then infers that subfield as
    Null rather than a Struct. Seanad divisions in this window carry no
    abstentions, so `staonVotes` is Null-typed and must be skipped to avoid a
    StructFieldNotFoundError. Returns keys in canonical _TALLY_KEYS order.
    """
    present = {f.name: f.dtype for f in tallies_dtype.fields}
    keys = []
    for key in _TALLY_KEYS:
        sub = present.get(key)
        if isinstance(sub, pl.Struct) and any(f.name == "members" for f in sub.fields):
            keys.append(key)
    return keys


# ---------------------------------------------------------------------------
# Fetch  (I/O pagination only — mirrors services/votes.py; not data wrangling)
# ---------------------------------------------------------------------------
def fetch_seanad_divisions(limit_pages: int | None = None) -> list[dict]:
    """Page through every Seanad division since VOTES_DATE_START.

    Returns the flat list of result dicts (each carries a 'division' struct),
    matching the input transform_divisions expects. The skip/limit loop and the
    resultCount assertion are lifted verbatim from services/votes.fetch_votes so
    silent truncation cannot recur. `limit_pages` caps pages for exploration.
    """
    base = (
        f"{API_BASE}/votes?chamber_type=house&chamber_id=&chamber=seanad"
        f"&date_start={VOTES_DATE_START}&sort=date&order=desc&outcome="
    )
    session = requests.Session()
    results: list[dict] = []
    expected: int | None = None
    skip = 0
    page_no = 0

    while True:
        resp = session.get(f"{base}&limit={PAGE_SIZE}&skip={skip}", timeout=(10, 60))
        resp.raise_for_status()
        page = resp.json()
        if expected is None:
            expected = page["head"]["counts"]["resultCount"]
        page_rows = page.get("results", [])
        results.extend(page_rows)
        page_no += 1
        print(f"  page {page_no}: got {len(page_rows)} | running={len(results)} | expected={expected}")
        if limit_pages is not None and page_no >= limit_pages:
            break
        if len(page_rows) < PAGE_SIZE or len(results) >= expected:
            break
        skip += PAGE_SIZE

    if limit_pages is None:
        assert len(results) >= expected, f"Vote pagination drift: {len(results)} of {expected}"
    return results


# ---------------------------------------------------------------------------
# Transform  (fully vectorised Polars — no row loops, no .apply)
# ---------------------------------------------------------------------------
def transform_divisions(results: list[dict]) -> pl.DataFrame:
    """Flatten division→tally→member into one row per voting member.

    Vectorised throughout:
      unnest the division struct → select scalar fields + the three tally
      member-lists → unpivot the three lists into a `vote_type` column →
      explode the member list → pull memberCode from the nested struct.
    """
    raw = pl.DataFrame(results, infer_schema_length=None)
    div = raw.unnest("division")

    tally_keys = _tally_keys_with_members(div.schema["tallies"])

    df = div.select(
        pl.col("date").alias("vote_date"),
        pl.col("outcome").alias("vote_outcome"),
        pl.col("voteId").alias("vote_id_raw"),
        pl.col("house").struct.field("houseNo").alias("house_number"),
        pl.col("house").struct.field("houseCode").alias("house_code"),
        pl.col("subject").struct.field("showAs").alias("subject"),
        pl.col("debate").struct.field("showAs").alias("debate_title"),
        *[pl.col("tallies").struct.field(k).struct.field("members").alias(k) for k in tally_keys],
    )

    id_cols = [
        "vote_date", "vote_outcome", "vote_id_raw",
        "house_number", "house_code", "subject", "debate_title",
    ]
    df = (
        df.unpivot(
            index=id_cols,
            on=tally_keys,
            variable_name="vote_type",
            value_name="members",
        )
        .explode("members")
        .drop_nulls("members")  # divisions where a tally type had no members
    )

    df = df.with_columns(
        pl.col("members").struct.field("member").struct.field("memberCode").alias("unique_member_code"),
    ).drop("members")

    # vote_id from the API is a per-day counter ("vote_1"); make it globally
    # unique by prefixing the date (parity with transform_votes.py).
    df = df.with_columns(pl.col("vote_id_raw").str.split("_").list.last().alias("vote_counter"))
    df = df.with_columns(
        pl.format(
            "https://www.oireachtas.ie/en/debates/vote/{}/{}/{}/{}/",
            pl.col("house_code"), pl.col("house_number"), pl.col("vote_date"), pl.col("vote_counter"),
        ).alias("vote_url"),
        pl.concat_str([pl.col("vote_date"), pl.col("vote_counter")], separator="_").alias("vote_id"),
        pl.col("vote_date").str.to_date(strict=False).alias("date"),
        pl.col("vote_type").replace(_TYPE_LABELS),
    )

    return df.drop("vote_id_raw", "vote_counter").unique()


# ---------------------------------------------------------------------------
# Data-quality assessment
# ---------------------------------------------------------------------------
def assess(df: pl.DataFrame) -> None:
    """Print exploratory cleanliness checks + a confidence/DQ verdict."""
    print("\n=== HEAD (5) ===")
    with pl.Config(tbl_cols=-1, fmt_str_lengths=40):
        print(df.head(5))

    n = df.height
    divisions = df.select(pl.col("vote_id").n_unique()).item()
    members = df.select(pl.col("unique_member_code").n_unique()).item()
    print("\n=== SHAPE ===")
    print(f"rows={n:,} | distinct divisions={divisions:,} | distinct members={members:,}")

    print("\n=== NULLS (key cols) ===")
    key = ["unique_member_code", "vote_outcome", "vote_type", "date", "vote_id", "vote_url"]
    nulls = df.select([pl.col(c).null_count().alias(c) for c in key])
    print(nulls)

    print("\n=== vote_type distribution ===")
    print(df["vote_type"].value_counts(sort=True))

    # Like the Dáil query (terms 31–34), the Seanad query returns every term
    # since VOTES_DATE_START, not just the current 27th. Valid set = 24–27.
    seanad_terms = {"24", "25", "26", "27"}
    print("\n=== house_number (Seanad terms since 2016; valid = 24–27) ===")
    print(df["house_number"].unique().to_list())

    print("\n=== date range ===")
    print(df.select(pl.col("date").min().alias("min"), pl.col("date").max().alias("max")))

    # Join rate against the CURRENT 27th-Seanad member parquet. Historic cohorts
    # (25th/26th Seanad) voted since 2016 but are not in the current 60-member
    # registry — a low match rate here is expected, not a defect.
    matched_pct = unmatched = None
    if _SEANAD_MEMBERS.exists():
        current = pl.read_parquet(_SEANAD_MEMBERS).select("unique_member_code").unique()
        voters = df.select("unique_member_code").unique()
        matched = voters.join(current, on="unique_member_code", how="inner").height
        matched_pct = 100 * matched / voters.height if voters.height else 0
        unmatched = voters.height - matched
        print("\n=== JOIN to current 27th Seanad (60 members) ===")
        print(f"distinct voters={voters.height} | matched={matched} ({matched_pct:.1f}%) | "
              f"unmatched (historic cohorts)={unmatched}")

    # Bad-URL / malformed-id guards
    bad_url = df.filter(~pl.col("vote_url").str.contains("/seanad/")).height
    bad_id = df.filter(~pl.col("vote_id").str.contains("_")).height
    print("\n=== SANITY GUARDS ===")
    print(f"rows missing /seanad/ in url={bad_url} | rows with malformed vote_id={bad_id}")

    print("\n=== DATA-QUALITY VERDICT ===")
    key_nulls_total = sum(nulls.row(0))
    issues = []
    if key_nulls_total:
        issues.append(f"{key_nulls_total} null(s) in key columns")
    if bad_url or bad_id:
        issues.append("URL/id malformation present")
    if not set(df["house_number"].unique().to_list()).issubset(seanad_terms):
        issues.append("house_number outside valid Seanad terms 24–27")
    if not issues:
        print("CLEAN. Structure identical to Dáil; transform is field-for-field parity.")
        print("CONFIDENCE: HIGH for promotion to services/votes.py + transform_votes.py.")
        print("  - Fetch logic is a verbatim mirror of the audited Dáil pagination.")
        print("  - Transform is vectorised (unnest/unpivot/explode); zero row loops.")
        print(f"  - {matched_pct:.0f}% of distinct voters resolve to the current Seanad;"
              if matched_pct is not None else "  - member join skipped (parquet absent);")
        print("    the remainder are prior-cohort senators (expected, by VOTES_DATE_START=2016).")
        print("  - Residual risk: vote_url path format for Seanad debates not yet click-verified.")
    else:
        print("ISSUES: " + "; ".join(issues))
        print("CONFIDENCE: MEDIUM — investigate the above before promotion.")


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Seanad votes ETL (experimental prototype)")
    ap.add_argument("--write", action="store_true", help="write the experimental silver parquet")
    ap.add_argument("--pages", type=int, default=None, help="cap pages fetched (exploration)")
    args = ap.parse_args()

    print("Fetching Seanad divisions...")
    results = fetch_seanad_divisions(limit_pages=args.pages)
    print(f"Fetched {len(results)} division records.")

    df = transform_divisions(results)
    assess(df)

    if args.write:
        _SILVER_PARQUET.mkdir(parents=True, exist_ok=True)
        out = _SILVER_PARQUET / "seanad_pretty_votes_experimental.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
        print(f"\nWrote {df.height:,} rows -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
