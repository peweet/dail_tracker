"""
pipeline_sandbox/lobbying_enrichment.py

Reads raw silver/bronze lobbying CSVs produced by lobby_processing.py, performs
all joins and aggregations in Polars, and writes flat denormalised Parquet files to:
    data/silver/lobbying/enriched/

These Parquet files are consumed by sql_views/lobbying_*.sql via read_parquet().
SQL views contain no JOINs — all join logic lives here.

Integration checklist (do NOT wire into pipeline.py until tested):
  1. Run standalone:  python pipeline_sandbox/lobbying_enrichment.py
  2. Confirm all enriched/*.parquet files appear in data/silver/lobbying/enriched/
  3. Start the Streamlit app and open the Lobbying page
  4. Verify each view loads correctly (hero stats, leaderboards, profiles)
  5. Only then wire enrich_lobbying() into pipeline.py after existing lobbying steps

Do not modify lobby_processing.py or pipeline.py.
"""
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT   = Path(__file__).resolve().parents[1]
_SILVER = _ROOT / "data" / "silver" / "lobbying"
_BRONZE = _ROOT / "data" / "bronze" / "lobbying_csv_data"
_OUT    = _SILVER / "enriched"


# ── Safe readers ───────────────────────────────────────────────────────────────

def _read(path: Path, **kwargs) -> pl.DataFrame:
    if not path.exists():
        print(f"  [skip] {path.name} not found")
        return pl.DataFrame()
    try:
        return pl.read_csv(path, infer_schema_length=10_000, **kwargs)
    except Exception as exc:
        print(f"  [warn] {path.name}: {exc}")
        return pl.DataFrame()


def _write(df: pl.DataFrame, name: str) -> None:
    _OUT.mkdir(parents=True, exist_ok=True)
    out = _OUT / name
    df.write_parquet(out)
    print(f"  [ok]   {name}  ({len(df):,} rows)")


def _parse_dates(df: pl.DataFrame, *cols: str) -> pl.DataFrame:
    for col in cols:
        if col in df.columns:
            df = df.with_columns(
                # Source datetimes are 'YYYY-MM-DDTHH:MM:SS.ffffff'; cast to Date.
                pl.col(col)
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                .cast(pl.Date)
            )
    return df


# ── Source loaders ─────────────────────────────────────────────────────────────

def _politician_returns() -> pl.DataFrame:
    df = _read(_SILVER / "politician_returns_detail.csv")
    return _parse_dates(df, "lobbying_period_start_date")


def _returns_master() -> pl.DataFrame:
    df = _read(_SILVER / "returns_master.csv")
    return _parse_dates(df, "lobbying_period_start_date", "lobbying_period_end_date")


def _lobby_counts() -> pl.DataFrame:
    return _read(_SILVER / "lobby_count_details.csv")


def _revolving_door_dpos() -> pl.DataFrame:
    return _read(_SILVER / "revolving_door_dpos.csv")


def _policy_area_breakdown() -> pl.DataFrame:
    return _read(_SILVER / "policy_area_breakdown.csv")


def _org_registry() -> pl.DataFrame:
    path = _BRONZE / "Lobbying_ie_organisation_results.csv"
    df = _read(path, ignore_errors=True) if path.exists() else pl.DataFrame()
    if df.is_empty():
        return df
    rename: dict[str, str] = {}
    for col in df.columns:
        if col == "Name":
            rename[col] = "lobbyist_name"
        elif "Main activities" in col:
            rename[col] = "sector_reg"
        elif col == "Website":
            rename[col] = "website_reg"
        elif col == "CompanyRegistrationNumber":
            rename[col] = "crn"
    return df.rename(rename)


# ── Enrichment steps ───────────────────────────────────────────────────────────

def _build_summary(returns: pl.DataFrame, pol: pl.DataFrame) -> None:
    """Single summary row for the hero banner."""
    if returns.is_empty():
        print("  [skip] summary — returns_master.csv missing")
        return

    total_returns     = len(returns)
    total_orgs        = returns["lobbyist_name"].drop_nulls().n_unique() if "lobbyist_name" in returns.columns else 0
    total_policy_areas = returns["public_policy_area"].drop_nulls().n_unique() if "public_policy_area" in returns.columns else 0
    total_politicians = pol["full_name"].drop_nulls().n_unique() if not pol.is_empty() and "full_name" in pol.columns else 0

    first_period = last_period = None
    if "lobbying_period_start_date" in returns.columns:
        dates = returns["lobbying_period_start_date"].drop_nulls()
        if len(dates):
            first_period = str(dates.min())[:7]
            last_period  = str(dates.max())[:7]

    _write(
        pl.DataFrame({
            "total_returns":              [total_returns],
            "total_orgs":                 [total_orgs],
            "total_politicians":          [total_politicians],
            "total_policy_areas":         [total_policy_areas],
            "first_period":               [first_period],
            "last_period":                [last_period],
            "source_summary":             ["lobbying.ie via lobby_processing.py"],
            "latest_fetch_timestamp_utc": [datetime.datetime.utcnow().isoformat()],
        }),
        "lobbying_summary.parquet",
    )


def _build_politician_index(pol: pl.DataFrame) -> None:
    """One row per politician — aggregated stats for the ranked leaderboard."""
    if pol.is_empty():
        print("  [skip] politician_index — politician_returns_detail.csv missing")
        return

    unique_cols = ["primary_key", "full_name", "lobbyist_name", "public_policy_area"]
    unique_cols = [c for c in unique_cols if c in pol.columns]
    deduped = pol.unique(subset=unique_cols, keep="first") if unique_cols else pol

    agg_exprs = [
        pl.col("primary_key").n_unique().alias("lobby_returns_targeting"),
        pl.col("lobbyist_name").n_unique().alias("distinct_orgs"),
        pl.col("public_policy_area").n_unique().alias("distinct_policy_areas"),
    ]
    if "lobbying_period_start_date" in deduped.columns:
        agg_exprs += [
            pl.col("lobbying_period_start_date").min().alias("first_period"),
            pl.col("lobbying_period_start_date").max().alias("last_period"),
        ]
    if "chamber" in deduped.columns:
        agg_exprs.append(pl.col("chamber").drop_nulls().first().alias("chamber"))
    if "position" in deduped.columns:
        agg_exprs.append(pl.col("position").drop_nulls().first().alias("position"))

    result = (
        deduped
        .group_by("full_name")
        .agg(agg_exprs)
        .sort("lobby_returns_targeting", descending=True)
    )
    _write(result, "lobbying_politician_index.parquet")


def _build_contact_detail(pol: pl.DataFrame) -> None:
    """
    One row per (politician, return, policy area).
    Supports both politician Stage 2 (filter by full_name)
    and org Stage 2 (filter by lobbyist_name).
    """
    if pol.is_empty():
        print("  [skip] contact_detail — politician_returns_detail.csv missing")
        return

    keep = [c for c in [
        "primary_key", "full_name", "chamber", "position",
        "lobbyist_name", "public_policy_area",
        "lobbying_period_start_date", "lobby_url",
    ] if c in pol.columns]

    unique_key = [c for c in ["primary_key", "full_name", "lobbyist_name", "public_policy_area"] if c in keep]

    detail = pol.select(keep).unique(subset=unique_key, keep="first")

    if "lobby_url" in detail.columns:
        detail = detail.rename({"lobby_url": "source_url"})

    if "lobbying_period_start_date" in detail.columns:
        detail = detail.sort("lobbying_period_start_date", descending=True, nulls_last=True)

    _write(detail, "lobbying_contact_detail.parquet")


def _build_policy_area_summary(returns: pl.DataFrame, pol: pl.DataFrame) -> None:
    """One row per policy area — return count, distinct orgs, distinct politicians."""
    # Try pre-computed breakdown first
    precomp = _policy_area_breakdown()
    if not precomp.is_empty() and {"public_policy_area", "return_count"}.issubset(set(precomp.columns)):
        base = precomp.select([c for c in ["public_policy_area", "return_count", "distinct_lobbyists"] if c in precomp.columns])
    elif not returns.is_empty() and "public_policy_area" in returns.columns:
        base = (
            returns
            .filter(pl.col("public_policy_area").is_not_null())
            .group_by("public_policy_area")
            .agg([
                pl.col("primary_key").n_unique().alias("return_count"),
                pl.col("lobbyist_name").n_unique().alias("distinct_lobbyists"),
            ])
        )
    else:
        print("  [skip] policy_area_summary — no source data")
        return

    # Add distinct_politicians from politician_returns_detail
    if not pol.is_empty() and {"full_name", "public_policy_area"}.issubset(set(pol.columns)):
        pol_agg = (
            pol
            .filter(pl.col("public_policy_area").is_not_null())
            .group_by("public_policy_area")
            .agg(pl.col("full_name").n_unique().alias("distinct_politicians"))
        )
        # Polars join — allowed in enrichment (not in SQL views)
        base = base.join(pol_agg, on="public_policy_area", how="left")

    result = (
        base
        .filter(pl.col("public_policy_area").is_not_null())
        .sort("return_count", descending=True)
    )
    _write(result, "lobbying_policy_area_summary.parquet")


def _build_org_index(pol: pl.DataFrame, counts: pl.DataFrame, registry: pl.DataFrame) -> None:
    """One row per org — aggregated stats + sector/website from counts and registry."""
    if pol.is_empty():
        print("  [skip] org_index — politician_returns_detail.csv missing")
        return

    unique_cols = [c for c in ["primary_key", "lobbyist_name", "full_name", "public_policy_area"] if c in pol.columns]
    deduped = pol.unique(subset=unique_cols, keep="first") if unique_cols else pol

    agg_exprs = [
        pl.col("primary_key").n_unique().alias("return_count"),
        pl.col("full_name").n_unique().alias("distinct_politicians_targeted"),
        pl.col("public_policy_area").n_unique().alias("distinct_policy_areas"),
    ]
    if "lobbying_period_start_date" in deduped.columns:
        agg_exprs += [
            pl.col("lobbying_period_start_date").min().alias("first_period"),
            pl.col("lobbying_period_start_date").max().alias("last_period"),
        ]

    agg = (
        deduped
        .filter(pl.col("lobbyist_name").is_not_null())
        .group_by("lobbyist_name")
        .agg(agg_exprs)
        .sort("return_count", descending=True)
    )

    # Enrich with sector + website from lobby_count_details.csv
    if not counts.is_empty() and "lobbyist_name" in counts.columns:
        count_keep = {
            "lobbyist_name": "lobbyist_name",
            "main_activities_of_organisation": "sector",
            "website": "website",
            "lobby_org_link": "profile_url",
        }
        count_cols = [c for c in count_keep if c in counts.columns]
        count_slim = (
            counts
            .select(count_cols)
            .rename({k: v for k, v in count_keep.items() if k in count_cols and k != v})
            .unique(subset=["lobbyist_name"], keep="first")
        )
        # Polars join — allowed in enrichment
        agg = agg.join(count_slim, on="lobbyist_name", how="left")

    # Fallback sector/website from bronze org registry
    if not registry.is_empty() and "lobbyist_name" in registry.columns:
        reg_keep = [c for c in ["lobbyist_name", "sector_reg", "website_reg"] if c in registry.columns]
        reg_slim = registry.select(reg_keep).unique(subset=["lobbyist_name"], keep="first")
        agg = agg.join(reg_slim, on="lobbyist_name", how="left")

        if "sector" not in agg.columns and "sector_reg" in agg.columns:
            agg = agg.rename({"sector_reg": "sector"})
        elif "sector" in agg.columns and "sector_reg" in agg.columns:
            agg = agg.with_columns(
                pl.when(pl.col("sector").is_null())
                .then(pl.col("sector_reg"))
                .otherwise(pl.col("sector"))
                .alias("sector")
            ).drop("sector_reg")

        if "website" not in agg.columns and "website_reg" in agg.columns:
            agg = agg.rename({"website_reg": "website"})
        elif "website" in agg.columns and "website_reg" in agg.columns:
            agg = agg.with_columns(
                pl.when(pl.col("website").is_null())
                .then(pl.col("website_reg"))
                .otherwise(pl.col("website"))
                .alias("website")
            ).drop("website_reg")

    # Ensure columns exist
    for col in ["sector", "website", "profile_url"]:
        if col not in agg.columns:
            agg = agg.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    _write(agg, "lobbying_org_index.parquet")


def _build_recent_returns(returns: pl.DataFrame) -> None:
    """200 most recent returns — the view LIMITs to 20."""
    if returns.is_empty():
        print("  [skip] recent_returns — returns_master.csv missing")
        return

    keep = [c for c in [
        "primary_key", "lobbying_period_start_date", "lobbyist_name",
        "public_policy_area", "relevant_matter", "lobby_url",
    ] if c in returns.columns]

    recent = (
        returns
        .select(keep)
        .filter(pl.col("lobbying_period_start_date").is_not_null())
        .sort("lobbying_period_start_date", descending=True)
        .head(200)
    )
    if "lobby_url" in recent.columns:
        recent = recent.rename({"lobby_url": "source_url"})

    _write(recent, "lobbying_recent_returns.parquet")


def _build_revolving_door(dpos: pl.DataFrame) -> None:
    """One row per former DPO — rename long column names to clean identifiers."""
    if dpos.is_empty():
        print("  [skip] revolving_door — experimental_revolving_door_dpos.csv missing")
        return

    name_col = "dpos_or_former_dpos_who_carried_out_lobbying_name"
    if name_col not in dpos.columns:
        print(f"  [skip] revolving_door — expected column '{name_col}' not found")
        return

    rename_map = {
        name_col: "individual_name",
        "current_or_former_dpos_position": "former_position",
        "current_or_former_dpos_chamber":  "former_chamber",
    }
    # returns_involved_in, distinct_lobbyist_firms, distinct_policy_areas already computed
    rename_map_clean = {k: v for k, v in rename_map.items() if k in dpos.columns}

    if "distinct_lobbyist_firms" in dpos.columns and "distinct_lobbyist_firms" not in rename_map_clean.values():
        rename_map_clean["distinct_lobbyist_firms"] = "distinct_firms"

    result = (
        dpos
        .rename(rename_map_clean)
        .filter(pl.col("individual_name").is_not_null() & (pl.col("individual_name") != ""))
    )
    if "returns_involved_in" in result.columns:
        result = result.sort("returns_involved_in", descending=True)

    _write(result, "lobbying_revolving_door.parquet")


# ── Entry point ────────────────────────────────────────────────────────────────

def enrich_lobbying() -> None:
    print("=" * 60)
    print("Lobbying enrichment")
    print(f"  source : {_SILVER}")
    print(f"  output : {_OUT}")
    print("=" * 60)

    returns = _returns_master()
    pol     = _politician_returns()
    counts  = _lobby_counts()
    dpos    = _revolving_door_dpos()
    reg     = _org_registry()

    _build_summary(returns, pol)
    _build_politician_index(pol)
    _build_contact_detail(pol)
    _build_policy_area_summary(returns, pol)
    _build_org_index(pol, counts, reg)
    _build_recent_returns(returns)
    _build_revolving_door(dpos)

    print("=" * 60)
    print("Enrichment complete.")
    print("=" * 60)


if __name__ == "__main__":
    enrich_lobbying()
