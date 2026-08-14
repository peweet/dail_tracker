"""Native Polars expressions for the planning appeal-outcomes extractor.

This module is intentionally small and deterministic.  It contains the columnar
transformations and the spatial-temporal candidate join, while network fetching,
artifact writes, coverage metadata and product copy remain in
``planning_appeal_outcomes``.  Keeping this seam small also makes mutation testing
practical.
"""

from __future__ import annotations

import polars as pl

SPATIAL_DEG = 0.0006
SPATIAL_DEG_WIDE = 0.0015
GRID = 0.002
MAX_LOOKBACK_YEARS = 5

OUT_COLS = [
    "ApplicationNumber",
    "PlanningAuthority",
    "decision_normalised",
    "AppealRefNumber",
    "abp_case",
    "council_decision",
    "abp_decision",
    "PLANINGATY",
    "CATEGORY",
    "DECIDED_ON",
    "match_method",
]


def authority_key_expr(column: str | pl.Expr) -> pl.Expr:
    """Return the existing authority join-key normalization as a native expression."""
    expr = pl.col(column) if isinstance(column, str) else column
    return (
        expr.fill_null("")
        .str.replace(r"\s*-\s*.*$", "")
        .str.normalize("NFKD")
        .str.replace_all(r"\p{M}", "")
        .str.to_lowercase()
        .str.replace_all(r"[^a-z]", "")
    )


def epoch_ms_date_expr(column: str | pl.Expr) -> pl.Expr:
    """Convert an ArcGIS UTC epoch-millisecond column to a calendar date."""
    expr = pl.col(column) if isinstance(column, str) else column
    return pl.from_epoch(expr, time_unit="ms").dt.date()


def council_decision_expr(column: str | pl.Expr) -> pl.Expr:
    """Map the council register vocabulary to GRANT / REFUSE / OTHER."""
    expr = pl.col(column) if isinstance(column, str) else column
    return (
        pl.when(expr.is_in(["Granted", "Granted-Conditional"]))
        .then(pl.lit("GRANT"))
        .when(expr == "Refused")
        .then(pl.lit("REFUSE"))
        .otherwise(pl.lit("OTHER"))
    )


def case_status_expr(decision_column: str, decided_column: str) -> pl.Expr:
    """Classify a case as live using the extractor's existing two-part rule."""
    decision = pl.col(decision_column).fill_null("")
    return (
        pl.when(pl.col(decided_column).is_null() | decision.str.contains(r"(?i)^\s*case is due to be decided"))
        .then(pl.lit("live"))
        .otherwise(pl.lit("decided"))
    )


def appeal_case_expr(column: str | pl.Expr) -> pl.Expr:
    """Extract the first six-digit ACP identifier from an appeal-reference field."""
    expr = pl.col(column) if isinstance(column, str) else column
    return expr.str.extract(r"(\d{6})", 1)


def spatial_temporal_matches(residual: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """Vectorized equivalent of the validated spatial-temporal fallback.

    Ordering fields are explicit because the former Python implementation used
    first-in-pool behavior to resolve equal-date candidates: tight before wide,
    then neighbouring-cell traversal order, then source application order.
    """
    schema = {
        "ApplicationNumber": pl.Utf8,
        "PlanningAuthority": pl.Utf8,
        "decision_normalised": pl.Utf8,
        "AppealRefNumber": pl.Utf8,
        "abp_case": pl.Utf8,
        "council_decision": pl.Utf8,
        "abp_decision": pl.Utf8,
        "PLANINGATY": pl.Utf8,
        "CATEGORY": pl.Utf8,
        "DECIDED_ON": pl.Int64,
        "match_method": pl.Utf8,
    }
    if residual.is_empty() or apps.is_empty():
        return pl.DataFrame(schema=schema)

    candidates = (
        apps.with_row_index("_application_order")
        .filter(pl.col("lon").is_not_null() & pl.col("lat").is_not_null())
        .with_columns(
            authority_key_expr("PlanningAuthority").alias("auth_key"),
            (pl.col("lat") / GRID).round(0).cast(pl.Int64).alias("_grid_lat"),
            (pl.col("lon") / GRID).round(0).cast(pl.Int64).alias("_grid_lon"),
        )
        .rename({"lat": "_application_lat", "lon": "_application_lon"})
    )
    indexed = residual.with_row_index("_residual_order").with_columns(
        (pl.col("lat") / GRID).round(0).cast(pl.Int64).alias("_base_grid_lat"),
        (pl.col("lon") / GRID).round(0).cast(pl.Int64).alias("_base_grid_lon"),
        pl.col("lodged_date").cast(pl.Date).alias("lodged_date"),
    )
    offsets = pl.DataFrame(
        {
            "_neighbour_order": range(9),
            "_lat_offset": [-1, -1, -1, 0, 0, 0, 1, 1, 1],
            "_lon_offset": [-1, 0, 1, -1, 0, 1, -1, 0, 1],
        }
    )
    pairs = (
        indexed.join(offsets, how="cross")
        .with_columns(
            (pl.col("_base_grid_lat") + pl.col("_lat_offset")).alias("_grid_lat"),
            (pl.col("_base_grid_lon") + pl.col("_lon_offset")).alias("_grid_lon"),
        )
        .join(candidates, on=["auth_key", "_grid_lat", "_grid_lon"], how="inner")
        .with_columns(
            (pl.col("_application_lat") - pl.col("lat")).abs().alias("_lat_delta"),
            (pl.col("_application_lon") - pl.col("lon")).abs().alias("_lon_delta"),
            (
                (pl.col("_application_lat") - pl.col("lat")) ** 2 + (pl.col("_application_lon") - pl.col("lon")) ** 2
            ).alias("_distance_sq"),
            (pl.col("lodged_date") - pl.duration(days=365 * MAX_LOOKBACK_YEARS)).alias("_cutoff"),
        )
    )

    dated = (
        pairs.filter(
            pl.col("lodged_date").is_not_null()
            & pl.col("DecisionDate").is_not_null()
            & (pl.col("DecisionDate") >= pl.col("_cutoff"))
            & (pl.col("DecisionDate") <= pl.col("lodged_date"))
            & (pl.col("_lat_delta") <= SPATIAL_DEG_WIDE)
            & (pl.col("_lon_delta") <= SPATIAL_DEG_WIDE)
        )
        .with_columns(
            pl.when((pl.col("_lat_delta") <= SPATIAL_DEG) & (pl.col("_lon_delta") <= SPATIAL_DEG))
            .then(0)
            .otherwise(1)
            .alias("_radius_band")
        )
        .sort(
            ["_residual_order", "_radius_band", "DecisionDate", "_neighbour_order", "_application_order"],
            descending=[False, False, True, False, False],
        )
        .unique("_residual_order", keep="first", maintain_order=True)
    )
    undated = (
        pairs.filter(
            pl.col("lodged_date").is_null()
            & (pl.col("_lat_delta") <= SPATIAL_DEG)
            & (pl.col("_lon_delta") <= SPATIAL_DEG)
        )
        .sort(["_residual_order", "_distance_sq", "_neighbour_order", "_application_order"])
        .unique("_residual_order", keep="first", maintain_order=True)
    )
    selected = pl.concat([dated, undated], how="diagonal_relaxed").sort("_residual_order")
    if selected.is_empty():
        return pl.DataFrame(schema=schema)

    return selected.select(
        "ApplicationNumber",
        "PlanningAuthority",
        "decision_normalised",
        pl.lit(None, dtype=pl.Utf8).alias("AppealRefNumber"),
        "abp_case",
        council_decision_expr("decision_normalised").alias("council_decision"),
        "abp_decision",
        "PLANINGATY",
        "CATEGORY",
        "DECIDED_ON",
        pl.lit("spatial_temporal").alias("match_method"),
    ).select(OUT_COLS)
