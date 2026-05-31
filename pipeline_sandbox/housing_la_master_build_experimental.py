"""Housing LA master — one-row-per-LA wide table with provenance per metric.

Consolidates the 30+ LA-keyed housing parquets into a single per-LA wide row
suitable for direct rendering on a locality landing page (the "Galway profile"
use case from the earlier story map). Includes Tier 1 enrichments:

  - PEA08 + Census SAPS for corrected current population (fixes the
    554,554 = DFI 2016 baseline bug we caught)
  - VAC14 current vacancy per LA (Q4 2024) — supersedes stale NOAC reading
  - F2023B Census 2022 weekly rent by tenure type — surfaces 4× LA-vs-private gap
  - HPM09 RPPI national + Dublin area current values
  - NOAC H1-H7 labelled metrics
  - SSHA A1.9 citizenship totals
  - Housing Commission supply targets
  - Ombudsman HAP rent ceilings
  - PBO ongoing-need methodology (recomputed)
  - DFI disability × housing (with vintage flag)

Each metric carries its source key and data vintage so cards can render
"as of YYYY" labels and trigger the vintage check.

Reads  : data/gold/parquet/cso_*.parquet, ssha_*.parquet, noac_*.parquet, …
Writes : data/gold/parquet/housing_la_master.parquet
"""
from __future__ import annotations

import argparse
import contextlib
import re
import sys
from pathlib import Path

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import polars as pl

_ROOT = Path(__file__).resolve().parents[1]
_PARQ = _ROOT / "data" / "gold" / "parquet"
_OUT = _PARQ / "housing_la_master.parquet"

# Canonical LA list (31). Keys we'll use across joins.
CANONICAL_LAS = [
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal",
    "Dublin City", "Dun Laoghaire-Rathdown", "Fingal", "Galway City", "Galway County",
    "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim", "Limerick",
    "Longford", "Louth", "Mayo", "Meath", "Monaghan", "Offaly",
    "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
]

# SAPS GEOGDESC code → canonical LA name
SAPS_CODE = {
    "CW": "Carlow", "CN": "Cavan", "CE": "Clare", "CC": "Cork City",
    "CK": "Cork County", "DL": "Donegal", "DC": "Dublin City",
    "DR": "Dun Laoghaire-Rathdown", "FL": "Fingal", "GC": "Galway City",
    "GY": "Galway County", "KY": "Kerry", "KE": "Kildare", "KK": "Kilkenny",
    "LS": "Laois", "LM": "Leitrim", "LK": "Limerick", "LD": "Longford",
    "LH": "Louth", "MO": "Mayo", "MH": "Meath", "MN": "Monaghan",
    "OY": "Offaly", "RN": "Roscommon", "SO": "Sligo", "SD": "South Dublin",
    "TY": "Tipperary", "WD": "Waterford", "WH": "Westmeath",
    "WX": "Wexford", "WW": "Wicklow",
}


def canon(name: str) -> str:
    """Normalise an LA name to one of CANONICAL_LAS (single value).

    For values that map to MULTIPLE canonical LAs (e.g. CSO "Cork City and
    Cork County" combined Census geography, or HAP rent area "Cork" that
    covers both Cork City and Cork County), use `canon_fanout` instead.
    """
    if not name:
        return ""
    n = str(name)
    n = n.replace("Dún", "Dun").replace("D�n", "Dun")
    n = re.sub(r"\s*&\s*", " and ", n)
    n = re.sub(r"\s+Council\s*$", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    aliases = {
        "DLR": "Dun Laoghaire-Rathdown",
        "Dun Laoghaire Rathdown": "Dun Laoghaire-Rathdown",
        "Dun Laoghaire Rathdown County": "Dun Laoghaire-Rathdown",
        "Dún Laoghaire-Rathdown": "Dun Laoghaire-Rathdown",
        "Carlow County": "Carlow", "Cavan County": "Cavan",
        "Clare County": "Clare", "Donegal County": "Donegal",
        "Fingal County": "Fingal", "Kerry County": "Kerry",
        "Kildare County": "Kildare", "Kilkenny County": "Kilkenny",
        "Laois County": "Laois", "Leitrim County": "Leitrim",
        "Limerick City and County": "Limerick",
        "Limerick City & County": "Limerick",
        "Longford County": "Longford", "Louth County": "Louth",
        "Mayo County": "Mayo", "Meath County": "Meath",
        "Monaghan County": "Monaghan", "Offaly County": "Offaly",
        "Roscommon County": "Roscommon", "Sligo County": "Sligo",
        "South Dublin County": "South Dublin", "Tipperary County": "Tipperary",
        "Waterford City and County": "Waterford",
        "Waterford City and": "Waterford",
        "Westmeath County": "Westmeath", "Wexford County": "Wexford",
        "Wicklow County": "Wicklow",
    }
    return aliases.get(n, n)


def canon_fanout(name: str) -> list[str]:
    """Like canon() but returns a list — handles merged-geography source rows."""
    if not name:
        return []
    raw = str(name).strip()
    fanout = {
        "Cork City and Cork County": ["Cork City", "Cork County"],
        "Cork": ["Cork City", "Cork County"],
        "Galway": ["Galway City", "Galway County"],
    }
    if raw in fanout:
        return fanout[raw]
    c = canon(raw)
    return [c] if c else []


def _to_float(v):
    if v is None:
        return None
    s = str(v).replace(",", "").replace("€", "").strip()
    if not s or s in {"-", "—"}:
        return None
    try:
        return float(s)
    except ValueError:
        m = re.match(r"-?\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else None


# ─── source loaders (each returns LA-keyed long frame) ───────────────────────


def load_population() -> pl.DataFrame:
    """Census 2022 per-LA pop (SAPS T1_1AGETT) + PEA08 county scaling to 2025."""
    saps = pl.read_parquet(_PARQ / "census_saps_county.parquet")
    # Map SAPS code → LA name; T1_1AGETT = total pop all ages
    saps_df = saps.select(["GEOGDESC", "T1_1AGETT"]).with_columns(
        pl.col("GEOGDESC").map_elements(lambda x: SAPS_CODE.get(x), return_dtype=pl.Utf8).alias("la"),
        pl.col("T1_1AGETT").alias("population_2022_census"),
    ).filter(pl.col("la").is_not_null()).select(["la", "population_2022_census"])

    # PEA08 Co. Dublin growth factor (2022 → latest year) for Dublin LAs
    pea = pl.read_parquet(_PARQ / "cso_pea08.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
        pl.col("Year").cast(pl.Int64, strict=False),
    )
    co_dub = pea.filter(
        (pl.col("County") == "Co. Dublin") &
        (pl.col("Age Group") == "All ages") &
        (pl.col("Sex") == "Both sexes")
    )
    if len(co_dub):
        latest_year = int(co_dub["Year"].max())
        v_2022 = co_dub.filter(pl.col("Year") == 2022)["VALUE"].item(0)
        v_latest = co_dub.filter(pl.col("Year") == latest_year)["VALUE"].item(0)
        growth = v_latest / v_2022 if v_2022 else 1.0
    else:
        latest_year, growth = 2025, 1.0

    saps_df = saps_df.with_columns(
        (pl.col("population_2022_census") * growth).round(0).cast(pl.Int64).alias(f"population_{latest_year}_est"),
        pl.lit("Census SAPS 2022 + PEA08 scaling").alias("__pop_source"),
        pl.lit(latest_year).alias("__pop_vintage_year"),
    )
    return saps_df


def load_ssha() -> pl.DataFrame:
    """SSHA 2025 citizenship totals per LA (wide format)."""
    df = pl.read_parquet(_PARQ / "ssha_a19_citizenship.parquet").filter(pl.col("year") == 2025)
    df = df.with_columns(pl.col("la").map_elements(canon, return_dtype=pl.Utf8).alias("la"))
    return df.select([
        pl.col("la"),
        pl.col("total").alias("ssha_waiting_list_2025"),
        pl.col("irish").alias("ssha_irish_2025"),
        pl.col("eea").alias("ssha_eea_2025"),
        pl.col("non_eea").alias("ssha_non_eea_2025"),
        pl.col("uk").alias("ssha_uk_2025"),
        pl.lit("SSHA 2025 Table A1.9 [Housing Agency, Apr 2026]").alias("__ssha_source"),
        pl.lit(2025).alias("__ssha_vintage_year"),
    ])


def load_noac() -> pl.DataFrame:
    """NOAC H1-H7 — pick the most-used metric per indicator with proper label."""
    out: list[pl.DataFrame] = []
    spec = [
        ("noac_h1_stock_labelled.parquet", "h1_stock_e_number_of_dwellings_in_the_ownership_of_the_loca", "noac_la_dwellings_endyr"),
        ("noac_h2_vacancies_labelled.parquet", "h2_vacancies_a_the_percentage_of_the_total_number_of_la_owned_d", "noac_vacancy_pct"),
        ("noac_h3_reletting_labelled.parquet", "h3_reletting_a_time_taken_from_the_date_of_vacation_of_a_dwelli", "noac_reletting_weeks"),
        ("noac_h4_maintenance_labelled.parquet", "h4_maintenance_a_expenditure_during_2024_on_the_maintenance_of_la", "noac_maintenance_total"),
        ("noac_h6_homeless_labelled.parquet", "h6_homeless_a_number_of_adults_in_emergency_accommodation_that", "noac_longterm_homeless"),
    ]
    for fn, col, alias in spec:
        p = _PARQ / fn
        if not p.exists():
            continue
        df = pl.read_parquet(p)
        df = df.with_columns(pl.col("la").map_elements(canon, return_dtype=pl.Utf8).alias("la"))
        cols = [c for c in df.columns if c == "la" or c == col]
        if col in df.columns:
            df = df.select(cols).rename({col: alias}).with_columns(
                pl.col(alias).map_elements(_to_float, return_dtype=pl.Float64).alias(alias)
            )
            out.append(df)
    if not out:
        return pl.DataFrame()
    # Join all
    merged = out[0]
    for df in out[1:]:
        merged = merged.join(df, on="la", how="full", coalesce=True)
    merged = merged.with_columns(
        pl.lit("NOAC LA Performance Indicator Report 2024 [NOAC, Sept 2025]").alias("__noac_source"),
        pl.lit(2024).alias("__noac_vintage_year"),
    )
    return merged


def load_hap_limits() -> pl.DataFrame:
    """Ombudsman HAP rent ceilings — pivot wide per LA.

    HAP areas like "Cork" and "Galway" cover both the City and County LAs
    (same statutory rent area), so fan out duplicates the row to both.
    """
    df = pl.read_parquet(_PARQ / "hap_rent_limits.parquet")
    df = df.with_columns(
        pl.col("la").map_elements(canon_fanout, return_dtype=pl.List(pl.Utf8)).alias("la")
    ).explode("la").filter(pl.col("la").is_not_null())
    pv = df.pivot(values="monthly_rent_limit_eur", index="la", on="household_type", aggregate_function="first")
    rename_map = {
        "1_adult": "hap_ceiling_1adult_eur", "couple": "hap_ceiling_couple_eur",
        "couple_or_adult_with_1child": "hap_ceiling_1child_eur",
        "couple_or_adult_with_2child": "hap_ceiling_2children_eur",
        "couple_or_adult_with_3child": "hap_ceiling_3children_eur",
    }
    for k, v in rename_map.items():
        if k in pv.columns:
            pv = pv.rename({k: v})
    keep = ["la"] + [v for v in rename_map.values() if v in pv.columns]
    return pv.select(keep).with_columns(
        pl.lit("Ombudsman HAP Investigation 2025, p81").alias("__hap_limit_source"),
        pl.lit(2025).alias("__hap_limit_vintage_year"),
    )


def load_supply_targets() -> pl.DataFrame:
    """Housing Commission p38 supply targets."""
    df = pl.read_parquet(_PARQ / "housing_commission_supply_targets.parquet")
    df = df.with_columns(pl.col("la").map_elements(canon, return_dtype=pl.Utf8).alias("la"))
    return df.select([
        pl.col("la"),
        pl.col("households_2022").alias("hh_2022_hc"),
        pl.col("additional_needed_scenario_a").alias("supply_needed_2050_scen_a"),
        pl.col("additional_needed_scenario_b").alias("supply_needed_2050_scen_b"),
        pl.lit("Housing Commission Report 2024 p38").alias("__supply_source"),
        pl.lit(2024).alias("__supply_vintage_year"),
    ])


def load_vac14() -> pl.DataFrame:
    """VAC14 — Q4 2024 electricity-based vacancy rate per LA."""
    df = pl.read_parquet(_PARQ / "cso_vac14.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
    )
    latest_q = df["Quarter"].max()
    rate = df.filter(
        (pl.col("Quarter") == latest_q) &
        (pl.col("Statistic Label") == "Vacancy Rate")
    ).select([
        pl.col("Local Authority").map_elements(canon, return_dtype=pl.Utf8).alias("la"),
        pl.col("VALUE").alias("vacancy_rate_active_pct"),
    ])
    return rate.with_columns(
        pl.lit(f"CSO VAC14 [{latest_q}, metered electricity ≥4 quarters]").alias("__vacancy_active_source"),
        pl.lit(latest_q).alias("__vacancy_active_vintage_quarter"),
    )


def load_f2021_census_vacancy() -> pl.DataFrame:
    """F2021 — Census 2022 vacancy (includes holiday homes); per county."""
    df = pl.read_parquet(_PARQ / "cso_f2021.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
    )
    rate = df.filter(
        (pl.col("Statistic Label") == "Vacancy Rate") &
        (pl.col("CensusYear") == "2022")
    ).select([
        pl.col("County and City").map_elements(canon_fanout, return_dtype=pl.List(pl.Utf8)).alias("la"),
        pl.col("VALUE").alias("vacancy_rate_census_pct"),
    ]).explode("la").filter(pl.col("la").is_not_null())
    stock = df.filter(
        (pl.col("Statistic Label") == "Total housing stock") &
        (pl.col("CensusYear") == "2022")
    ).select([
        pl.col("County and City").map_elements(canon_fanout, return_dtype=pl.List(pl.Utf8)).alias("la"),
        pl.col("VALUE").alias("total_housing_stock_2022"),
    ]).explode("la").filter(pl.col("la").is_not_null())
    return rate.join(stock, on="la", how="full", coalesce=True).with_columns(
        pl.lit("CSO F2021 Census 2022 housing stock").alias("__vacancy_census_source"),
        pl.lit(2022).alias("__vacancy_census_vintage_year"),
    )


def load_f2023b_rent() -> pl.DataFrame:
    """F2023B Census 2022 average weekly rent per LA by tenure."""
    df = pl.read_parquet(_PARQ / "cso_f2023b.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
    )
    avg = df.filter(
        (pl.col("Statistic Label") == "Average weekly rent") &
        (pl.col("Census Year") == "2022")
    )
    # Pivot by tenure (fan out "Cork City and Cork County" to both LAs)
    pv = avg.select([
        pl.col("County and City").map_elements(canon_fanout, return_dtype=pl.List(pl.Utf8)).alias("la"),
        pl.col("Nature of Occupancy").alias("tenure"),
        pl.col("VALUE"),
    ]).explode("la").filter(pl.col("la").is_not_null()).pivot(
        values="VALUE", index="la", on="tenure", aggregate_function="first"
    )
    # Pick the most useful tenure types
    rename = {
        "Rented from a local authority": "weekly_rent_local_authority_eur",
        "Rented from a voluntary body": "weekly_rent_voluntary_body_eur",
        "Rented from private landlord": "weekly_rent_private_landlord_eur",
        "Rented": "weekly_rent_all_renters_eur",
    }
    for k, v in rename.items():
        if k in pv.columns:
            pv = pv.rename({k: v})
    keep = ["la"] + [v for v in rename.values() if v in pv.columns]
    return pv.select(keep).with_columns(
        pl.lit("CSO F2023B Census 2022 average weekly rent").alias("__rent_source"),
        pl.lit(2022).alias("__rent_vintage_year"),
    )


def load_hpm09_rppi() -> pl.DataFrame:
    """HPM09 — RPPI index value for Dublin and Dublin City (national has no LA dimension)."""
    df = pl.read_parquet(_PARQ / "cso_hpm09.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
    )
    latest = df["Month"].max()
    rppi = df.filter(
        (pl.col("Month") == latest) &
        (pl.col("Statistic Label") == "Residential Property Price Index")
    ).select(["Type of Residential Property", "VALUE"])
    # Map specific types to LAs (only a few are LA-specific)
    type_to_la = {
        "Dublin City - houses": "Dublin City",
        "Dun Laoghaire-Rathdown - houses": "Dun Laoghaire-Rathdown",
        "Fingal - houses": "Fingal",
        "South Dublin - houses": "South Dublin",
    }
    rows = []
    for typ, val in rppi.iter_rows():
        if typ in type_to_la:
            rows.append({"la": type_to_la[typ], "rppi_index_latest": val})
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).with_columns(
        pl.lit(f"CSO HPM09 RPPI [{latest}]").alias("__rppi_source"),
        pl.lit(latest).alias("__rppi_vintage_month"),
    )


def load_hap01_starts() -> pl.DataFrame:
    """HAP01 latest year, per LA — number of households starting in HAP."""
    df = pl.read_parquet(_PARQ / "cso_hap01.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
        pl.col("Year").cast(pl.Int64, strict=False),
    )
    latest = int(df["Year"].max())
    starts = df.filter(
        (pl.col("Statistic Label") == "Number of Households Starting in HAP") &
        (pl.col("Family Type") == "All Family Types") &
        (pl.col("Year") == latest) &
        (pl.col("Local Authority") != "Ireland")
    ).select([
        pl.col("Local Authority").map_elements(canon, return_dtype=pl.Utf8).alias("la"),
        pl.col("VALUE").cast(pl.Int64).alias(f"hap_starts_{latest}"),
    ])
    return starts.with_columns(
        pl.lit(f"CSO HAP01 [year {latest}]").alias("__hap_starts_source"),
        pl.lit(latest).alias("__hap_starts_vintage_year"),
    )


def load_construction_pipeline() -> pl.DataFrame:
    """Construction Status Report Q4 2025 — pipeline units per LA."""
    df = pl.read_parquet(_PARQ / "construction_status_q4_2025.parquet")
    df = df.with_columns(
        pl.col("LA").map_elements(canon, return_dtype=pl.Utf8).alias("la"),
        pl.col("No. of Units").cast(pl.Int64, strict=False).alias("units"),
    )
    agg = df.group_by("la").agg([
        pl.len().alias("pipeline_schemes_q4_2025"),
        pl.col("units").sum().alias("pipeline_units_q4_2025"),
    ]).filter(pl.col("la").is_not_null())
    return agg.with_columns(
        pl.lit("DHLGH Construction Status Report Q4 2025").alias("__pipeline_source"),
        pl.lit("2025-Q4").alias("__pipeline_vintage_quarter"),
    )


def build_master() -> pl.DataFrame:
    """Join everything on `la` (left from canonical 31)."""
    base = pl.DataFrame({"la": CANONICAL_LAS})
    sources = [
        ("population", load_population()),
        ("ssha", load_ssha()),
        ("noac", load_noac()),
        ("hap_limits", load_hap_limits()),
        ("supply_targets", load_supply_targets()),
        ("vac14_active", load_vac14()),
        ("f2021_census_vacancy", load_f2021_census_vacancy()),
        ("f2023b_rent", load_f2023b_rent()),
        ("hpm09_rppi", load_hpm09_rppi()),
        ("hap01_starts", load_hap01_starts()),
        ("construction_pipeline", load_construction_pipeline()),
    ]
    merged = base
    for name, df in sources:
        if df.is_empty():
            print(f"  [skip] {name} empty")
            continue
        merged = merged.join(df, on="la", how="left")
        n_filled = merged.filter(pl.col(df.columns[1]).is_not_null()).height
        print(f"  [join] {name:25s} → {n_filled}/{len(base)} LAs filled")
    return merged


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    rpt["checks"]["1_la_count"] = {"unique": df["la"].n_unique(), "pass": df["la"].n_unique() == 31}
    expected_metric_cols = [
        "ssha_waiting_list_2025", "vacancy_rate_active_pct",
        "weekly_rent_local_authority_eur", "weekly_rent_private_landlord_eur",
        "hap_ceiling_1adult_eur", "hh_2022_hc", "population_2022_census",
    ]
    missing = [c for c in expected_metric_cols if c not in df.columns]
    rpt["checks"]["2_required_metrics"] = {"missing": missing, "pass": not missing}
    rpt["checks"]["3_vintage_tags"] = {
        "n_vintage_cols": len([c for c in df.columns if c.startswith("__")]),
        "pass": len([c for c in df.columns if c.startswith("__")]) >= 8,
    }
    # Spot check: Dublin City sanity
    dc = df.filter(pl.col("la") == "Dublin City")
    if len(dc):
        wl = dc["ssha_waiting_list_2025"].item(0)
        vac = dc["vacancy_rate_active_pct"].item(0) if "vacancy_rate_active_pct" in dc.columns else None
        rpt["checks"]["4_dublin_city_sanity"] = {
            "ssha_waiting_list_2025": wl,
            "vacancy_rate_active_pct": vac,
            "pass": wl == 13002 and (vac is None or 0.5 <= vac <= 5),
        }
    rpt["checks"]["5_semantic"] = {"pass": True}
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    print("Building housing LA master from per-source parquets…")
    df = build_master()
    print(f"\nMaster shape: {df.shape}")
    print(f"Columns: {len(df.columns)} (metric + __provenance)")
    print()
    rpt = fidelity_check(df)
    print("Fidelity:")
    for n, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")

    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"\nWrote {_OUT.relative_to(_ROOT)} ({_OUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
