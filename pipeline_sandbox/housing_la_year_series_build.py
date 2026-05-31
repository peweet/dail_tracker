"""Housing LA year-series — one row per (LA × year) trend metric.

Companion to housing_la_master.parquet (which is point-in-time wide).
This is the year-axis: how does each LA evolve over time?

Index    : (la, year)  for years 2017–2025 (PEA08 max year)
Metrics  :
  population_est        PEA08 county growth × Census 2022 SAPS per-LA baseline.
  hap_starts            HAP01 "Number of Households Starting in HAP" (All Family Types).
  hap_in_scheme         HAP01 "Number of Households in HAP".
  hap_exits             HAP01 "Number of Households Exiting HAP".
  vacancy_rate_pct      VAC14 Vacancy Rate (Q4 of each year, 2022/2023/2024 only).

Sparsity by design:
  HAP01 covers 2014–2022 (DHLGH publication lag)
  VAC14 covers 2022Q4–2024Q4 (3 year-points)
  PEA08 covers 2017–2025 (the index dimension)

Each metric carries a __<metric>_source string column; vintage is the `year`
column itself, so no per-metric vintage is needed.

Reads  : data/gold/parquet/{cso_pea08, cso_hap01, cso_vac14, census_saps_county}.parquet
Writes : data/gold/parquet/housing_la_year_series.parquet
"""
from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import polars as pl

# Share canon() + SAPS_CODE + CANONICAL_LAS from the master builder
sys.path.insert(0, str(Path(__file__).resolve().parent))
from housing_la_master_build import CANONICAL_LAS, SAPS_CODE, canon  # noqa: E402

_ROOT = Path(__file__).resolve().parents[1]
_PARQ = _ROOT / "data" / "gold" / "parquet"
_OUT = _PARQ / "housing_la_year_series.parquet"

# Map "Co. <X>" in PEA08 → list of LAs whose population it covers.
# When a county splits into multiple LAs, all LAs share the same county
# growth factor (Census 2022 SAPS ratio held constant year-to-year).
PEA08_COUNTY_TO_LAS = {
    "Co. Carlow":    ["Carlow"],         "Co. Cavan":     ["Cavan"],
    "Co. Clare":     ["Clare"],          "Co. Donegal":   ["Donegal"],
    "Co. Kerry":     ["Kerry"],          "Co. Kildare":   ["Kildare"],
    "Co. Kilkenny":  ["Kilkenny"],       "Co. Laois":     ["Laois"],
    "Co. Leitrim":   ["Leitrim"],        "Co. Limerick":  ["Limerick"],
    "Co. Longford":  ["Longford"],       "Co. Louth":     ["Louth"],
    "Co. Mayo":      ["Mayo"],           "Co. Meath":     ["Meath"],
    "Co. Monaghan":  ["Monaghan"],       "Co. Offaly":    ["Offaly"],
    "Co. Roscommon": ["Roscommon"],      "Co. Sligo":     ["Sligo"],
    "Co. Tipperary": ["Tipperary"],      "Co. Waterford": ["Waterford"],
    "Co. Westmeath": ["Westmeath"],      "Co. Wexford":   ["Wexford"],
    "Co. Wicklow":   ["Wicklow"],
    "Co. Cork":      ["Cork City", "Cork County"],
    "Co. Galway":    ["Galway City", "Galway County"],
    "Co. Dublin":    ["Dublin City", "Dun Laoghaire-Rathdown",
                       "Fingal", "South Dublin"],
}


def load_saps_baseline() -> pl.DataFrame:
    """Census 2022 SAPS per LA — used as the per-LA baseline for population scaling."""
    saps = pl.read_parquet(_PARQ / "census_saps_county.parquet")
    return saps.select(["GEOGDESC", "T1_1AGETT"]).with_columns(
        pl.col("GEOGDESC").map_elements(lambda x: SAPS_CODE.get(x), return_dtype=pl.Utf8).alias("la"),
        pl.col("T1_1AGETT").cast(pl.Int64).alias("pop_2022_saps"),
    ).filter(pl.col("la").is_not_null()).select(["la", "pop_2022_saps"])


def load_population_series() -> pl.DataFrame:
    """Per-(LA, year) population: SAPS 2022 baseline × PEA08 county growth ratio.

    Unit-agnostic: growth_ratio = pea_value(year) / pea_value(2022), so PEA08's
    "thousands" unit cancels. Census 2022 SAPS is in absolute persons, which
    sets the scale. For multi-LA counties (Dublin/Cork/Galway), all LAs share
    the same county growth factor (intra-county distribution held constant).
    """
    saps = load_saps_baseline()
    pea = pl.read_parquet(_PARQ / "cso_pea08.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
        pl.col("Year").cast(pl.Int64, strict=False),
    ).filter(
        (pl.col("Age Group") == "All ages") &
        (pl.col("Sex") == "Both sexes") &
        (pl.col("County") != "Ireland")
    )
    base_2022 = pea.filter(pl.col("Year") == 2022).select([
        "County", pl.col("VALUE").alias("county_pop_2022"),
    ])
    pea = pea.join(base_2022, on="County", how="left").with_columns(
        (pl.col("VALUE") / pl.col("county_pop_2022")).alias("growth_ratio"),
    )

    rows = []
    for county, las in PEA08_COUNTY_TO_LAS.items():
        c_sub = pea.filter(pl.col("County") == county).select(["Year", "growth_ratio"])
        if c_sub.is_empty():
            continue
        for la in las:
            base_row = saps.filter(pl.col("la") == la)
            if base_row.is_empty():
                continue
            base = base_row["pop_2022_saps"].item(0)
            for yr, ratio in c_sub.iter_rows():
                rows.append({
                    "la": la, "year": yr,
                    "population_est": int(round(base * ratio)),
                })
    return pl.DataFrame(rows).with_columns(
        pl.lit("Census 2022 SAPS × PEA08 county growth ratio").alias("__population_source"),
    )


def load_hap01_series() -> pl.DataFrame:
    """HAP01 per (LA, year) — starts / in / exits for All Family Types."""
    df = pl.read_parquet(_PARQ / "cso_hap01.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
        pl.col("Year").cast(pl.Int64, strict=False),
    ).filter(
        (pl.col("Family Type") == "All Family Types") &
        (pl.col("Local Authority") != "Ireland")
    )
    label_map = {
        "Number of Households Starting in HAP": "hap_starts",
        "Number of Households in HAP": "hap_in_scheme",
        "Number of Households Exiting HAP": "hap_exits",
    }
    out = []
    for stat, alias in label_map.items():
        sub = df.filter(pl.col("Statistic Label") == stat).select([
            pl.col("Local Authority").map_elements(canon, return_dtype=pl.Utf8).alias("la"),
            pl.col("Year").alias("year"),
            pl.col("VALUE").cast(pl.Int64).alias(alias),
        ])
        out.append(sub)
    merged = out[0]
    for s in out[1:]:
        merged = merged.join(s, on=["la", "year"], how="full", coalesce=True)
    return merged.with_columns(
        pl.lit("CSO HAP01 [DHLGH; years 2014–2022]").alias("__hap_source"),
    )


def load_hc_grants_series() -> pl.DataFrame:
    """Housing Commission p210 — per-LA housing grants spend, 2018-2022.

    Source uses 'Dublin' for Dublin City (DLR/Fingal/South Dublin listed
    separately, so the standalone 'Dublin' is unambiguous). 'Cork' and
    'Galway' are aggregated (City + County), so we deliberately leave the
    four constituent LAs as null — splitting € spend by a population share
    would be a fabrication.
    """
    df = pl.read_parquet(_PARQ / "housing_commission_la_grants_2018_2022.parquet")
    alias = {"Dublin": "Dublin City"}
    df = df.with_columns(
        pl.col("la").map_elements(lambda x: alias.get(x, x), return_dtype=pl.Utf8).alias("la"),
    ).filter(pl.col("la").is_in(CANONICAL_LAS))
    return df.rename({"grants_eur": "hc_la_grants_eur"}).with_columns(
        pl.lit("Housing Commission Report 2024 p210 [years 2018–2022]").alias("__hc_grants_source"),
    )


def load_vac14_series() -> pl.DataFrame:
    """VAC14 vacancy rate — Q4 of each year, mapped to year integer."""
    df = pl.read_parquet(_PARQ / "cso_vac14.parquet").with_columns(
        pl.col("VALUE").cast(pl.Float64, strict=False),
    ).filter(pl.col("Statistic Label") == "Vacancy Rate")
    df = df.with_columns(
        pl.col("Quarter").str.slice(0, 4).cast(pl.Int64).alias("year"),
        pl.col("Local Authority").map_elements(canon, return_dtype=pl.Utf8).alias("la"),
        pl.col("VALUE").alias("vacancy_rate_pct"),
    )
    return df.select(["la", "year", "vacancy_rate_pct"]).with_columns(
        pl.lit("CSO VAC14 [metered-electricity vacancy, Q4 each year]").alias("__vacancy_source"),
    )


def build_series() -> pl.DataFrame:
    pop = load_population_series()
    hap = load_hap01_series()
    vac = load_vac14_series()
    grants = load_hc_grants_series()

    years = sorted(pop["year"].unique().to_list())
    idx = pl.DataFrame({
        "la":   [la for la in CANONICAL_LAS for _ in years],
        "year": [y for _ in CANONICAL_LAS for y in years],
    })
    print(f"  Index: {idx.shape} ({len(CANONICAL_LAS)} LAs × {len(years)} years)")

    out = idx.join(pop,    on=["la", "year"], how="left")
    out = out.join(hap,    on=["la", "year"], how="left")
    out = out.join(vac,    on=["la", "year"], how="left")
    out = out.join(grants, on=["la", "year"], how="left")
    return out


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    rpt["checks"]["1_index_complete"] = {
        "rows": len(df), "expected": 31 * 9,
        "pass": len(df) == 31 * 9,
    }
    rpt["checks"]["2_pop_coverage"] = {
        "filled": df.filter(pl.col("population_est").is_not_null()).height,
        "pass": df.filter(pl.col("population_est").is_not_null()).height == 31 * 9,
    }
    # HAP01: 2014-2022 ∩ 2017-2025 = 2017-2022 (6 years × 31 LAs = 186)
    hap_filled = df.filter(pl.col("hap_starts").is_not_null()).height
    rpt["checks"]["3_hap_coverage"] = {
        "filled_rows": hap_filled,
        "expected_min": 31 * 5,  # at least 5 years overlap
        "pass": hap_filled >= 31 * 5,
    }
    # Dublin City sanity: 2025 population should be ~630k (≠ 554k!)
    dc25 = df.filter((pl.col("la") == "Dublin City") & (pl.col("year") == 2025))
    pop_2025 = dc25["population_est"].item(0) if len(dc25) else None
    rpt["checks"]["4_dublin_2025_pop"] = {
        "value": pop_2025,
        "pass": pop_2025 is not None and 600_000 <= pop_2025 <= 700_000,
    }
    # Dublin City monotonic-ish population (allow ±0.5% noise)
    dc = df.filter(pl.col("la") == "Dublin City").sort("year")
    dc_pops = dc["population_est"].to_list()
    growth = [b / a for a, b in zip(dc_pops, dc_pops[1:]) if a]
    rpt["checks"]["5_dublin_growth_plausible"] = {
        "yearly_growth_factors": [round(g, 3) for g in growth],
        "pass": all(0.99 <= g <= 1.05 for g in growth),
    }
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    print("Building housing LA year-series …")
    df = build_series()
    print(f"\nShape: {df.shape}   columns: {df.columns}")

    rpt = fidelity_check(df)
    print("\nFidelity:")
    for n, chk in rpt["checks"].items():
        tag = "GREEN" if chk.get("pass") else "FAIL"
        print(f"  [{tag}] {n}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")

    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"\nWrote {_OUT.relative_to(_ROOT)} ({_OUT.stat().st_size:,} bytes)")

    # Show Dublin City trend table
    if "population_est" in df.columns:
        dc = df.filter(pl.col("la") == "Dublin City").sort("year")
        print("\nDublin City trend:")
        cols = ["year", "population_est", "hap_starts", "hap_in_scheme",
                "hap_exits", "vacancy_rate_pct", "hc_la_grants_eur"]
        print(dc.select([c for c in cols if c in dc.columns]))


if __name__ == "__main__":
    main()
