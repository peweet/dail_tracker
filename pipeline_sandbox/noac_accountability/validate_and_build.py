"""EXPERIMENTAL sandbox — validate each scorecard metric on real data, then build the keeper.

Runs an end-to-end check per metric (join, column, distribution, direction, service-null)
and only writes the tidy keeper file if every join resolves. NOTHING here touches gold.

Outputs (sandbox dir):
  noac_council_scorecard.parquet  — long: local_authority, metric_key, value, year
  scorecard_meta.csv              — one row per metric: label/unit/direction/page/deep_link/caption
"""
from __future__ import annotations

import unicodedata
from pathlib import Path

import polars as pl

D = Path(__file__).resolve().parent
PDF = "https://cdn.noac.ie/wp-content/uploads/2025/09/NOAC-Local-Authority-Performance-Indicator-Report-2024.pdf"

# Canonical NOAC-name -> page `local_authority` key. Source: la_map in
# sql_views/constituency/constituency_council_housing_performance.sql (noac_la -> local_authority).
# Keyed by accent-folded NOAC row label; 'DLR' aliased (M1 table spells it 'DLR').
_PAIRS = {
    "Carlow County": "Carlow", "Cavan County": "Cavan", "Clare County": "Clare",
    "Cork City": "Cork City", "Cork County": "Cork County", "Donegal County": "Donegal",
    "Dublin City": "Dublin City", "Dun Laoghaire-Rathdown": "Dun Laoghaire-Rathdown",
    "Fingal County": "Fingal", "Galway City": "Galway City", "Galway County": "Galway County",
    "Kerry County": "Kerry", "Kildare County": "Kildare", "Kilkenny County": "Kilkenny",
    "Laois County": "Laois", "Leitrim County": "Leitrim", "Limerick City and County": "Limerick",
    "Longford County": "Longford", "Louth County": "Louth", "Mayo County": "Mayo",
    "Meath County": "Meath", "Monaghan County": "Monaghan", "Offaly County": "Offaly",
    "Roscommon County": "Roscommon", "Sligo County": "Sligo", "South Dublin County": "South Dublin",
    "Tipperary County": "Tipperary", "Waterford City and County": "Waterford",
    "Westmeath County": "Westmeath", "Wexford County": "Wexford", "Wicklow County": "Wicklow",
}
_ALIAS = {"DLR": "Dun Laoghaire-Rathdown"}


def _fold(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


_MAP = {_fold(k): v for k, v in _PAIRS.items()}


def to_local_authority(noac_name: str) -> str | None:
    n = _fold(_ALIAS.get(noac_name.strip(), noac_name.strip()))
    return _MAP.get(n)


# metric_key, file, value column, label, unit, direction (good=), page (printed), caption
KEEPERS = [
    ("revenue_balance_pct", "noac_m1_revenue_balance", "balance_pct_of_income",
     "Revenue balance", "% of income", "higher", 183,
     "Cumulative surplus (or deficit) on the revenue account, as a share of income."),
    ("sickness_absence_pct", "noac_c2_sickness_absence", "pct_days_lost_sickness_certified",
     "Sick-leave lost", "% days", "lower", 168,
     "Paid working days lost to medically-certified sickness absence."),
    ("roads_poor_pct", "noac_r1_pavement_condition", "pct_primary_psci_1_4_poor",
     "Roads in poor condition", "% primary roads", "lower", 61,
     "Share of local primary roads rated poor (PSCI 1-4)."),
    ("fire_within_10min_pct", "noac_f3_fire_attendance", "pct_fire_attendance_within_10min",
     "Fires reached in 10 min", "% of fires", "higher", 132,
     "Share of fire incidents where the brigade reached the scene within 10 minutes."),
    # Derived: % area moderately+ polluted. Robust vs pct_area_unpolluted, which is noisy
    # because councils split arbitrarily between "unpolluted" and "slightly polluted".
    ("litter_problem_pct", "noac_e3_litter_pollution",
     ("pct_area_moderately_polluted", "pct_area_significantly_polluted", "pct_area_grossly_polluted"),
     "Area with a litter problem", "% of area", "lower", 97,
     "Share of surveyed area graded moderately, significantly or grossly polluted."),
    # Water (W1) dropped from the headline scorecard: low spread (95-100%), and it covers
    # only the residual private-scheme role — public supply is Uisce Éireann's, not the
    # council's (NOAC p83), so a "drinking water" tile would imply an accountability the
    # council does not hold. Available in the full drill-down only.
]

# Service-null: councils with no own service (data-driven, not hardcoded).
FIRE_METRICS = {"fire_within_10min_pct"}
WATER_METRICS = {"water_compliant_pct"}


def _service_null_sets() -> tuple[set[str], set[str]]:
    f1 = pl.read_parquet(D / "noac_f1_fire_cost_per_capita_wide.parquet")
    fire = {to_local_authority(n) for n in f1.filter(pl.col("fire_cost_per_capita_eur") == 0)["la"]}
    w1 = pl.read_parquet(D / "noac_w1_water_compliance_wide.parquet")
    water = {to_local_authority(n) for n in w1.filter(pl.col("pct_private_scheme_drinking_water_compliant") == 0)["la"]}
    return fire, water


def main() -> None:
    fire_null, water_null = _service_null_sets()
    print(f"service-null  fire={sorted(fire_null)}\n              water={sorted(water_null)}\n")

    long_rows, meta_rows = [], []
    crosswalk = set(_PAIRS.values())
    ok = True
    for key, fname, col, label, unit, good, page, caption in KEEPERS:
        df = pl.read_parquet(D / f"{fname}_wide.parquet").filter(pl.col("year") == 2024)
        df = df.with_columns(pl.col("la").map_elements(to_local_authority, return_dtype=pl.Utf8).alias("local_authority"))

        # --- JOIN VALIDATION ---
        unmatched = df.filter(pl.col("local_authority").is_null())["la"].to_list()
        las = set(df["local_authority"].drop_nulls())
        n, dupes = df.height, df.height - df["local_authority"].n_unique()
        join_ok = not unmatched and not dupes and las == crosswalk
        ok &= join_ok

        # base value: a single column, or the sum of several (derived metric)
        base = (sum((pl.col(c).fill_null(0) for c in col[1:]), pl.col(col[0]).fill_null(0))
                if isinstance(col, tuple) else pl.col(col))
        df = df.with_columns(base.alias("_base"))

        # --- SERVICE NULL ---
        null_set = fire_null if key in FIRE_METRICS else water_null if key in WATER_METRICS else set()
        df = df.with_columns(
            pl.when(pl.col("local_authority").is_in(list(null_set))).then(None).otherwise(pl.col("_base")).alias("value")
        )
        live = df.filter(pl.col("value").is_not_null())
        s = live["value"]
        med = float(s.median())
        worst = live.sort("value", descending=(good == "lower")).head(3)
        best = live.sort("value", descending=(good == "higher")).head(3)

        print(f"=== {key}  [{label}, good={good}, {unit}] ===")
        print(f"  JOIN: {n} rows -> {len(las)}/31 matched, dupes={dupes}, unmatched={unmatched}  {'OK' if join_ok else '*** FAIL ***'}")
        print(f"  NULLED (no service): {sorted(null_set) or 'none'}  -> {live.height} live")
        print(f"  range [{s.min():.1f}, {s.max():.1f}]  median={med:.1f}")
        print(f"  WORST: {', '.join('%s %.1f' % (r['local_authority'], r['value']) for r in worst.iter_rows(named=True))}")
        print(f"  BEST : {', '.join('%s %.1f' % (r['local_authority'], r['value']) for r in best.iter_rows(named=True))}\n")

        for r in df.iter_rows(named=True):
            long_rows.append({"local_authority": r["local_authority"], "metric_key": key,
                              "value": r["value"], "year": 2024})
        meta_rows.append({"metric_key": key, "label": label, "unit": unit, "direction_good": good,
                          "national_median": round(med, 2), "source_page": page,
                          "deep_link": f"{PDF}#page={page + 2}", "caption": caption})

    if not ok:
        print("JOIN VALIDATION FAILED — not writing keeper files.")
        return
    pl.DataFrame(long_rows).write_parquet(D / "noac_council_scorecard.parquet")
    pl.DataFrame(meta_rows).write_csv(D / "scorecard_meta.csv")
    print(f"ALL JOINS OK — wrote noac_council_scorecard.parquet ({len(long_rows)} rows) + scorecard_meta.csv")


if __name__ == "__main__":
    main()
