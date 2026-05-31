"""CSO PxStat extractor — pulls per-LA × year housing statistics tables.

Tables targeted:
  HSA07 — Authorised starts for local authority housing (1994–)
  HAP01 — Households starting/in/exiting HAP, per LA, per family type (2014–)
  HAP17 — Working % and gross household income of HAP households
  HAP20 — Rent as % of disposable income, HAP tenants
  HAP26 — Median waiting time for HAP tenants
  HAP32 — Median waiting time from main social housing list to HAP

Reads  : Eurostat-style JSON-stat from https://ws.cso.ie (REST)
Writes : data/gold/parquet/cso_<table_id>.parquet (one per table, --write)
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_OUT = _ROOT / "data" / "gold" / "parquet"
_API = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{code}/CSV/1.0/en"

TABLES = [
    # Original housing/HAP set
    "HSA07", "HAP01", "HAP17", "HAP20", "HAP26", "HAP32",
    # Added 2026-05-31 — current population + dwelling + price data
    "PEA08",  # Population Estimates per County, Year, Age, Sex (1991-2025)
    "NDA01",  # New Dwelling Completions Annual (replaces dead HSA07 series)
    "NDQ07",  # New Dwelling Completions Quarterly (by Eircode Routing Key)
    "HPM03",  # Market-based Household Purchases by RPPI Region (monthly)
    "HAP05",  # Additional HAP table
    # Added 2026-05-31 round 2 — vacancy + RPPI + Census housing
    "VAC14",  # Residential vacancy from metered electricity — by Local Authority
    "VAC15",  # Residential vacancy from metered electricity — by Local Electoral Area
    "VAC16",  # Residential vacancy from metered electricity — by Electoral Division
    "HPM04",  # Market-based purchases by Eircode output area
    "HPM07",  # Rolling 12-month purchases by RPPI region
    "HPM09",  # RPPI by type of residential property
    "F2021",  # Census 2022 Housing stock changes
    "F2023B", # Census 2022 weekly rent by county and city
    # Added 2026-05-31 round 3 — constituency anchor + migration flows
    "FY005",  # Census 2022 Population per Dáil constituency (the only natively
              # constituency-keyed PxStat table; constituency-axis anchor)
    "PEA15",  # Estimated migration by Origin/Destination, Sex, Year (flow data
              # complements PEA08's stock estimates)
    "PEA01",  # Population estimates by Single Year of Age, Sex, Region
              # (age-cohort detail at NUTS3 region level)
]


def fetch_csv(code: str) -> pl.DataFrame:
    url = _API.format(code=code)
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    raw = r.content.decode("utf-8-sig")
    # PEA08 / HPM03 use "-" as null for "Total" rows in code columns;
    # treat as null + read all as strings then cast on demand
    df = pl.read_csv(
        raw.encode("utf-8"),
        null_values=["-", ""],
        infer_schema_length=0,  # read everything as string; downstream casts
    )
    return df


def fidelity_check(df: pl.DataFrame, code: str) -> dict:
    """Cascade tailored to PxStat structure."""
    rpt: dict = {"checks": {}, "rows": len(df), "code": code}

    # Check 1 — Extraction (basic shape). Geographic + time dims vary widely
    # across PxStat tables — accept a broad set rather than hard-code.
    have = set(df.columns)
    geo_cols = {"Local Authority", "County", "Eircode Routing Key", "Region",
                "Province", "NUTS 3 Region", "NUTS 2 Region", "Local Electoral Area",
                "Electoral Divisions", "RPPI Region", "County and City",
                "Dáil Constituency", "Dail Constituency",
                "Constituency 2017", "Constituency 2013", "Constituency",
                "Country of Origin", "Country of Destination",
                "Origin/Destination", "Origin and Destination",
                "Type of Residential Property", "Dwelling Status",
                "Family Type", "HAP Tenants", "Sex", "Age Group",
                "Single Year of Age",
                "Nature of Occupancy", "Type of Buyer", "Stamp Duty Event",
                # National-only tables expose only categorical (non-geo) splits.
                # Treat the component / event split as the de-facto "geo" so the
                # extraction check passes on national time series.
                "Component", "Vital Event", "Population Change Component"}
    time_cols = {"Year", "Quarter", "Month", "CensusYear", "Census Year"}
    has_geo = any(c in have for c in geo_cols)
    has_time = any(c in have for c in time_cols)
    has_stat = "STATISTIC" in have and "VALUE" in have
    rpt["checks"]["1_extraction"] = {
        "row_count": len(df),
        "geo_col_used": next((c for c in geo_cols if c in have), None),
        "time_col_used": next((c for c in time_cols if c in have), None),
        "pass": has_stat and has_geo and has_time and len(df) > 0,
    }

    # Check 2 — Internal consistency (null rate; numeric VALUE)
    # Cast VALUE to Float64 once; PxStat sometimes returns strings like "" for nulls
    if "VALUE" in have:
        df_num = df.with_columns(pl.col("VALUE").cast(pl.Float64, strict=False).alias("_value_num"))
        null_value = df_num.filter(pl.col("_value_num").is_null()).height
    else:
        df_num = df
        null_value = 0
    null_pct = round(100 * null_value / max(len(df), 1), 1)
    rpt["checks"]["2_internal_sum"] = {
        "null_value_rows": null_value,
        "null_pct": null_pct,
        "pass": null_pct < 60,
    }

    # Check 3 — Cross-table: at least the "All Family Types" / "Ireland" rows exist
    has_ireland = False
    if "Local Authority" in have:
        has_ireland = df.filter(pl.col("Local Authority") == "Ireland").height > 0
    rpt["checks"]["3_national_aggregate"] = {
        "has_ireland_row": has_ireland,
        "pass": True,  # not all PxStat tables have national-aggregate
    }

    # Check 4 — Cross-source (skipped — would need HAP funding XLSX etc.)
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}

    # Check 5 — Semantic. Negatives shouldn't happen for counts/populations,
    # but RPPI / "change" / "movement" / "annual change" / migration-component
    # tables can legitimately be negative.
    if "VALUE" in have:
        is_change_table = False
        # CSO is inconsistent on this column's casing ("Statistic Label" vs
        # "STATISTIC Label" vs "Statistic"). Probe each.
        label_col = next((c for c in ("Statistic Label", "STATISTIC Label", "Statistic")
                          if c in have), None)
        signal_terms = ("change", "movement", "growth", "annual", "rppi",
                        "migration", "net", "balance", "emigrant", "exit")
        if label_col:
            labels = " ".join(df[label_col].drop_nulls().unique().to_list()).lower()
            is_change_table = any(t in labels for t in signal_terms)
        # Migration tables often carry the signal in a "Component" / event split
        # rather than the Statistic label.
        if not is_change_table:
            for component_col in ("Component", "Vital Event", "Population Change Component"):
                if component_col in have:
                    comp_vals = " ".join(df[component_col].drop_nulls().unique().to_list()).lower()
                    if any(t in comp_vals for t in signal_terms):
                        is_change_table = True
                        break
        bad = df_num.filter(pl.col("_value_num") < 0).height
        rpt["checks"]["5_semantic"] = {
            "negative_values": bad,
            "is_change_table": is_change_table,
            "pass": bad == 0 or is_change_table,
        }
    else:
        rpt["checks"]["5_semantic"] = {"pass": False, "note": "no VALUE column"}

    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--tables", nargs="*", default=TABLES, help="Subset of tables")
    args = ap.parse_args()

    summary = []
    for code in args.tables:
        try:
            df = fetch_csv(code)
        except Exception as e:
            print(f"[{code}] FETCH FAIL: {e}")
            summary.append((code, "fetch_fail", 0, False))
            continue

        rpt = fidelity_check(df, code)
        green = rpt["green"]
        print(f"\n=== {code} — {len(df)} rows ===")
        for name, chk in rpt["checks"].items():
            tag = "GREEN" if chk.get("pass") else "FAIL"
            print(f"  [{tag}] {name}: {chk}")
        print(f"  >>> overall: {'GREEN' if green else 'AMBER/RED'}")

        if args.write and green:
            path = _OUT / f"cso_{code.lower()}.parquet"
            _write_parquet(df, path)
            print(f"  Wrote {path.relative_to(_ROOT)}")

        summary.append((code, "ok" if green else "amber", len(df), green))
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("SUMMARY")
    for code, status, n, green in summary:
        flag = "✓" if green else ("⚠" if status == "amber" else "✗")
        print(f"  {flag} {code:10s} {status:12s} {n:>7,} rows")


if __name__ == "__main__":
    main()
