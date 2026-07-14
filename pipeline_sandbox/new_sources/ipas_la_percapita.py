"""IP applicants per 1,000 of population, per LOCAL AUTHORITY — the real per-capita
metric the IPAS choropleth was missing (ipas_county_profile.py currently carries
ip_per_1000_population as an explicit NULL + unknown_reason).

SANDBOX ONLY. Writes c:/tmp/dail_new_sources/silver/ipas_la_percapita.parquet.

This is the C&AG's own map metric: RoAPS 2024, Ch.10, Fig 10.2 — "IP applicants per
1,000 of population", banded 0-2 / 3-5 / 6-8 / 9-11 / 12+.

  numerator   IPAS weekly statistics report, snapshot 2024-12-29 — IP applicants in
              State-provided accommodation, per local authority (31 LAs; validated to
              the PDF's own Grand Total 32,702).
  denominator CSO Census 2022, PxStat FY003A — Population by Sex and Administrative
              County (Both sexes). CC-BY 4.0. The 31 LAs tile the State exactly
              (sum == 5,149,139 == the published State total).

TWO DIFFERENT DATES, DELIBERATELY: the numerator is a Dec-2024 snapshot, the
denominator an Apr-2022 census enumeration. That is exactly the C&AG's own
construction (there is no LA-level census after 2022), but it is a real caveat and is
carried in the row as `metric_caveat` — never silently.

value_safe_to_sum=False: a RATE. Never sum, never average unweighted across LAs.
"""
from __future__ import annotations

import polars as pl

from _common import SILVER, now_iso

IPAS_URL = "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf"
CSO_TABLE_CODE = "FY003A"
CSO_URL = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{CSO_TABLE_CODE}/CSV/1.0/en"
CSO_TABLE_PAGE = f"https://data.cso.ie/table/{CSO_TABLE_CODE}"
CAG_URL = (
    "https://www.audit.gov.ie/media/huahyz0u/"
    "10-management-of-international-protection-accommodation-contracts-copy.pdf"
)
CSO_LICENCE = "CC-BY 4.0 — Central Statistics Office"

# The C&AG's published bands (RoAPS 2024, Fig 10.2 legend).
CAG_BANDS = [(0, 2, "0-2"), (3, 5, "3-5"), (6, 8, "6-8"), (9, 11, "9-11"), (12, None, "12+")]

PROVENANCE_FOOTER = (
    f"IP applicants: IPAS weekly statistics report, snapshot 2024-12-29 ({IPAS_URL}). "
    f"Population: CSO Census 2022, PxStat table {CSO_TABLE_CODE} (Population by Sex and "
    f"Administrative County, Both sexes) — {CSO_TABLE_PAGE} — {CSO_LICENCE}. "
    f"Rate = IP applicants / population x 1,000; bands follow C&AG Report on the Accounts "
    f"of the Public Services 2024, Ch.10, Fig 10.2 ({CAG_URL}). "
    f"CAVEAT: numerator is a Dec-2024 snapshot, denominator an Apr-2022 census enumeration "
    f"(no later LA-level census exists). A RATE — never sum or unweighted-average across LAs."
)

METRIC_CAVEAT = (
    "Numerator (IP applicants) is 2024-12-29; denominator (population) is Census 2022 "
    "(2022-04-03). No LA-level population exists for 2024, so the C&AG's own map uses the "
    "same mixed-date construction. Population change 2022->2024 is NOT adjusted for."
)


def band_of(rate: float | None) -> str:
    """Assign the C&AG Fig 10.2 band. The published legend is integer-labelled
    (0-2 / 3-5 / 6-8 / 9-11 / 12) but the rate is continuous, so the bands are read as
    HALF-OPEN: [0,3) [3,6) [6,9) [9,12) [12,inf). e.g. Roscommon 2.93 -> "0-2".
    The C&AG does not publish its bin edges; this is the only reading that tiles the
    continuum without gaps, and it is a presentation choice, not a data claim."""
    if rate is None:
        return "unknown"
    for lo, hi, label in CAG_BANDS:
        if hi is None:
            if rate >= lo:
                return label
        elif lo <= rate < hi + 1:
            return label
    return "unknown"


def main() -> None:
    ipas = pl.read_parquet(SILVER / "ipas_by_local_authority.parquet")
    pop = pl.read_parquet(SILVER / "cso_la_population.parquet")

    df = (
        ipas.select(
            pl.col("local_authority_raw").alias("local_authority"),
            "ip_applicants",
            "snapshot_date",
        )
        .join(
            pop.select(
                pl.col("local_authority_key").alias("local_authority"),
                pl.col("local_authority").alias("cso_local_authority"),
                "population_2022",
                pl.col("unknown_reason").alias("population_unknown_reason"),
            ),
            on="local_authority",
            how="left",
        )
        .with_columns(
            pl.when(pl.col("population_2022").is_not_null() & (pl.col("population_2022") > 0))
            .then((pl.col("ip_applicants") / pl.col("population_2022") * 1000).round(2))
            .otherwise(None)
            .alias("ip_per_1000_population"),
        )
        .with_columns(
            pl.col("ip_per_1000_population")
            .map_elements(band_of, return_dtype=pl.Utf8)
            .alias("cag_band"),
            pl.when(pl.col("population_2022").is_null())
            .then(
                pl.coalesce(
                    pl.col("population_unknown_reason"),
                    pl.lit("no Census 2022 population matched for this local authority"),
                )
            )
            .otherwise(None)
            .alias("unknown_reason"),
            pl.lit(2022).alias("population_census_year"),
            pl.lit(IPAS_URL).alias("source_url_ip_applicants"),
            pl.lit(CSO_URL).alias("source_url_population"),
            pl.lit(CSO_TABLE_CODE).alias("source_table_code_population"),
            pl.lit(CSO_LICENCE).alias("licence_population"),
            pl.lit(CAG_URL).alias("source_url_cag_bands"),
            pl.lit(METRIC_CAVEAT).alias("metric_caveat"),
            pl.lit(PROVENANCE_FOOTER).alias("provenance_footer"),
            pl.lit(now_iso()).alias("derived_at"),
            pl.lit("join(ipas_weekly_pdf, cso_pxstat_fy003a)").alias("extraction_method"),
            pl.when(pl.col("population_2022").is_not_null())
            .then(pl.lit("high"))
            .otherwise(pl.lit("unknown"))
            .alias("confidence"),
            pl.lit("public_aggregates").alias("privacy_tier"),
            pl.lit(False).alias("value_safe_to_sum"),  # a RATE
        )
        .sort("ip_per_1000_population", descending=True, nulls_last=True)
    )

    df.write_parquet(SILVER / "ipas_la_percapita.parquet", compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    df.write_csv(SILVER / "_eyeball" / "ipas_la_percapita.csv")

    # ---------------- report ----------------
    n_missing = df.filter(pl.col("ip_per_1000_population").is_null()).height
    tot_ip = int(df["ip_applicants"].sum())
    tot_pop = int(df["population_2022"].sum())
    national = round(tot_ip / tot_pop * 1000, 2)
    print(f"ipas_la_percapita: {df.height} LAs | population unknown for {n_missing}")
    print(f"IP applicants total {tot_ip:,} (IPAS Grand Total 32,702) | population total {tot_pop:,}")
    print(f"NATIONAL rate (properly weighted, not a mean of LA rates): {national} per 1,000")
    with pl.Config(tbl_rows=35, fmt_str_lengths=30, tbl_width_chars=120):
        print(
            df.select(
                "local_authority", "ip_applicants", "population_2022", "ip_per_1000_population", "cag_band"
            )
        )
    print("\n--- C&AG Fig 10.2 band distribution ---")
    for _, _, label in CAG_BANDS:
        sub = df.filter(pl.col("cag_band") == label)
        names = ", ".join(sub["local_authority"].to_list())
        print(f"  band {label:>4s}: {sub.height:2d} LAs  {names}")
    unk = df.filter(pl.col("cag_band") == "unknown")
    if unk.height:
        print(f"  band  UNK: {unk.height} LAs  {', '.join(unk['local_authority'].to_list())}")


if __name__ == "__main__":
    main()
