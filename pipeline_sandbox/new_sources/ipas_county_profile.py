"""County/LA drill-down profile for the IPAS map: how many people, what a room
costs there, what the inspector found — with per-figure provenance for a footer.

SANDBOX ONLY. Feeds a clickable council choropleth: click a county ->
  * IP applicants in that LA (IPAS weekly stats, validated to its own Grand Total)
  * the ACTUAL contracted nightly rates for sampled properties in that county
    (C&AG Annex 10A) + the national €92 private vs €34 State-owned benchmark
  * HIQA inspection count for that county
  * accommodation type mix (hotel / apartment / dormitory / guesthouse)

HOUSING-IMPACT HONESTY: 'rooms absorbed' is NOT PUBLISHED per property. The ONLY
room figure in the C&AG chapter is 2,612 rooms across the 25 competitively-tendered
contracts (nationally). Everything else is BED capacity, not rooms, and the
tourism/housing stock split is not published either. Those land as explicit
unknown rows — never estimated.

Every output row carries source_url + page ref so the UI can render a provenance
footer verbatim.
"""
from __future__ import annotations

import re
import polars as pl

from _common import SILVER, now_iso

CAG_URL = ("https://www.audit.gov.ie/media/huahyz0u/"
           "10-management-of-international-protection-accommodation-contracts-copy.pdf")
IPAS_URL = "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf"
HIQA_URL = ("https://www.hiqa.ie/sites/default/files/2025-03/"
            "Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf")

# LA (weekly-stats form) -> the county label used in the C&AG Annex / HIQA
LA_TO_COUNTY = {
    "South Dublin County": "South Dublin", "Dublin City": "Dublin",
    "Fingal County": "Dublin", "Dun Laoghaire": "Dublin",
    "Cork City": "Cork", "Cork County": "Cork",
    "Galway City Council": "Galway", "Galway County Council": "Galway",
    "Limerick City & County": "Limerick", "Waterford City and County": "Waterford",
    "Carlow County Council": "Carlow",
}


def county_of(la: str) -> str:
    if la in LA_TO_COUNTY:
        return LA_TO_COUNTY[la]
    return re.sub(r"\s+(County|City)(\s+Council)?$", "", la).strip()


def main() -> None:
    # ---- 1. people per LA (validated: sums to the PDF's own Grand Total 32,702) ----
    la = pl.read_parquet(SILVER / "ipas_by_local_authority.parquet")
    la = la.with_columns(pl.col("local_authority_raw")
                           .map_elements(county_of, return_dtype=pl.Utf8).alias("county"))

    # ---- 2. the real nightly rates, per property, per county (C&AG Annex 10A) ----
    fig = pl.read_parquet(SILVER / "cag_ipas_chapter_figures.parquet")
    samp = fig.filter(pl.col("category") == "sample_property")
    rx = re.compile(r"property (\d+): ([^,]+), ([^(]+) \(([^)]+)\)")
    props = []
    for r in samp.iter_rows(named=True):
        m = rx.search(r["metric"])
        if not m:
            continue
        props.append({
            "property_no": int(m.group(1)),
            "accommodation_type": m.group(2).strip(),
            "county": m.group(3).strip(),
            "procurement_route": m.group(4).strip(),
            "contracted_rate_eur_per_person_night": r["value_numeric"],
            "rate_known": not r["is_unknown"],
            "notes": r["notes"],
            "source_url": CAG_URL, "source_ref": "C&AG RoAPS 2024, Ch.10, Annex 10A (pp.175-176)",
        })
    prop = pl.DataFrame(props)
    # normalise the Annex's Dublin sub-labels to match the LA-derived county
    prop = prop.with_columns(
        pl.when(pl.col("county").str.contains("(?i)north dublin"))
          .then(pl.lit("Dublin"))
          .when(pl.col("county").str.contains("(?i)south dublin"))
          .then(pl.lit("South Dublin"))
          .otherwise(pl.col("county")).alias("county"))
    prop.write_parquet(SILVER / "ipas_sample_property_rates.parquet",
                       compression="zstd", statistics=True)

    # ---- 3. HIQA inspections per county ----
    hiqa = (pl.read_parquet(SILVER / "hiqa_ipas_inspections.parquet")
              .group_by("county").agg(pl.len().alias("hiqa_inspections"),
                                      pl.col("centre_name").n_unique().alias("hiqa_centres")))

    # ---- 4. county profile ----
    rates = (prop.filter(pl.col("rate_known"))
                 .group_by("county")
                 .agg(pl.col("contracted_rate_eur_per_person_night").min().alias("rate_min"),
                      pl.col("contracted_rate_eur_per_person_night").max().alias("rate_max"),
                      pl.col("contracted_rate_eur_per_person_night").median().alias("rate_median"),
                      pl.len().alias("sampled_properties"),
                      pl.col("accommodation_type").unique().alias("property_types"),
                      pl.col("procurement_route").unique().alias("procurement_routes")))

    prof = (la.group_by("county")
              .agg(pl.col("ip_applicants").sum(),
                   pl.col("local_authority_raw").alias("local_authorities"))
              .join(rates, on="county", how="left")
              .join(hiqa, on="county", how="left")
              .sort("ip_applicants", descending=True)
              .with_columns([
                  pl.lit(92).alias("national_benchmark_private_eur_night"),
                  pl.lit(34).alias("national_benchmark_state_owned_eur_night"),
                  # explicit unknowns — never estimated
                  pl.lit(None, dtype=pl.Int64).alias("rooms_absorbed"),
                  pl.lit(None, dtype=pl.Float64).alias("ip_per_1000_population"),
                  pl.lit("rooms_absorbed: NOT PUBLISHED per property/county. The only room "
                         "figure in the source is 2,612 rooms across the 25 competitively "
                         "tendered contracts NATIONALLY (C&AG 10.12); all other capacity is "
                         "published as BEDS, not rooms, and the tourism-vs-housing stock split "
                         "is not published. ip_per_1000_population: needs CSO Census 2022 "
                         "county population (our only population table is CONSTITUENCY-level "
                         "and constituencies span multiple LAs — not safely divisible)."
                         ).alias("unknown_reason"),
                  pl.lit(now_iso()).alias("derived_at"),
                  pl.lit(f"IP applicants: IPAS weekly stats 2024-12-29 ({IPAS_URL}) | "
                         f"Nightly rates: C&AG RoAPS 2024 Ch.10 Annex 10A ({CAG_URL}) | "
                         f"Cost benchmark: IGEES via C&AG 10.18 | "
                         f"Inspections: HIQA 2024 ({HIQA_URL})").alias("provenance_footer"),
                  pl.lit(False).alias("value_safe_to_sum"),
              ]))

    prof.write_parquet(SILVER / "ipas_county_profile.parquet", compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    prof.drop("local_authorities", "property_types", "procurement_routes") \
        .write_csv(SILVER / "_eyeball" / "ipas_county_profile.csv")
    prop.write_csv(SILVER / "_eyeball" / "ipas_sample_property_rates.csv")

    print(f"ipas_county_profile: {prof.height} counties (total {prof['ip_applicants'].sum():,} applicants)")
    with pl.Config(tbl_rows=30, fmt_str_lengths=28, tbl_width_chars=140):
        print(prof.select("county", "ip_applicants", "rate_min", "rate_median", "rate_max",
                          "sampled_properties", "hiqa_inspections"))
    print("\n--- the 20 sampled properties (what a bed actually costs, by county) ---")
    with pl.Config(tbl_rows=25, fmt_str_lengths=30, tbl_width_chars=140):
        print(prop.select("property_no", "county", "accommodation_type", "procurement_route",
                          "contracted_rate_eur_per_person_night")
                  .sort("contracted_rate_eur_per_person_night", descending=True, nulls_last=True))
    known = prop.filter(pl.col("rate_known"))
    print(f"\nnightly rate: min EUR {known['contracted_rate_eur_per_person_night'].min():.0f} "
          f"/ median EUR {known['contracted_rate_eur_per_person_night'].median():.0f} "
          f"/ max EUR {known['contracted_rate_eur_per_person_night'].max():.0f} "
          f"(vs IGEES: EUR 92 private, EUR 34 State-owned)")
    print(f"rate UNKNOWN for {prop.filter(~pl.col('rate_known')).height} of {prop.height} "
          f"sampled properties (C&AG: 'Unclear' / Department-run)")
    print("\nprocurement route mix:")
    print(prop.group_by("procurement_route").len().sort("len", descending=True))


if __name__ == "__main__":
    main()
