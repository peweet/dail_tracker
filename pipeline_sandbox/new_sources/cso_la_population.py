"""CSO Census 2022 population by LOCAL AUTHORITY — the denominator the IPAS map needs.

SANDBOX ONLY. Writes c:/tmp/dail_new_sources/silver/cso_la_population.parquet.

WHY THIS TABLE
--------------
The C&AG's own IPAS map (RoAPS 2024, Fig 10.2) is "IP applicants per 1,000 of
population". We hold IP applicants per local authority; we had NO LA population.
Our only population table in the repo is CONSTITUENCY-level
(data/gold/parquet/ec_constituency_pop_2022.parquet) and Dáil constituencies span
multiple local authorities, so it CANNOT be safely divided into LAs.

PxStat table chosen: **FY003A** — "Population" (Census 2022) by CensusYear × Sex ×
**Administrative Counties**. It is the only Census-2022 population table whose
geography dimension is the 31 LOCAL AUTHORITIES themselves (32 categories = 31 LAs
+ "Ireland"), labelled exactly as councils ("Cork City Council", "Cork County
Council", "Dún Laoghaire Rathdown County Council", ...).

Rejected alternatives (checked, not guessed):
  * FY003B "Population and Actual and Percentage Change 2006 to 2022" — geography is
    "County and City" and it MERGES Cork into "Cork City and Cork County" → only 30
    distinct LAs. Unusable for a 31-LA map.
  * FY005 — Dáil-constituency keyed (the constituency trap above).
  * PEA08 — inter-censal county ESTIMATES, not the Census 2022 enumerated count the
    C&AG used.

Source  : CSO PxStat JSON-stat/CSV REST API (ws.cso.ie) — plain HTTP, no WAF.
Licence : Creative Commons Attribution 4.0 (CC-BY 4.0) — CSO open data.
Grain   : one row per local authority (31) + one row per unmapped LA (explicit UNKNOWN).
Note    : population is a STOCK (headcount at a point in time), not a summable flow —
          value_safe_to_sum=False. The 31 LA rows DO tile the State exactly, but the
          column must never be summed across a hierarchy or across snapshots.
"""
from __future__ import annotations

import io
import sys
import unicodedata

import polars as pl

from _common import SILVER, fetch, now_iso

TABLE_CODE = "FY003A"
CSO_URL = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{TABLE_CODE}/CSV/1.0/en"
CSO_TABLE_PAGE = f"https://data.cso.ie/table/{TABLE_CODE}"
LICENCE = "CC-BY 4.0 (Creative Commons Attribution 4.0) — Central Statistics Office"
CENSUS_STATE_TOTAL = 5_149_139  # Census 2022 State population, CSO published figure

# The 31 LA labels as they appear in ipas_by_local_authority.parquet (local_authority_raw,
# i.e. the IPAS weekly-stats spelling) -> the EXACT CSO FY003A "Administrative Counties"
# label. Explicit, hand-verified, no fuzzy matching. NB the CSO string quirks that a
# naive matcher would miss and that we therefore pin literally:
#   "Mayo  County Council"                    -- DOUBLE space after Mayo
#   "Dún Laoghaire Rathdown County Council"   -- fada, and NO hyphen
#   "Limerick City & County Council" / "Waterford City & County Council" -- ampersand
CROSSWALK: dict[str, str] = {
    "Carlow County Council": "Carlow County Council",
    "Cavan County": "Cavan County Council",
    "Clare County": "Clare County Council",
    "Cork City": "Cork City Council",
    "Cork County": "Cork County Council",
    "Donegal County": "Donegal County Council",
    "Dublin City": "Dublin City Council",
    "Dun Laoghaire": "Dún Laoghaire Rathdown County Council",
    "Fingal County": "Fingal County Council",
    "Galway City Council": "Galway City Council",
    "Galway County Council": "Galway County Council",
    "Kerry County": "Kerry County Council",
    "Kildare County": "Kildare County Council",
    "Kilkenny County": "Kilkenny County Council",
    "Laois County": "Laois County Council",
    "Leitrim County": "Leitrim County Council",
    "Limerick City & County": "Limerick City & County Council",
    "Longford County": "Longford County Council",
    "Louth County": "Louth County Council",
    "Mayo County": "Mayo  County Council",
    "Meath County": "Meath County Council",
    "Monaghan County": "Monaghan County Council",
    "Offaly County": "Offaly County Council",
    "Roscommon County": "Roscommon County Council",
    "Sligo County": "Sligo County Council",
    "South Dublin County": "South Dublin County Council",
    "Tipperary County": "Tipperary County Council",
    "Waterford City and County": "Waterford City & County Council",
    "Westmeath County": "Westmeath County Council",
    "Wexford County": "Wexford County Council",
    "Wicklow County": "Wicklow County Council",
}


def _fold(s: str) -> str:
    """NFKD accent-fold + squash — used ONLY as an independent cross-check of the
    hand-written crosswalk, never as the matcher itself. Strips the trailing
    'Council' but NEVER the City/County token (Cork City != Cork County)."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().replace("&", "and").replace("-", " ")
    if s.endswith(" council"):
        s = s[: -len(" council")]
    return " ".join(s.split())


def fetch_fy003a() -> tuple[pl.DataFrame, dict]:
    # Fetch BINARY and decode utf-8-sig ourselves: the PxStat CSV response carries no
    # charset, so requests' r.text guesses latin-1 and mangles "Dún Laoghaire".
    payload, meta = fetch(CSO_URL, binary=True, timeout=90)
    df = pl.read_csv(io.BytesIO(payload.decode("utf-8-sig").encode("utf-8")), infer_schema_length=0)
    need = {"Administrative Counties", "Sex", "CensusYear", "VALUE", "Statistic Label"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"{TABLE_CODE}: unexpected schema, missing {missing}")
    both = df.filter(
        (pl.col("Sex") == "Both sexes")
        & (pl.col("CensusYear") == "2022")
        & (pl.col("Statistic Label") == "Population")
    ).select(
        pl.col("Administrative Counties").alias("local_authority"),
        pl.col("VALUE").cast(pl.Int64).alias("population_2022"),
    )
    if both.height != 32:
        raise ValueError(f"{TABLE_CODE}: expected 32 geo rows (31 LAs + Ireland), got {both.height}")
    return both, meta


def main() -> None:
    both, meta = fetch_fy003a()
    fetched_at = meta.get("fetched_at") or now_iso()

    state_row = both.filter(pl.col("local_authority") == "Ireland")
    cso_state_total = int(state_row["population_2022"][0])
    las = both.filter(pl.col("local_authority") != "Ireland")
    cso_labels = set(las["local_authority"].to_list())

    ipas = pl.read_parquet(SILVER / "ipas_by_local_authority.parquet")
    our_las = sorted(ipas["local_authority_raw"].unique().to_list())

    # ---- crosswalk validation: every one of our 31 LAs must resolve to a real CSO label,
    #      and the mapping must be a bijection onto the 31 CSO LAs. No guessing. ----
    pop_by_cso = {r["local_authority"]: r["population_2022"] for r in las.iter_rows(named=True)}
    rows, unmapped, bad_target = [], [], []
    for ours in our_las:
        cso = CROSSWALK.get(ours)
        if cso is None:
            unmapped.append(ours)
            reason = "no entry in CROSSWALK for this IPAS local-authority label"
        elif cso not in pop_by_cso:
            bad_target.append((ours, cso))
            reason = f"crosswalk target '{cso}' not present in CSO {TABLE_CODE} Administrative Counties"
        else:
            reason = None
        rows.append(
            {
                "local_authority": cso if reason is None else None,
                "local_authority_key": ours,
                "population_2022": pop_by_cso[cso] if reason is None else None,
                "unknown_reason": reason,
            }
        )

    matched = [r for r in rows if r["population_2022"] is not None]
    used_cso = {r["local_authority"] for r in matched}
    unused_cso = sorted(cso_labels - used_cso)
    dupes = len(matched) - len(used_cso)

    # Independent cross-check of the hand-written crosswalk (accent-folded key equality).
    fold_cso = {_fold(c): c for c in cso_labels}
    disagree = [
        (r["local_authority_key"], r["local_authority"], fold_cso.get(_fold(r["local_authority_key"])))
        for r in matched
        if fold_cso.get(_fold(r["local_authority_key"])) not in (None, r["local_authority"])
    ]

    out = (
        pl.DataFrame(
            rows,
            schema={
                "local_authority": pl.Utf8,
                "local_authority_key": pl.Utf8,
                "population_2022": pl.Int64,
                "unknown_reason": pl.Utf8,
            },
        )
        .with_columns(
            pl.lit(CSO_URL).alias("source_url"),
            pl.lit(TABLE_CODE).alias("source_table_code"),
            pl.lit(CSO_TABLE_PAGE).alias("source_table_page"),
            pl.lit("CSO Census 2022 (PxStat FY003A) — Population by Sex and Administrative County").alias("source_name"),
            pl.lit(LICENCE).alias("licence"),
            pl.lit(meta.get("source_document_hash")).alias("source_document_hash"),
            pl.lit(fetched_at).alias("fetched_at"),
            pl.lit("cso_pxstat_restful_csv").alias("extraction_method"),
            pl.when(pl.col("population_2022").is_not_null())
            .then(pl.lit("high"))
            .otherwise(pl.lit("unknown"))
            .alias("confidence"),
            pl.lit("public_aggregates").alias("privacy_tier"),
            # A STOCK (headcount), not a summable flow across a hierarchy/snapshots.
            pl.lit(False).alias("value_safe_to_sum"),
        )
        .sort("population_2022", descending=True, nulls_last=True)
    )

    out.write_parquet(SILVER / "cso_la_population.parquet", compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    out.write_csv(SILVER / "_eyeball" / "cso_la_population.csv")

    # ---------------- validation report ----------------
    la_sum = int(las["population_2022"].sum())
    matched_sum = sum(int(r["population_2022"]) for r in matched)
    print(f"PxStat table   : {TABLE_CODE}  ({CSO_TABLE_PAGE})")
    print(f"Licence        : {LICENCE}")
    print(f"Fetched        : {fetched_at}  sha256={meta.get('source_document_hash', '')[:16]}...")
    print(f"CSO geo rows   : {both.height} (31 LAs + Ireland)")
    print("-" * 72)
    print(f"CSO 31 LA sum          : {la_sum:,}")
    print(f"CSO 'Ireland' row      : {cso_state_total:,}")
    print(f"Census 2022 State total: {CENSUS_STATE_TOTAL:,}  (expected)")
    print(f"DELTA (31 LAs - State) : {la_sum - CENSUS_STATE_TOTAL:+,}")
    print(f"DELTA (Ireland - State): {cso_state_total - CENSUS_STATE_TOTAL:+,}")
    recon = la_sum == CENSUS_STATE_TOTAL == cso_state_total
    print(f"RECONCILES EXACTLY     : {recon}")
    print("-" * 72)
    print(f"our LAs                : {len(our_las)}")
    print(f"mapped                 : {len(matched)}")
    print(f"UNMAPPED (no crosswalk): {unmapped or 'none'}")
    print(f"BAD TARGET (not in CSO): {bad_target or 'none'}")
    print(f"duplicate CSO targets  : {dupes}")
    print(f"CSO LAs unused         : {unused_cso or 'none'}")
    print(f"mapped-population sum  : {matched_sum:,}  (delta vs State {matched_sum - CENSUS_STATE_TOTAL:+,})")
    print(f"fold cross-check disagreements: {disagree or 'none'}")
    with pl.Config(tbl_rows=35, fmt_str_lengths=40):
        print(out.select("local_authority_key", "local_authority", "population_2022", "unknown_reason"))

    if unmapped or bad_target or dupes or unused_cso:
        print("\n*** DQ FLAG: crosswalk is NOT a clean 31<->31 bijection — see UNKNOWN rows above.")
        sys.exit(0)  # sandbox: report, do not fail the run


if __name__ == "__main__":
    main()
