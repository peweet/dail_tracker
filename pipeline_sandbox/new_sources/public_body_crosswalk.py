"""Public-body crosswalk + joinability test (READ-ONLY, SANDBOX).

Builds a canonical public-body reference universe from the project's existing
spine (payments-gold publishers + the 31 LAs + department aliases + the
procurement publishers seed), then maps the bodies named in the sandbox
ingests (OIC public_body, DPC org-in-title, data.gov publisher) onto it.

Reuses the project's matching conventions: NFKD accent-fold, the `canonical_la`
suffix-strip for councils, and `si_department_aliases.csv` for departments.
Reports match rates by tier + joined examples. Nothing is promoted.
"""
from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

import polars as pl

REPO = Path("c:/Users/pglyn/PycharmProjects/dail_extractor")
META = REPO / "data/_meta"
GOLD = REPO / "data/gold/parquet"
SILVER = Path("c:/tmp/dail_new_sources/silver")
OUT = Path("c:/tmp/dail_new_sources")

# ── normalisers (consistent with the project) ────────────────────────────────
_APPLICANT_PREFIX = re.compile(r"^(?:mr|ms|mrs|dr|a)\b[\w.\s'-]*?\band\s+", re.I)
_FOI_SUFFIX = re.compile(r"\s*\(?\s*foi act\s*20\d\d\)?\s*$", re.I)
_LA_SUFFIX = re.compile(r"\s+(county council|city and county council|city council|borough council|town council|council)\s*$", re.I)


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c))


def norm_body(name: str) -> str:
    """Body normaliser: fold accents, strip applicant prefixes / FOI suffixes /
    sub-units after '/' or '(', map & -> and, drop 'the ', squeeze."""
    n = fold(name or "").strip()
    n = _APPLICANT_PREFIX.sub("", n)        # "Mr X and Health Service Executive" -> "Health Service Executive"
    n = _FOI_SUFFIX.sub("", n)
    n = n.split("/")[0]                       # HSE/University Hospital Waterford -> HSE
    n = re.split(r"\s*\(", n)[0]              # drop "(HSE)" etc.
    n = n.replace("&", " and ")
    n = n.lower()
    n = re.sub(r"^the\s+", "", n)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def norm_la(name: str) -> str:
    """LA key: strip the council suffix (mirrors canonical_la) then norm."""
    n = _LA_SUFFIX.sub("", fold(name or "")).strip()
    return norm_body(n)


# ── canonical reference universe ─────────────────────────────────────────────
def load_reference() -> tuple[pl.DataFrame, dict, list[tuple[str, str]]]:
    rows: list[dict] = []

    # A. payments-gold publishers (the real money-disclosing spine)
    try:
        pub = (pl.scan_parquet(GOLD / "procurement_payments_fact.parquet")
               .select(["publisher_name", "publisher_type"]).unique().collect())
        for r in pub.iter_rows(named=True):
            if r["publisher_name"]:
                rows.append({"ref_name": r["publisher_name"], "ref_type": r["publisher_type"] or "publisher", "ref_source": "payments_gold"})
    except Exception as e:  # noqa: BLE001
        print(f"  (payments gold unavailable: {e})")

    # B. 31 local authorities
    with open(META / "la_chief_executives.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({"ref_name": r["council_name"], "ref_type": "local_authority", "ref_source": "la_roster"})

    # C. departments (labels + alias phrases for contains-match)
    dept_aliases: list[tuple[str, str]] = []
    with open(META / "si_department_aliases.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({"ref_name": r["department_label"], "ref_type": "department", "ref_source": "dept_aliases"})
            dept_aliases.append((norm_body(r["alias"]), r["department_label"]))

    # D. procurement publishers seed (broader universe incl. semi-states)
    with open(META / "procurement_publishers/publishers_seed.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({"ref_name": r["publisher_name"], "ref_type": r.get("publisher_type") or "publisher", "ref_source": "publishers_seed"})

    # E. CSO Register of Public Sector Bodies (the authoritative body universe)
    rpsb = SILVER / "rpsb_bodies.parquet"
    if rpsb.exists():
        for r in pl.read_parquet(rpsb).iter_rows(named=True):
            rows.append({"ref_name": r["entity_name"], "ref_type": f"rpsb_{r['sector']}", "ref_source": "rpsb"})

    ref = pl.DataFrame(rows).with_columns([
        pl.col("ref_name").map_elements(norm_body, return_dtype=pl.Utf8).alias("ref_norm"),
        pl.col("ref_name").map_elements(norm_la, return_dtype=pl.Utf8).alias("ref_la"),
    ]).unique(subset=["ref_norm", "ref_source"])

    norm_to_canon: dict[str, dict] = {}
    for r in ref.iter_rows(named=True):
        norm_to_canon.setdefault(r["ref_norm"], r)
    la_to_canon = {r["ref_la"]: r for r in ref.filter(pl.col("ref_type") == "local_authority").iter_rows(named=True)}
    return ref, {"norm": norm_to_canon, "la": la_to_canon}, dept_aliases


def match_one(raw: str, idx: dict, dept_aliases: list[tuple[str, str]]) -> dict:
    nb = norm_body(raw)
    nl = norm_la(raw)
    # tier 1: exact normalised
    if nb in idx["norm"]:
        c = idx["norm"][nb]
        return {"match_tier": "1_exact", "canonical_name": c["ref_name"], "canonical_type": c["ref_type"], "ref_source": c["ref_source"]}
    # tier 2: LA suffix-stripped equality
    if nl in idx["la"]:
        c = idx["la"][nl]
        return {"match_tier": "2_la", "canonical_name": c["ref_name"], "canonical_type": "local_authority", "ref_source": c["ref_source"]}
    # tier 3: department alias contained in the body
    for alias, label in dept_aliases:
        if alias and re.search(rf"\b{re.escape(alias)}\b", nb):
            return {"match_tier": "3_dept_alias", "canonical_name": label, "canonical_type": "department", "ref_source": "dept_aliases"}
    # tier 4: containment fallback (guarded: ref token-phrase >=2 words, inside body)
    for rn, c in idx["norm"].items():
        if len(rn) >= 8 and " " in rn and (rn in nb or nb in rn):
            return {"match_tier": "4_contains", "canonical_name": c["ref_name"], "canonical_type": c["ref_type"], "ref_source": c["ref_source"]}
    return {"match_tier": None, "canonical_name": None, "canonical_type": None, "ref_source": None}


# ── per-source body extraction ───────────────────────────────────────────────
_DPC_PREFIX = re.compile(r"^inquiry\s+(?:concerning|into|in respect of|relating to)\s+(?:the\s+)?", re.I)


def bodies_from(source: str) -> pl.DataFrame:
    df = pl.read_parquet(SILVER / f"{source}.parquet")
    if source == "oic_foi_decisions":
        return df.select(pl.col("public_body").alias("raw_body")).filter(pl.col("raw_body").is_not_null())
    if source == "datagov_catalogue":
        return df.select(pl.col("publisher").alias("raw_body")).filter(pl.col("raw_body").is_not_null())
    if source == "dpc_decisions":
        return df.select(pl.col("title").map_elements(lambda t: _DPC_PREFIX.sub("", t or "").strip(), return_dtype=pl.Utf8).alias("raw_body"))
    raise ValueError(source)


def main() -> None:
    ref, idx, dept_aliases = load_reference()
    print(f"Reference universe: {ref.height} canonical bodies "
          f"(payments={ref.filter(pl.col('ref_source')=='payments_gold').height}, "
          f"LAs={ref.filter(pl.col('ref_source')=='la_roster').height}, "
          f"depts={ref.filter(pl.col('ref_source')=='dept_aliases').height}, "
          f"seed={ref.filter(pl.col('ref_source')=='publishers_seed').height}, "
          f"rpsb={ref.filter(pl.col('ref_source')=='rpsb').height})\n")

    report: list[str] = ["# Public-body crosswalk — joinability test\n",
                         f"Reference universe: {ref.height} canonical bodies.\n"]
    all_xwalk: list[pl.DataFrame] = []

    for source in ["oic_foi_decisions", "dpc_decisions", "datagov_catalogue"]:
        bodies = bodies_from(source)
        n_rows = bodies.height
        distinct = bodies.unique()
        matches = [match_one(r["raw_body"], idx, dept_aliases) for r in distinct.iter_rows(named=True)]
        m = distinct.with_columns([
            pl.Series("match_tier", [x["match_tier"] for x in matches]),
            pl.Series("canonical_name", [x["canonical_name"] for x in matches]),
            pl.Series("canonical_type", [x["canonical_type"] for x in matches]),
            pl.Series("ref_source", [x["ref_source"] for x in matches]),
        ]).with_columns(pl.lit(source).alias("source_dataset"))
        all_xwalk.append(m)

        # row-weighted match rate (join distinct map back to all rows)
        full = bodies.join(m.select(["raw_body", "match_tier"]), on="raw_body", how="left")
        row_matched = full.filter(pl.col("match_tier").is_not_null()).height
        d_matched = m.filter(pl.col("match_tier").is_not_null()).height

        emit = report.append
        emit(f"\n## {source}")
        emit(f"- distinct bodies: **{distinct.height}**  ·  matched: **{d_matched} ({100*d_matched/distinct.height:.0f}%)**")
        emit(f"- rows: **{n_rows:,}**  ·  matched by row: **{row_matched:,} ({100*row_matched/n_rows:.0f}%)**")
        tiers = m.filter(pl.col("match_tier").is_not_null()).group_by("match_tier").len().sort("match_tier")
        emit("- by tier (distinct): " + ", ".join(f"{r['match_tier']}={r['len']}" for r in tiers.to_dicts()))
        print(f"{source}: distinct {distinct.height} matched {d_matched} ({100*d_matched/distinct.height:.0f}%) | "
              f"rows {n_rows} matched {row_matched} ({100*row_matched/n_rows:.0f}%)")
        # top unmatched
        unm = (full.filter(pl.col("match_tier").is_null()).group_by("raw_body").len()
               .sort("len", descending=True).head(12))
        emit("- top UNMATCHED bodies: " + "; ".join(f"{r['raw_body']} ({r['len']})" for r in unm.to_dicts()))
        # matched examples
        ex = m.filter(pl.col("match_tier").is_not_null()).head(6)
        emit("- matched examples:")
        for r in ex.to_dicts():
            emit(f"    `{r['raw_body']}` → **{r['canonical_name']}** [{r['canonical_type']}, {r['match_tier']}]")

    xwalk = pl.concat(all_xwalk, how="diagonal")
    xwalk.write_parquet(OUT / "silver" / "public_body_crosswalk.parquet", compression="zstd")
    (OUT / "CROSSWALK_REPORT.md").write_text("\n".join(report), encoding="utf-8")
    print(f"\nCROSSWALK: {OUT/'silver'/'public_body_crosswalk.parquet'}  rows={xwalk.height}")
    print(f"REPORT:    {OUT/'CROSSWALK_REPORT.md'}")

    # ── concrete joinability demo: OIC decisions ↔ payments spine ────────────
    oic = pl.read_parquet(SILVER / "oic_foi_decisions.parquet")
    oic_map = xwalk.filter((pl.col("source_dataset") == "oic_foi_decisions") & (pl.col("canonical_name").is_not_null()))
    joined = (oic.join(oic_map.select(["raw_body", "canonical_name", "canonical_type"]),
                       left_on="public_body", right_on="raw_body", how="inner"))
    print(f"\nDEMO — OIC decisions that join to a canonical public body: {joined.height:,} of {oic.height:,}")
    demo = (joined.group_by(["canonical_name", "canonical_type"]).len()
            .sort("len", descending=True).head(10))
    print("  top bodies by FOI-decision count (now joinable to payments/departments):")
    for r in demo.to_dicts():
        print(f"    {r['len']:>4}  {r['canonical_name']}  [{r['canonical_type']}]")


if __name__ == "__main__":
    main()
