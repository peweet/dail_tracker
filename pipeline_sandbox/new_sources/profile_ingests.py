"""Usability profile of the sandbox ingests (READ-ONLY).

Loads each silver parquet from c:/tmp/dail_new_sources/silver and reports
schema, completeness, key-field quality, value ranges, and joinability hints.
Writes a markdown report alongside the data.
"""
from __future__ import annotations

import glob
import os

import polars as pl

SILVER = "c:/tmp/dail_new_sources/silver"
OUT = "c:/tmp/dail_new_sources/USABILITY_REPORT.md"
lines: list[str] = []


def emit(s: str = "") -> None:
    print(s)
    lines.append(s)


def nullrates(df: pl.DataFrame) -> list[tuple[str, float]]:
    n = df.height
    out = []
    for c in df.columns:
        nulls = df[c].null_count()
        # also treat empty strings as "missing" for usability
        empties = 0
        if df[c].dtype == pl.Utf8:
            empties = df.filter((pl.col(c).is_not_null()) & (pl.col(c).str.strip_chars() == "")).height
        miss = nulls + empties
        out.append((c, 100.0 * miss / n if n else 0.0))
    return out


def profile(path: str) -> None:
    name = os.path.basename(path).replace(".parquet", "")
    df = pl.read_parquet(path)
    emit(f"\n## {name}  —  {df.height:,} rows × {df.width} cols  ({os.path.getsize(path)//1024} KB)")
    emit("\n**Completeness (% missing incl. empty strings):**")
    for c, pct in nullrates(df):
        bar = "█" * int(pct / 5)
        flag = "  ⚠" if pct > 20 else ""
        emit(f"  {c:24} {pct:5.1f}% {bar}{flag}")

    # per-dataset key-field checks
    if name == "datagov_catalogue":
        emit("\n**Join/usability:**")
        emit(f"  distinct publishers: {df['publisher'].n_unique()}")
        emit(f"  rows with metadata_modified: {100*df.filter(pl.col('metadata_modified').is_not_null()).height/df.height:.1f}%")
        emit(f"  rows with a licence_id: {100*df.filter(pl.col('licence_id').is_not_null()).height/df.height:.1f}%")
        emit(f"  rows with >=1 resource: {df.filter(pl.col('num_resources')>0).height:,} ({100*df.filter(pl.col('num_resources')>0).height/df.height:.1f}%)")
        emit(f"  rows with 0 resources (dead/empty datasets): {df.filter(pl.col('num_resources')==0).height:,}")
        mm = df.select(pl.col("metadata_modified").str.slice(0,4).alias("y")).group_by("y").len().sort("y")
        emit("  metadata_modified by year: " + ", ".join(f"{r['y']}:{r['len']}" for r in mm.to_dicts() if r['y']))

    if name == "oic_foi_decisions":
        emit("\n**Join/usability:**")
        cr = df["case_reference"]
        emit(f"  case_reference non-null: {100*cr.is_not_null().sum()/df.height:.1f}%   distinct: {cr.n_unique()}   dup refs: {df.height - cr.n_unique()}")
        emit(f"  decision_date parsed (ISO): {100*df.filter(pl.col('decision_date').is_not_null()).height/df.height:.1f}%")
        bad = df.filter(pl.col("decision_date") > "2026-12-31")
        emit(f"  impossible/typo dates (>2026): {bad.height}  -> {bad['decision_date'].to_list()[:5]}")
        emit(f"  distinct public bodies: {df['public_body'].n_unique()}")
        # normalisation sniff: HSE variants
        hse = df.filter(pl.col("public_body").str.contains("(?i)health service executive"))["public_body"].unique().to_list()
        emit(f"  HSE name variants (need norm): {hse}")
        emit(f"  rows with FOI sections captured: {100*df.filter(pl.col('foi_sections').is_not_null()).height/df.height:.1f}%")

    if name == "cag_reports":
        emit("\n**Join/usability:**")
        emit("  by report_type: " + ", ".join(f"{r['report_type']}:{r['len']}" for r in df.group_by('report_type').len().to_dicts()))
        emit(f"  report_number parsed: {100*df.filter(pl.col('report_number').is_not_null()).height/df.height:.1f}%")
        emit(f"  source_published_date captured: {100*df.filter(pl.col('source_published_date').is_not_null()).height/df.height:.1f}% (best-effort, low confidence)")
        emit(f"  pdf_url captured: {100*df.filter(pl.col('pdf_url').is_not_null()).height/df.height:.1f}% (MAX_DETAIL=200 cap)")
        sr = df.filter(pl.col("report_type")=="special_report")
        emit(f"  special-report number range: {sr['report_number'].min()}–{sr['report_number'].max()}")

    if name == "dpc_decisions":
        emit("\n**Join/usability:**")
        emit(f"  decision_date parsed: {100*df.filter(pl.col('decision_date').is_not_null()).height/df.height:.1f}%")
        emit(f"  with GDPR articles: {100*df.filter(pl.col('gdpr_articles').is_not_null()).height/df.height:.1f}%")
        emit(f"  with sector tags: {100*df.filter(pl.col('sector_tags')!='').height/df.height:.1f}%")
        top = df.select(pl.col("sector_tags").str.split(";").alias("t")).explode("t").filter(pl.col("t")!="").group_by("t").len().sort("len",descending=True).head(8)
        emit("  top sector tags: " + ", ".join(f"{r['t']}({r['len']})" for r in top.to_dicts()))

    emit("\n**Sample rows:**")
    cols = [c for c in df.columns if c not in ("fetched_at","extraction_method","confidence","privacy_tier","caveat","list_page")]
    with pl.Config(tbl_rows=4, tbl_cols=-1, fmt_str_lengths=42, tbl_width_chars=160):
        emit(str(df.select(cols[:7]).head(4)))


def main() -> None:
    emit("# Sandbox ingest — usability report")
    paths = sorted(glob.glob(f"{SILVER}/*.parquet"))
    for p in paths:
        profile(p)
    open(OUT, "w", encoding="utf-8").write("\n".join(lines))
    emit(f"\n\nWritten: {OUT}")


if __name__ == "__main__":
    main()
