"""Merge council-published register rows into the national planning register (silver).

The national ArcGIS feed stops carrying a council when that council changes system. Checked
2026-09-29: Cork City's last national row is 2025-11-04 and Dublin City's 2026-07-30, while both
keep publishing on their own registers. A collector outside this repository writes those rows,
already passed through the national `transform`, to SUPPLEMENT_NAME. This module is the one place they join the
register, and both writers call it:

    national ingest ──► point candidate ──► merge_into() ──► silver register
    council collector ──► supplement ─────► merge_into() ──┘

so a national refresh cannot silently drop the council rows, and a council refresh lands without
waiting for the next national run. Nothing here calls a council API.

Rules:
  * the national row always wins — a council row whose (authority, reference key) the national
    feed already holds is dropped, so a council that resumes national publishing stops being
    supplemented with no change here;
  * every row carries `register_source` (NATIONAL or COUNCIL);
  * the merge is idempotent — council rows already in the register are replaced, never stacked;
  * reference keys must be unique within the supplement, or the merge refuses.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from services.parquet_io import save_parquet

SUPPLEMENT_NAME = "planning_applications_council_supplement.parquet"
NATIONAL = "national_arcgis"
COUNCIL = "council_agile"
SOURCE_COL = "register_source"


class SupplementError(ValueError):
    """The supplement cannot be merged without double-counting or losing national rows."""


def reference_key(col: str = "ApplicationNumber") -> pl.Expr:
    """Separator-free reference: Cork City's '25/44149' (Agile) is '2544149' in the national feed."""
    return pl.col(col).cast(pl.String).str.to_uppercase().str.replace_all(r"[^0-9A-Z]", "")


def merged_register(register: pl.LazyFrame, supplement: pl.DataFrame) -> tuple[pl.LazyFrame, dict]:
    """The register with supplement rows the national feed lacks, plus per-authority merge counts.

    `register` may already hold council rows from an earlier merge; they are discarded and
    re-derived from `supplement`, so running the merge twice gives the same register.
    """
    names = register.collect_schema().names()
    if SOURCE_COL in names:
        # A null source is a national row; `!= COUNCIL` alone would drop it with the council rows.
        source = pl.col(SOURCE_COL).fill_null(NATIONAL)
        national = register.filter(source != COUNCIL).with_columns(source.alias(SOURCE_COL))
    else:
        national = register.with_columns(pl.lit(NATIONAL).alias(SOURCE_COL))
    supplement = supplement.with_columns(pl.lit(COUNCIL).alias(SOURCE_COL), reference_key().alias("_key"))
    dupes = supplement.group_by("PlanningAuthority", "_key").len().filter(pl.col("len") > 1)
    if dupes.height:
        raise SupplementError(f"duplicate supplement reference keys: {dupes.head(5).rows()}")

    authorities = supplement["PlanningAuthority"].unique().to_list()
    national_keys = (
        national.filter(pl.col("PlanningAuthority").is_in(authorities))
        .select("PlanningAuthority", reference_key().alias("_key"))
        .unique()
        .collect()
    )
    added = supplement.join(national_keys, on=["PlanningAuthority", "_key"], how="anti").drop("_key")
    national_rows = int(national.select(pl.len()).collect().item())

    stats = {"national_rows": national_rows, "authorities": {}}
    for authority in sorted(authorities):
        offered = supplement.filter(pl.col("PlanningAuthority") == authority).height
        kept = added.filter(pl.col("PlanningAuthority") == authority).height
        stats["authorities"][authority] = {
            "supplement_rows": offered,
            "already_national": offered - kept,
            "added": kept,
        }
    merged = pl.concat([national, added.lazy()], how="diagonal_relaxed")
    return merged, stats


def merge_into(register_path: Path, supplement_path: Path, dest: Path) -> dict | None:
    """Write `register_path` + unmatched supplement rows to `dest`; None when no supplement exists.

    `dest` must differ from `register_path`: the register is scanned lazily while `dest` is
    written, so the caller replaces the canonical file only after this returns.
    """
    if Path(dest).resolve() == Path(register_path).resolve():
        raise SupplementError("merge_into writes a new file; dest must not be the register it reads")
    if not Path(supplement_path).exists():
        return None
    merged, stats = merged_register(pl.scan_parquet(register_path), pl.read_parquet(supplement_path))
    # Never fewer rows than the national feed alone: a short write must not replace the register.
    save_parquet(merged, dest, min_rows=stats["national_rows"])
    return stats
