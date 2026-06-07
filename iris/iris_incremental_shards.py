"""
iris_incremental_shards.py — incremental ETL caching for Iris Oifigiúil PDFs.

Wired into `iris_oifigiuil_etl_polars.run()`. Per-PDF parquet shard + audit JSON
+ member-extract JSON, fingerprinted by (mtime_ns, size, EXTRACTOR_VERSION) so
unchanged PDFs are skipped on subsequent runs. Silver/gold still rebuild over
the full corpus from the concat of all shards — back-dated corrections to a
single PDF still propagate.

PATTERN
-------
For each source PDF:
  1. compute fingerprint = (mtime_ns, size, EXTRACTOR_VERSION).
  2. if manifest entry matches AND shard files exist on disk → skip extract.
  3. else: run extract_lines_raw() + find_member_interest_page_ranges(),
     write bronze rows to a parquet shard + audit JSON + member-extract JSON,
     and update the manifest entry.

Gold rebuild is unchanged in shape — sourced from the cache:
    bronze = pl.concat([pl.scan_parquet(p) for p in shards]).collect()
    # then build_records(bronze) → enrich → quarantine → write CSVs as before.

The CSV writes still overwrite, because back-dated corrections to history can
land between any two existing rows. Determinism comes from sorting on
(issue_date, source_file, page_number, line_order) before writing, NOT from
insertion order — so adding 5 new shards never reorders the rest.

EXTRACTOR_VERSION
-----------------
Bump this string whenever the extraction logic in `extract_lines_raw` or
`find_member_interest_page_ranges` changes shape (new columns, different
parsing rules, fixed bugs that change values). The manifest treats a version
mismatch as cache invalidation — every shard gets rebuilt on the next run.
File mtime/size alone are insufficient because the PDF is unchanged but
the *interpretation* of it has changed.

STANDALONE USAGE
----------------
The wired path is `python iris_oifigiuil_etl_polars.py` (incremental by
default) and `python iris_oifigiuil_etl_polars.py --rebuild` (full re-extract).
This module can still be invoked directly to populate / inspect the cache
without running the silver/gold step:

    python iris_incremental_shards.py "data/bronze/iris_oifigiuil/*.pdf"
    python iris_incremental_shards.py "data/bronze/iris_oifigiuil/*.pdf" --rebuild
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from config import BRONZE_DIR, SILVER_DIR
from iris.iris_oifigiuil_etl_polars import (
    extract_lines_raw,
    find_member_interest_page_ranges,
)
from services.parquet_io import save_parquet

# Bump on any change to extract_lines_raw / find_member_interest_page_ranges
# that affects column shape, values, or parsing rules. Cached shards built
# under an older version are rebuilt automatically on the next run.
EXTRACTOR_VERSION = "iris-extract/2026-05-05"

DEFAULT_SHARD_ROOT = SILVER_DIR / "iris_oifigiuil_shards"
DEFAULT_INPUT_GLOB = str(BRONZE_DIR / "iris_oifigiuil" / "*.pdf")

MANIFEST_NAME = "_manifest.json"
BRONZE_SUBDIR = "bronze"
AUDIT_SUBDIR = "audit"
MEMBER_SUBDIR = "member_extracts"

# 404 stubs from the publisher are tiny HTML pages with a .pdf extension.
# fitz crashes on them; iris_oifigiuil_etl_polars.main filters at the same
# threshold.
MIN_REAL_PDF_BYTES = 5_000


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


@dataclass
class ShardSummary:
    added: int = 0
    skipped: int = 0
    restaged: int = 0  # re-extracted because version or fingerprint changed
    failed: int = 0


def _shard_paths(shard_root: Path, source_file: str) -> dict[str, Path]:
    stem = Path(source_file).stem
    return {
        "bronze": shard_root / BRONZE_SUBDIR / f"{stem}.parquet",
        "audit": shard_root / AUDIT_SUBDIR / f"{stem}.json",
        "member": shard_root / MEMBER_SUBDIR / f"{stem}.json",
    }


def _ensure_dirs(shard_root: Path) -> None:
    (shard_root / BRONZE_SUBDIR).mkdir(parents=True, exist_ok=True)
    (shard_root / AUDIT_SUBDIR).mkdir(parents=True, exist_ok=True)
    (shard_root / MEMBER_SUBDIR).mkdir(parents=True, exist_ok=True)


def _manifest_path(shard_root: Path) -> Path:
    return shard_root / MANIFEST_NAME


def load_manifest(shard_root: Path) -> dict[str, dict[str, Any]]:
    p = _manifest_path(shard_root)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def save_manifest(shard_root: Path, manifest: dict[str, dict[str, Any]]) -> None:
    tmp = _manifest_path(shard_root).with_suffix(".json.part")
    tmp.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(_manifest_path(shard_root))


def fingerprint(pdf_path: Path) -> dict[str, Any]:
    st = pdf_path.stat()
    return {
        "mtime_ns": st.st_mtime_ns,
        "size": st.st_size,
        "extractor_version": EXTRACTOR_VERSION,
    }


def is_fresh(
    pdf_path: Path,
    manifest: dict[str, dict[str, Any]],
    shard_root: Path,
) -> bool:
    entry = manifest.get(pdf_path.name)
    if not entry:
        return False
    if entry.get("extractor_version") != EXTRACTOR_VERSION:
        return False
    fp = fingerprint(pdf_path)
    if entry.get("mtime_ns") != fp["mtime_ns"] or entry.get("size") != fp["size"]:
        return False
    # Manifest can be ahead of disk if a previous run was interrupted between
    # parquet write and manifest save. Verify shard files actually exist.
    paths = _shard_paths(shard_root, pdf_path.name)
    return all(p.exists() for p in paths.values())


# ---------------------------------------------------------------------------
# Extraction → shard write
# ---------------------------------------------------------------------------


def process_pdf_to_shard(pdf_path: Path, shard_root: Path) -> dict[str, Any]:
    """Extract one PDF and write its three shard files. Returns the audit meta."""
    rows, meta = extract_lines_raw(str(pdf_path))
    member_extracts = find_member_interest_page_ranges(str(pdf_path))

    paths = _shard_paths(shard_root, pdf_path.name)

    df = pl.from_dicts(rows) if rows else pl.DataFrame()
    save_parquet(df, paths["bronze"])
    _atomic_write_json(meta, paths["audit"])
    _atomic_write_json(member_extracts, paths["member"])

    return meta


def _atomic_write_json(payload: Any, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(dest)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def incremental_extract(
    pdf_paths: Iterable[str | Path],
    shard_root: Path,
    *,
    rebuild: bool = False,
) -> ShardSummary:
    shard_root = Path(shard_root)
    _ensure_dirs(shard_root)

    manifest = {} if rebuild else load_manifest(shard_root)
    summary = ShardSummary()

    pdfs: list[Path] = []
    for raw in pdf_paths:
        p = Path(raw)
        if not p.exists() or p.stat().st_size < MIN_REAL_PDF_BYTES:
            continue
        pdfs.append(p)

    total = len(pdfs)
    for idx, pdf in enumerate(pdfs, start=1):
        if not rebuild and is_fresh(pdf, manifest, shard_root):
            summary.skipped += 1
            continue

        existed_in_manifest = pdf.name in manifest
        try:
            print(f"[{idx}/{total}] extracting {pdf.name}")
            process_pdf_to_shard(pdf, shard_root)
            manifest[pdf.name] = fingerprint(pdf)
            if existed_in_manifest:
                summary.restaged += 1
            else:
                summary.added += 1
        except Exception as e:
            summary.failed += 1
            print(f"  FAILED {pdf.name}: {e}", file=sys.stderr)

    save_manifest(shard_root, manifest)
    return summary


# ---------------------------------------------------------------------------
# Concat — what the gold step calls
# ---------------------------------------------------------------------------


def concat_bronze(shard_root: Path) -> pl.LazyFrame:
    """Lazy-scan every bronze shard. Caller materializes once before passing
    into build_records. Empty-sentinel shards are filtered out so the
    downstream schema matches a normal extract."""
    shard_root = Path(shard_root)
    bronze_dir = shard_root / BRONZE_SUBDIR
    paths = sorted(bronze_dir.glob("*.parquet"))
    if not paths:
        return pl.LazyFrame()

    lazy_frames = []
    for p in paths:
        lf = pl.scan_parquet(p)
        # Sentinel-empty shards have just `_empty`; skip them.
        if lf.collect_schema().names() == ["_empty"]:
            continue
        lazy_frames.append(lf)
    return pl.concat(lazy_frames, how="vertical_relaxed") if lazy_frames else pl.LazyFrame()


def load_audit(shard_root: Path) -> list[dict[str, Any]]:
    audit_dir = Path(shard_root) / AUDIT_SUBDIR
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(audit_dir.glob("*.json"))]


def load_member_extracts(shard_root: Path) -> list[dict[str, Any]]:
    member_dir = Path(shard_root) / MEMBER_SUBDIR
    out: list[dict[str, Any]] = []
    for p in sorted(member_dir.glob("*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            out.extend(payload)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("paths", nargs="*", default=[DEFAULT_INPUT_GLOB])
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--rebuild", action="store_true", help="ignore manifest; re-extract every PDF")
    args = parser.parse_args(argv)

    matched: list[str] = []
    for pat in args.paths:
        hits = glob.glob(pat)
        matched.extend(hits if hits else [pat])
    matched = list(dict.fromkeys(matched))

    if not matched:
        parser.error(f"no PDFs matched any of: {args.paths}")

    print(f"shard root: {args.shard_root}")
    print(f"input pdfs: {len(matched)}")
    summary = incremental_extract(matched, args.shard_root, rebuild=args.rebuild)
    print(
        f"\nsummary: added={summary.added} skipped={summary.skipped} "
        f"restaged={summary.restaged} failed={summary.failed}"
    )

    # Sanity-check the concat path so a broken shard surfaces here, not in
    # the gold step. Lazy schema-only check; no full materialize.
    lf = concat_bronze(args.shard_root)
    schema = lf.collect_schema() if lf is not None else None
    print(f"concat schema cols: {len(schema) if schema else 0}")
    return 0 if summary.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
