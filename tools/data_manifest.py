"""tools/data_manifest.py — content-hash manifest of the raw + derived data trees.

Writes ``data/_meta/backup_manifest.tsv``: one line per file under ``data/bronze``
and ``data/silver`` recording ``sha256<TAB>size<TAB>relpath`` (relative to
``data/``), sorted by path. The file is git-tracked (the ``!data/_meta/`` negation
in .gitignore reaches it), so it changes **only when file content changes** —
mtime churn from re-downloads does not move it. That makes its git diff a precise
record of *what actually changed in the data*, even for the 9 GB of bronze/silver
that itself is never committed.

Why this exists
---------------
Most bronze captures come from sources that MUTATE or VANISH — council and
public-body procurement PDFs get re-published in place, and some SIPO candidate
documents already 403 at source. A plain backup mirror silently overwrites the
old version with the new one. This manifest is the *detector*: run it before each
backup and the diff tells you which source PDFs changed since last time, so a
mutated council file is a visible event (a changed sha line) rather than a silent
overwrite. Pair it with object versioning on the R2 bucket (see doc/DATA_BACKUP.md)
so the prior bytes are also recoverable, not just detectable.

What it does NOT do
-------------------
It does not fetch, back up, or restore anything — it only fingerprints what is on
disk now. The actual upload is ``tools/backup_to_r2.ps1`` (rclone). It hashes the
full tree every run (~9 GB ≈ a minute or two); there is deliberately no incremental
cache to keep the tool stateless and the output trustworthy.

Usage:
    python tools/data_manifest.py                 # write manifest, print drift summary
    python tools/data_manifest.py --print         # also list every changed/added/removed path
    python tools/data_manifest.py --check         # exit 1 if anything drifted (for a guard)
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from config import BRONZE_DIR, DATA_DIR, SILVER_DIR
from services.logging_setup import setup_standalone_logging

log = logging.getLogger(__name__)

MANIFEST_PATH = DATA_DIR / "_meta" / "backup_manifest.tsv"
ROOTS = (BRONZE_DIR, SILVER_DIR)

# The siting layer store moved on 2026-08-27 from data/silver/parquet/planning_layers/ to
# planning/product/data/planning_layers/ (private tree). The old path is a directory JUNCTION
# onto it, which pathlib descends (a junction is not `is_symlink()`), so the silver scan must
# skip it or every layer file is hashed twice under two names. The store is scanned from its
# real location under its OLD manifest prefix, so the manifest keys match the R2 layout that
# tools/backup_to_r2.ps1 keeps (`silver/parquet/planning_layers/`). The same excludes as that
# script apply: raw upstream archives and the _ingest staging tree are public re-downloadable
# sources and are neither backed up nor fingerprinted.
LAYERS_DIR = DATA_DIR.parent / "planning" / "product" / "data" / "planning_layers"
LAYERS_MANIFEST_PREFIX = "silver/parquet/planning_layers"
_LAYERS_JUNCTION = SILVER_DIR / "parquet" / "planning_layers"
_LAYERS_SKIP_DIRS = frozenset(
    {"_ingest", "_corrupt", "_superseded", "_archived", "_retired", "_rebuild_logs"}
)
_LAYERS_SKIP_SUFFIXES = (".zip", ".gpkg", ".shp")


def _layer_file_is_backed_up(path: Path) -> bool:
    rel = path.relative_to(LAYERS_DIR)
    if rel.parts and rel.parts[0] in _LAYERS_SKIP_DIRS:
        return False
    if any(part.endswith(".gdb") for part in rel.parts[:-1]):
        return False
    return not path.name.lower().endswith(_LAYERS_SKIP_SUFFIXES)
_CHUNK = 1024 * 1024  # 1 MiB read buffer for hashing


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(_CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def _scan() -> dict[str, tuple[str, int]]:
    """Return {relpath_posix: (sha256, size)} for every file under the roots."""
    out: dict[str, tuple[str, int]] = {}
    for root in ROOTS:
        if not root.exists():
            log.warning("root missing, skipping: %s", root)
            continue
        for path in sorted(p for p in root.rglob("*") if p.is_file()):
            if _LAYERS_JUNCTION in path.parents:
                continue  # scanned below from its real location
            rel = path.relative_to(DATA_DIR).as_posix()
            out[rel] = (_sha256(path), path.stat().st_size)
    if LAYERS_DIR.is_dir():
        for path in sorted(p for p in LAYERS_DIR.rglob("*") if p.is_file()):
            if not _layer_file_is_backed_up(path):
                continue
            rel = f"{LAYERS_MANIFEST_PREFIX}/{path.relative_to(LAYERS_DIR).as_posix()}"
            out[rel] = (_sha256(path), path.stat().st_size)
    else:
        log.warning("layer store missing, skipping: %s", LAYERS_DIR)
    return out


def _load_prior() -> dict[str, str]:
    """Read the committed manifest into {relpath: sha256}; empty on first run."""
    prior: dict[str, str] = {}
    if not MANIFEST_PATH.exists():
        return prior
    for line in MANIFEST_PATH.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        sha, _size, rel = line.split("\t", 2)
        prior[rel] = sha
    return prior


def _write(current: dict[str, tuple[str, int]]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    total = sum(size for _sha, size in current.values())
    header = (
        f"# data backup manifest — sha256<TAB>size<TAB>relpath (relative to data/)\n"
        f"# generated {datetime.now(UTC).isoformat(timespec='seconds')} | "
        f"{len(current)} files | {total / 1e9:.2f} GB\n"
    )
    body = "".join(f"{sha}\t{size}\t{rel}\n" for rel, (sha, size) in sorted(current.items()))
    MANIFEST_PATH.write_text(header + body, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print", dest="verbose", action="store_true", help="list every added/removed/changed path")
    parser.add_argument("--check", action="store_true", help="exit 1 if anything drifted since the committed manifest")
    args = parser.parse_args()

    setup_standalone_logging("data_manifest")

    prior = _load_prior()
    current = _scan()

    prior_keys, cur_keys = set(prior), set(current)
    added = sorted(cur_keys - prior_keys)
    removed = sorted(prior_keys - cur_keys)
    changed = sorted(k for k in prior_keys & cur_keys if prior[k] != current[k][0])
    # A changed PDF means a source file was re-published in place — the case the
    # whole exercise exists to catch. Surface it separately from ordinary churn.
    changed_pdfs = [k for k in changed if k.lower().endswith(".pdf")]

    # A restore check must not mutate its expected baseline. Otherwise a failed
    # check overwrites the manifest with the damaged restore and a retry can
    # falsely pass.
    if not args.check:
        _write(current)

    total_gb = sum(size for _sha, size in current.values()) / 1e9
    log.info("manifest: %d files, %.2f GB -> %s", len(current), total_gb, MANIFEST_PATH)
    log.info(
        "drift since last manifest: +%d added  -%d removed  ~%d changed (%d PDFs)",
        len(added),
        len(removed),
        len(changed),
        len(changed_pdfs),
    )

    if args.verbose:
        for tag, items in (("ADDED", added), ("REMOVED", removed), ("CHANGED", changed)):
            for rel in items:
                log.info("  %-7s %s", tag, rel)
    elif changed_pdfs:
        log.warning("source PDFs re-published in place (showing up to 10):")
        for rel in changed_pdfs[:10]:
            log.warning("  CHANGED %s", rel)

    if args.check and (added or removed or changed):
        return 1
    return 0


if __name__ == "__main__":
    exit_code = main()
    # Explicitly release the rotating file handler before interpreter teardown.
    # On this Windows workstation an inherited handler has occasionally held the
    # standalone process open after the manifest was already written.
    logging.shutdown()
    os._exit(exit_code)
