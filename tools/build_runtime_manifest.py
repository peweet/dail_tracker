"""tools/build_runtime_manifest.py — classify every tracked data/ parquet by retention.

Generates ``data/_meta/runtime_data_manifest.json`` — the single source of truth for
*which* tracked parquet a deployment needs at runtime and *why*. Three retention states:

* ``runtime`` — read by a registered SQL view; MUST reach the clone.
* ``lineage`` — never read at runtime; an ETL *input* kept deliberately for reproducibility
  (e.g. the silver payment-facts folded into a consolidated gold fact). Not a deletion target.
* ``dead``    — no runtime reader found AND not in the lineage allow-list; an untrack
  *candidate*, surfaced for human review. NEVER auto-untracked by anything.

This is the *data-retention / lineage* sense of "provenance", deliberately distinct from the
user-facing verifiability concept in doc/API_PROVENANCE_REVIEW.md (the T1/T2/T3 tier model).

The ``runtime`` set is computed from ACTUAL reads, never a hand list: every
``read_parquet('data/...')`` literal in ``sql_views/**`` plus every ``{KEY}`` placeholder
resolved through ``PLACEHOLDER_TO_PATH`` (which mirrors the ``substitutions=`` maps in
``dail_tracker_core/connections.py``). All runtime parquet reads flow through SQL views — there
are no direct ``read_parquet('data/...')`` literals in the Python runtime dirs — so the view scan
is the complete audit surface. test/tools/test_runtime_manifest.py fails if a new placeholder
appears in the SQL that this map doesn't cover, or if a view reads a parquet not marked runtime.

R2 key = ``runtime/`` + path-relative-to-``data/`` — the runtime publish lane
(tools/publish_runtime_to_r2.ps1) writes there, leaving the append-only bronze/silver archive
(tools/backup_to_r2.ps1) untouched.

Usage:
    python tools/build_runtime_manifest.py            # regenerate the manifest
    python tools/build_runtime_manifest.py --check     # build in-memory, exit 1 if the on-disk
                                                        # manifest is stale (CI guard), write nothing
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

# tools/ scripts run from the repo root in practice, but make the import robust either way.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import (  # noqa: E402 — sys.path shim must precede project imports
    BASE_DIR,
    DATA_DIR,
    GOLD_SPEECHES_FACT_PARQUET,
    GOLD_SEANAD_VOTE_HISTORY_PARQUET,
    GOLD_VOTE_HISTORY_PARQUET,
    SILVER_PARQUET_DIR,
)
from pdf_infra.pdf_fingerprint import sha256_file  # noqa: E402 — reuse, don't re-implement
from services.logging_setup import setup_standalone_logging  # noqa: E402

_log = logging.getLogger("build_runtime_manifest")

SQL_VIEWS_DIR = _REPO_ROOT / "sql_views"
MANIFEST_PATH = DATA_DIR / "_meta" / "runtime_data_manifest.json"

_LITERAL_RE = re.compile(r"read_parquet\('(data/[^']+\.parquet)'\)")
_PLACEHOLDER_RE = re.compile(r"read_parquet\('(\{[A-Z_]+\})'\)")


def _rel(path: Path) -> str:
    """Repo-relative posix path under ``data/`` (matches ``git ls-files`` output)."""
    return "data/" + path.relative_to(DATA_DIR).as_posix()


# Mirrors the inline ``substitutions=`` maps in dail_tracker_core/connections.py. The SPEECH fact
# resolves to the committed LITE slice (speeches_fact.parquet) — the full fact is gitignored
# (>100 MB) and only present locally/API, so the lite slice is the runtime artifact that ships.
PLACEHOLDER_TO_PATH: dict[str, str] = {
    "{MEMBER_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "flattened_members.parquet"),
    "{SEANAD_MEMBER_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"),
    "{HISTORIC_DAIL_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "historic_members_dail.parquet"),
    "{HISTORIC_SEANAD_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "historic_members_seanad.parquet"),
    "{MEMBER_TERMS_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "member_terms.parquet"),
    "{EXTERNAL_LINKS_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "member_external_links.parquet"),
    "{CONTACT_DETAILS_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "member_contact_details.parquet"),
    "{NEWS_MENTIONS_PARQUET_PATH}": _rel(SILVER_PARQUET_DIR / "news_mentions.parquet"),
    "{PARQUET_PATH}": _rel(GOLD_VOTE_HISTORY_PARQUET),
    "{SEANAD_VOTE_PARQUET_PATH}": _rel(GOLD_SEANAD_VOTE_HISTORY_PARQUET),
    "{SPEECH_FACT_PARQUET_PATH}": _rel(GOLD_SPEECHES_FACT_PARQUET),
}

# Runtime inputs that are read but deliberately absent-tolerant: the view registers under its own
# try/except (swallow_errors), so a missing parquet degrades gracefully rather than breaking a page.
# These are NOT committed and must NOT be reported as ship gaps. See connections.py phases 3c.
KNOWN_OPTIONAL: frozenset[str] = frozenset(
    {
        "data/silver/parquet/news_mentions.parquet",  # per-member Google-News search (optional)
    }
)

# Hand-curated ETL-input files kept deliberately for reproducibility — never read at runtime.
# Seeded from doc/DATA_DISTRIBUTION_PLAN.md §"Candidate non-runtime": the per-source silver
# payment-facts folded into gold procurement_payments_fact by the consolidate chain, plus the gold
# pre-union copies superseded by that consolidated fact. NOT a deletion list — these are the
# provenance/reproducibility tail. (CSO series are deliberately omitted: which are surfaced changes
# as views are built, so they are left to automatic read-detection rather than a stale hand list.)
LINEAGE_ALLOWLIST: dict[str, str] = {
    "data/silver/parquet/public_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/silver/parquet/hse_tusla_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/silver/parquet/nta_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/silver/parquet/nphdb_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/silver/parquet/seai_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/silver/parquet/dept_readingorder_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/silver/parquet/la_payments_fact.parquet": "ETL input folded into gold procurement_payments_fact (consolidate chain)",
    "data/gold/parquet/public_payments_fact.parquet": "gold pre-union copy superseded by consolidated procurement_payments_fact",
    "data/gold/parquet/hse_tusla_payments_fact.parquet": "gold pre-union copy superseded by consolidated procurement_payments_fact",
}


def tracked_parquet() -> list[str]:
    """Every git-tracked ``data/**/*.parquet`` (repo-relative posix), via a read-only git query.

    Tracked — not a raw rglob — so local-only files (e.g. the gitignored speeches_fact_full.parquet)
    are never mis-flagged as ship candidates.
    """
    out = subprocess.run(
        ["git", "ls-files", "data/**/*.parquet"],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    return sorted(line.strip() for line in out.splitlines() if line.strip())


def runtime_reads() -> tuple[dict[str, list[str]], list[str]]:
    """Scan ``sql_views/**`` for parquet reads.

    Returns ``(readers, unresolved)`` where ``readers`` maps each data-relative parquet path to the
    list of view files / placeholders that read it, and ``unresolved`` lists ``{KEY}`` placeholders
    found in the SQL that ``PLACEHOLDER_TO_PATH`` does not cover (should be empty — the parity test
    guards it).
    """
    readers: dict[str, list[str]] = {}
    unresolved: set[str] = set()
    for sql_file in sorted(SQL_VIEWS_DIR.glob("**/*.sql")):
        text = sql_file.read_text(encoding="utf-8")
        label = sql_file.relative_to(SQL_VIEWS_DIR).as_posix()
        for rel in _LITERAL_RE.findall(text):
            readers.setdefault(rel, [])
            if label not in readers[rel]:
                readers[rel].append(label)
        for placeholder in _PLACEHOLDER_RE.findall(text):
            target = PLACEHOLDER_TO_PATH.get(placeholder)
            if target is None:
                unresolved.add(placeholder)
                continue
            readers.setdefault(target, [])
            tag = f"{label} {placeholder}"
            if tag not in readers[target]:
                readers[target].append(tag)
    return readers, sorted(unresolved)


def build() -> dict:
    """Assemble the manifest dict (no I/O beyond reading sql_views, git, and hashing data files)."""
    tracked = tracked_parquet()
    tracked_set = set(tracked)
    readers, unresolved = runtime_reads()
    runtime_set = set(readers)

    files = []
    counts = {"runtime": 0, "lineage": 0, "dead": 0}
    bytes_runtime = 0
    for rel in tracked:
        if rel in runtime_set:
            retention = "runtime"
            kept_because = "read by " + ", ".join(readers[rel][:3]) + ("…" if len(readers[rel]) > 3 else "")
        elif rel in LINEAGE_ALLOWLIST:
            retention = "lineage"
            kept_because = LINEAGE_ALLOWLIST[rel]
        else:
            retention = "dead"
            kept_because = "no runtime reader found — untrack candidate (review before removing)"
        counts[retention] += 1

        abs_path = BASE_DIR / rel
        size = abs_path.stat().st_size if abs_path.exists() else 0
        if retention == "runtime":
            bytes_runtime += size
        files.append(
            {
                "path": rel,
                "retention": retention,
                "read_at_runtime": retention == "runtime",
                "kept_because": kept_because,
                "r2_key": "runtime/" + rel[len("data/") :],
                "sha256": sha256_file(abs_path) if abs_path.exists() else None,
                "size_bytes": size,
            }
        )

    # Runtime reads pointing at a parquet that is NOT tracked. Split: a real ship gap (the view
    # would fail to register on a fresh clone — errors are swallowed) vs a deliberately optional
    # input (KNOWN_OPTIONAL — absent-tolerant by design). Surfaced, never silently dropped.
    untracked = sorted(runtime_set - tracked_set)
    referenced_but_untracked = [
        {"path": rel, "readers": readers[rel]} for rel in untracked if rel not in KNOWN_OPTIONAL
    ]
    optional_untracked = [
        {"path": rel, "readers": readers[rel]} for rel in untracked if rel in KNOWN_OPTIONAL
    ]

    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": (
            "GENERATED by tools/build_runtime_manifest.py. Source of truth for which tracked parquet "
            "a deployment needs and why. retention: runtime=read by a SQL view, must reach the clone; "
            "lineage=ETL input kept for reproducibility, not read at runtime; dead=no runtime reader "
            "found, untrack CANDIDATE (review only, never auto-removed). r2_key is under the runtime/ "
            "prefix used by tools/publish_runtime_to_r2.ps1."
        ),
        "summary": {
            **counts,
            "tracked_total": len(tracked),
            "bytes_runtime": bytes_runtime,
        },
        "referenced_but_untracked": referenced_but_untracked,
        "optional_untracked": optional_untracked,
        "unresolved_placeholders": unresolved,
        "files": files,
    }


def _write_atomic(manifest: dict) -> None:
    """Write JSON to ``<dest>.part`` then ``os.replace`` it onto the manifest (atomic)."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST_PATH.parent / (MANIFEST_PATH.name + ".part")
    payload = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    try:
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, MANIFEST_PATH)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _stable(manifest: dict) -> dict:
    """Manifest minus the volatile timestamp, for stale-comparison in --check."""
    return {k: v for k, v in manifest.items() if k != "generated_at"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--check",
        action="store_true",
        help="build in-memory and exit 1 if the on-disk manifest is stale; write nothing",
    )
    args = parser.parse_args(argv)
    setup_standalone_logging("build_runtime_manifest")

    manifest = build()
    s = manifest["summary"]
    _log.info(
        "runtime=%d lineage=%d dead=%d (tracked=%d, %.1f MB runtime)",
        s["runtime"], s["lineage"], s["dead"], s["tracked_total"], s["bytes_runtime"] / 1e6,
    )
    if manifest["unresolved_placeholders"]:
        _log.warning("UNRESOLVED placeholders (add to PLACEHOLDER_TO_PATH): %s", manifest["unresolved_placeholders"])
    if manifest["referenced_but_untracked"]:
        for entry in manifest["referenced_but_untracked"]:
            _log.warning("SHIP GAP — runtime-read but NOT git-tracked: %s (read by %s)", entry["path"], entry["readers"])

    if args.check:
        if not MANIFEST_PATH.exists():
            _log.error("--check: %s does not exist; run without --check to generate it", MANIFEST_PATH)
            return 1
        on_disk = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        if _stable(on_disk) != _stable(manifest):
            _log.error("--check: %s is STALE — re-run `python tools/build_runtime_manifest.py`", MANIFEST_PATH.name)
            return 1
        _log.info("--check: manifest is up to date.")
        return 0

    _write_atomic(manifest)
    _log.info("wrote %s", MANIFEST_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
