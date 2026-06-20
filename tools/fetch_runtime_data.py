"""tools/fetch_runtime_data.py — rehydrate the runtime data working set from Cloudflare R2.

The fetch side of doc/DATA_DISTRIBUTION_PLAN.md. Reads data/_meta/runtime_data_manifest.json and,
for every ``retention: runtime`` entry, pulls its ``r2_key`` from R2 to ``data/<relpath>`` *iff*
the local file is missing or its sha256 doesn't match the manifest. Idempotent — a second run with
everything present is a no-op.

R2 keys live under the ``runtime/`` prefix written by tools/publish_runtime_to_r2.ps1, in the same
``dail-tracker-backup`` bucket as the append-only archive (which this never touches).

PHASE 1 SCOPE: a manual rehydrate/verify tool (e.g. prove a clean checkout can refill its data from
R2). It is NOT yet wired into the Dockerfile or a Streamlit Cloud cold-start hook, and credential
handling for those environments is deferred to Phase 2 — locally it uses the same rclone ``r2``
remote as tools/backup_to_r2.ps1.

Usage:
    python tools/fetch_runtime_data.py --dry-run    # report missing/stale vs ok; fetch nothing
    python tools/fetch_runtime_data.py              # fetch missing/stale runtime files from R2
    python tools/fetch_runtime_data.py --dest DIR   # rehydrate into DIR instead of ./data (testing)
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import DATA_DIR  # noqa: E402
from pdf_infra.pdf_fingerprint import sha256_file  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402

_log = logging.getLogger("fetch_runtime_data")

MANIFEST_PATH = DATA_DIR / "_meta" / "runtime_data_manifest.json"
R2_REMOTE = "r2"
R2_BUCKET = "dail-tracker-backup"


def _resolve_rclone() -> str:
    """Find rclone on PATH (mirrors tools/backup_to_r2.ps1's resolution intent)."""
    exe = shutil.which("rclone")
    if not exe:
        raise SystemExit("rclone not found on PATH — see doc/DATA_BACKUP.md for setup.")
    return exe


def _local_status(entry: dict, dest_root: Path) -> str:
    """'missing' | 'stale' | 'ok' for a manifest entry relative to ``dest_root``."""
    # entry['path'] is 'data/<rel>'; dest_root stands in for the repo's data/ dir.
    rel = entry["path"][len("data/") :]
    local = dest_root / rel
    if not local.exists():
        return "missing"
    if entry.get("sha256") and sha256_file(local) != entry["sha256"]:
        return "stale"
    return "ok"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="report status; fetch nothing")
    parser.add_argument(
        "--dest",
        type=Path,
        default=None,
        help="data root to rehydrate into (default: the repo's ./data); useful for testing",
    )
    args = parser.parse_args(argv)
    setup_standalone_logging("fetch_runtime_data")

    if not MANIFEST_PATH.exists():
        raise SystemExit(f"manifest not found: {MANIFEST_PATH} — run tools/build_runtime_manifest.py first.")
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    runtime = [f for f in manifest["files"] if f["retention"] == "runtime"]

    # dest_root holds the gold/ silver/ tree: default → the repo's data/ dir; --dest DIR → DIR.
    dest_root = DATA_DIR if args.dest is None else args.dest

    need: list[dict] = []
    ok = 0
    for entry in runtime:
        status = _local_status(entry, dest_root)
        if status == "ok":
            ok += 1
        else:
            need.append(entry)
            _log.info("%-7s %s", status.upper(), entry["path"])

    _log.info("runtime files: %d total | %d ok | %d to fetch", len(runtime), ok, len(need))

    if args.dry_run:
        _log.info("--dry-run — nothing fetched.")
        return 0
    if not need:
        _log.info("all runtime files present and current — nothing to do.")
        return 0

    rclone = _resolve_rclone()
    failed = 0
    for entry in need:
        rel = entry["path"][len("data/") :]
        local = dest_root / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        src = f"{R2_REMOTE}:{R2_BUCKET}/{entry['r2_key']}"
        # `rclone copyto` pulls a single object to an exact destination path.
        r = subprocess.run([rclone, "copyto", src, str(local)], text=True, capture_output=True)
        if r.returncode != 0:
            failed += 1
            _log.error("fetch FAILED %s: %s", entry["path"], (r.stderr or "").strip())
            continue
        if entry.get("sha256") and sha256_file(local) != entry["sha256"]:
            failed += 1
            _log.error("hash MISMATCH after fetch %s — R2 object differs from manifest", entry["path"])
            continue
        _log.info("fetched %s", entry["path"])

    if failed:
        _log.error("%d of %d fetches failed", failed, len(need))
        return 1
    _log.info("rehydrated %d runtime file(s) from R2.", len(need))
    return 0


if __name__ == "__main__":
    sys.exit(main())
