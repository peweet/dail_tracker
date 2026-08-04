"""Path + ID helpers for per-run pipeline log directories.

Layout produced by a pipeline run:

    logs/
    ├── runs/
    │   └── <run_id>/
    │       ├── pipeline.log              # orchestrator-only log
    │       ├── manifest.json             # this run's full manifest record
    │       └── steps/
    │           └── NN_<slug>.log         # captured stdout+stderr per step
    ├── latest_run_id.txt                 # pointer to most-recent run_id
    └── pipeline.log                      # legacy fallback for standalone scripts

Run IDs use UTC ISO time with Windows- and zip-safe characters:
    2026-05-27T14-22-08Z-<8-hex>

Colons are replaced with hyphens so the id can drop into any filename or
CI artifact path without escaping.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

from config import LOG_DIR
from paths import PROJECT_ROOT

RUNS_DIR = LOG_DIR / "runs"
LATEST_POINTER = LOG_DIR / "latest_run_id.txt"

ENV_RUN_ID = "DAIL_PIPELINE_RUN_ID"

_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z", re.ASCII)


def make_run_id() -> str:
    """Generate a sortable, filesystem-safe run identifier (UTC + short uuid)."""
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"{now}-{uuid.uuid4().hex[:8]}"


def validate_run_id(run_id: str) -> str:
    """Return a safe run ID or raise ``ValueError``.

    Run IDs become directory names and are also accepted from the inherited
    ``DAIL_PIPELINE_RUN_ID`` environment variable. Restricting them to one
    portable path component prevents absolute paths and ``..`` traversal from
    redirecting logs outside :data:`RUNS_DIR`.
    """
    if not isinstance(run_id, str) or not _RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            "run_id must be 1-128 ASCII letters, digits, underscores, or hyphens and must start with a letter or digit"
        )
    return run_id


def run_dir(run_id: str) -> Path:
    """Return the contained absolute run directory, creating it if needed."""
    safe_run_id = validate_run_id(run_id)
    runs_root = RUNS_DIR.resolve()
    p = (runs_root / safe_run_id).resolve()
    try:
        p.relative_to(runs_root)
    except ValueError as exc:  # defense in depth if the ID policy changes
        raise ValueError(f"run_id resolves outside the run directory: {run_id!r}") from exc
    (p / "steps").mkdir(parents=True, exist_ok=True)
    return p


_SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    """Slugify a step name for use in log filenames."""
    slug = _SLUG_NON_ALNUM.sub("_", name.lower()).strip("_")
    return slug or "step"


def step_log_path(run_id: str, ordinal: int, name: str) -> Path:
    """Per-step capture path, e.g. steps/09_poll_lobbying_returns_ytd.log."""
    return run_dir(run_id) / "steps" / f"{ordinal:02d}_{slugify(name)}.log"


def write_latest_pointer(run_id: str) -> None:
    """Write the most-recent run_id as a tiny text file (no symlinks)."""
    safe_run_id = validate_run_id(run_id)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_POINTER.write_text(safe_run_id, encoding="utf-8")


def get_git_sha() -> str | None:
    """Best-effort short git SHA. Returns None outside a git checkout."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        return result.stdout.strip() or None
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return None


def prune_old_runs(days: int = 60) -> int:
    """Delete per-run log directories older than `days`. Returns count deleted.

    Not wired in by default — uncomment the call in pipeline.main() to enable.
    """
    if not RUNS_DIR.exists():
        return 0
    cutoff = datetime.now(UTC).timestamp() - days * 86400
    deleted = 0
    for child in RUNS_DIR.iterdir():
        if not child.is_dir():
            continue
        if child.stat().st_mtime < cutoff:
            shutil.rmtree(child, ignore_errors=True)
            deleted += 1
    return deleted
