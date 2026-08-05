"""Ephemeral repository copies for evals whose scorer must stay out of agent reach."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

MARKER = ".eval-cleanroom.json"
EXCLUDED_PREFIXES = (
    "tools/evals/",
    "test/tools/evals/",
    "planning/product/",
    ".git/",
    ".git-siting/",
)
EXCLUDED_NAMES = {".git", ".git-siting", MARKER}


def should_copy(relative_path: str) -> bool:
    path = relative_path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    pure = PurePosixPath(path)
    if not path or pure.is_absolute() or ":" in pure.parts[0] or ".." in pure.parts:
        return False
    return pure.name not in EXCLUDED_NAMES and not any(path.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def repository_files(repo: Path) -> list[str]:
    """Return tracked plus visible untracked files from the current working tree."""
    command = ["git", "-C", str(repo), "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
    completed = subprocess.run(command, capture_output=True, check=True)
    return sorted({item.decode("utf-8", errors="surrogateescape") for item in completed.stdout.split(b"\0") if item})


def source_revision(repo: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def _copy_files(source: Path, destination: Path, files: Sequence[str]) -> int:
    count = 0
    for relative in files:
        if not should_copy(relative):
            continue
        src = source / relative
        if not src.is_file():
            continue
        try:
            src.resolve().relative_to(source)
        except ValueError:  # do not follow a tracked symlink out to host files
            continue
        dst = destination / relative
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        count += 1
    return count


def validate_cleanroom(path: Path) -> dict:
    marker = path / MARKER
    if not marker.is_file():
        raise ValueError(f"not an eval cleanroom: marker absent at {marker}")
    if (path / ".git").exists() or (path / "tools" / "evals").exists():
        raise ValueError("eval cleanroom exposes repository history or scorer source")
    return json.loads(marker.read_text(encoding="utf-8"))


@contextmanager
def prepare_cleanroom(source: str | Path, *, files: Sequence[str] | None = None) -> Iterator[Path]:
    """Copy the live tree without Git metadata, private overlay, eval prompts, or scorers."""
    repo = Path(source).resolve()
    temp_root = Path(tempfile.mkdtemp(prefix="dail-harness-")).resolve()
    destination = temp_root / "repo"
    destination.mkdir()
    try:
        selected = repository_files(repo) if files is None else list(files)
        copied = _copy_files(repo, destination, selected)
        metadata = {
            "created_utc": datetime.now(UTC).isoformat(),
            "source_revision": source_revision(repo),
            "files_copied": copied,
            "excluded_prefixes": list(EXCLUDED_PREFIXES),
        }
        (destination / MARKER).write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        validate_cleanroom(destination)
        yield destination
    finally:
        expected_parent = Path(tempfile.gettempdir()).resolve()
        if temp_root.parent == expected_parent and temp_root.name.startswith("dail-harness-"):
            shutil.rmtree(temp_root, ignore_errors=True)
