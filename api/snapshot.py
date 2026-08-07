"""Stable data-snapshot identity and HTTP validator helpers for the public API."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

_SNAPSHOT_FORMAT = "dail-api-snapshot/1"
_REPRESENTATION_FORMAT = "dail-api-representation/1"
_MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "_meta" / "runtime_data_manifest.json"


@dataclass(frozen=True)
class DataSnapshot:
    """The exact runtime parquet inventory available to this API process."""

    identifier: str
    generated_at: str


def load_data_snapshot(manifest_path: Path = _MANIFEST_PATH) -> DataSnapshot:
    """Hash the pipeline-produced runtime inventory, not file mtimes or API output."""
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = manifest["files"]
        generated_at = str(manifest["generated_at"])
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"cannot load runtime data manifest: {manifest_path}") from exc

    runtime_files = sorted(
        {(str(entry["path"]), str(entry["sha256"])) for entry in files if entry.get("read_at_runtime") is True}
    )
    if not runtime_files:
        raise RuntimeError(f"runtime data manifest has no runtime files: {manifest_path}")
    canonical = json.dumps(
        {"format": _SNAPSHOT_FORMAT, "files": runtime_files},
        separators=(",", ":"),
        sort_keys=True,
    )
    return DataSnapshot(identifier=hashlib.sha256(canonical.encode()).hexdigest(), generated_at=generated_at)


def representation_etag(snapshot: DataSnapshot, *, path: str, query_items: list[tuple[str, str]]) -> str:
    """Return a strong validator for one API representation of a data snapshot."""
    canonical_query = urlencode(sorted(query_items))
    canonical = "\n".join((_REPRESENTATION_FORMAT, snapshot.identifier, path, canonical_query))
    return f'"{hashlib.sha256(canonical.encode()).hexdigest()}"'


def is_not_modified(if_none_match: str | None, etag: str) -> bool:
    """Match the current strong validator against a standard If-None-Match value."""
    if not if_none_match:
        return False
    candidates = (candidate.strip() for candidate in if_none_match.split(","))
    return any(candidate == "*" or candidate.removeprefix("W/") == etag for candidate in candidates)
